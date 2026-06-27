#!/usr/bin/env python3
"""Stock-client (pyiceberg) probe of LakeCat's Iceberg-write compatibility.

This reproduces the PHASE 0 findings the `read-write` bench summarizes. It is a
*diagnostic*, not part of the measured bench (the Rust bench self-documents the
catalog gap via raw REST). Run it to see, end to end, exactly where a stock
Iceberg `RestCatalog` write breaks against LakeCat.

Setup (kept out of git via .gitignore):
    cd crates/read-write
    python3.12 -m venv .venv
    .venv/bin/pip install "pyiceberg[pyarrow,s3fs]"
    .venv/bin/python stock-append-probe.py

What it shows, in order:
  1. WITHOUT a shim, `RestCatalog(...)` raises a pydantic `dict_type` error
     because LakeCat's GET /v1/config returns the `defaults`/`overrides` MAP
     fields as JSON ARRAYS of {key,value}. (set SHIM=0 to see this raw failure.)
  2. WITH the response-rewriting shim (default): the config + endpoints are
     normalized so pyiceberg initializes; create_namespace + create_table
     succeed; but `table.append(arrow)` is rejected by LakeCat with
     `apply_table_updates: add-snapshot` — and the Parquet data file + manifest +
     manifest-list have ALREADY been written to MinIO (the data plane works; only
     the catalog snapshot registration is gated).
"""

import json
import os
import sys
import traceback

import pyarrow as pa
import requests

LAKECAT = os.environ.get("LAKECAT_BASE", "http://127.0.0.1:8181/catalog")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT", "http://127.0.0.1:9000")
S3_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
SHIM = os.environ.get("SHIM", "1") != "0"

# --- The response-rewriting shim. LakeCat serializes Iceberg map fields
# (defaults/overrides/config) as JSON arrays of {key,value}, and advertises
# non-canonical `endpoints` strings. Rewrite the arrays back to maps and drop
# `endpoints` so pyiceberg uses its default capability set. This is the minimal
# transform that lets a stock client reach the deeper append path. ---
MAP_KEYS = {"defaults", "overrides", "config"}


def normalize(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "endpoints":
                continue
            if (
                k in MAP_KEYS
                and isinstance(v, list)
                and all(isinstance(e, dict) and {"key", "value"} <= set(e) for e in v)
            ):
                out[k] = {e["key"]: e["value"] for e in v}
            else:
                out[k] = normalize(v)
        return out
    if isinstance(obj, list):
        return [normalize(e) for e in obj]
    return obj


if SHIM:
    _orig_send = requests.adapters.HTTPAdapter.send

    def _patched_send(self, request, **kw):
        resp = _orig_send(self, request, **kw)
        if "json" in resp.headers.get("content-type", "") and resp.content:
            try:
                data = json.loads(resp.content)
                fixed = normalize(data)
                if fixed != data:
                    resp._content = json.dumps(fixed).encode()
            except Exception:
                pass
        return resp

    requests.adapters.HTTPAdapter.send = _patched_send


def main() -> int:
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    props = {
        "uri": LAKECAT,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_KEY,
        "s3.secret-access-key": S3_SECRET,
        "s3.region": S3_REGION,
        "s3.path-style-access": "true",
    }
    print(f"shim={'ON' if SHIM else 'OFF'}  uri={LAKECAT}")
    print("=== 1. RestCatalog init (GET /v1/config) ===")
    try:
        cat = RestCatalog("lakecat", **props)
        print("    OK")
    except Exception as e:  # noqa: BLE001
        print(f"    FAIL: {type(e).__name__}: {str(e)[:200]}")
        if not SHIM:
            print(
                "    ^ this is the raw config-array break; re-run with SHIM=1 to get past it."
            )
        return 1

    ns, tbl = "rw_pyi_probe", "events"
    try:
        cat.create_namespace(ns)
    except Exception as e:  # noqa: BLE001
        print(f"    create_namespace note: {type(e).__name__}: {str(e)[:120]}")

    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "measure_a", LongType(), required=False),
        NestedField(3, "measure_b", DoubleType(), required=False),
        NestedField(4, "grp", StringType(), required=False),
    )
    ident = (ns, tbl)
    try:
        cat.drop_table(ident)
    except Exception:  # noqa: BLE001
        pass
    print("=== 2. create_table ===")
    t = cat.create_table(ident, schema=schema, location=f"s3://warehouse/{ns}/events")
    print(f"    OK; metadata_location={t.metadata_location}")

    n = 1000
    arr = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "measure_a": pa.array([i * 7 % 1000 for i in range(n)], pa.int64()),
            "measure_b": pa.array([float(i) * 0.5 for i in range(n)], pa.float64()),
            "grp": pa.array([f"g{i % 8}" for i in range(n)], pa.string()),
        }
    )
    print("=== 3. table.append(arrow) — the real Iceberg snapshot append ===")
    try:
        t.append(arr)
        t2 = cat.load_table(ident)
        print(f"    OK; snapshots after append: {len(t2.metadata.snapshots)}")
        print(f"    scan row count: {t2.scan().to_arrow().num_rows}")
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"    GATED: {type(e).__name__}: {str(e)[:300]}")
        print(
            "    ^ the data file + manifest + manifest-list ARE in MinIO; only the\n"
            "      catalog snapshot-registration commit (add-snapshot) was rejected."
        )
        return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(3)
