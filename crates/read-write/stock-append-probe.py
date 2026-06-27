#!/usr/bin/env python3
"""Stock-client (pyiceberg) probe of LakeCat's Iceberg-write round-trip.

This is BOTH a standalone diagnostic AND the helper the `read-write` bench shells
out to for its `stock-roundtrip` phase. It drives a RAW, stock `pyiceberg 0.11.1`
`RestCatalog` against LakeCat end to end: init (GET /v1/config) → create_namespace
→ create_table → `table.append(arrow)` (a REAL Iceberg snapshot append) → scan the
rows back.

Setup (kept out of git via .gitignore):
    cd crates/read-write
    python3.12 -m venv .venv
    .venv/bin/pip install "pyiceberg[pyarrow,s3fs]"
    .venv/bin/python stock-append-probe.py

Env knobs: LAKECAT_BASE (e.g. http://127.0.0.1:8183/catalog), AWS_ENDPOINT,
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, SHIM (default 1).

`SHIM=0` runs a TRULY stock client (no response rewriting). After the five fixes
(LakeCat H8 config-objects + canonical endpoints + listTables + H9, and Sail
add-snapshot / set-snapshot-ref) this completes the full round-trip with
`SHIM=0`. The legacy `SHIM=1` response-rewriting shim is retained only to
reproduce the OLD pre-fix failure modes for comparison.

Machine-readable output: the last line is always
    ROUNDTRIP_RESULT {json}
with `status` one of `ok` / `gated` / `error`, plus `snapshots_after`,
`rows_scanned`, and `reason`. The Rust bench scrapes this line.
"""

import json
import os
import sys
import time
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


def emit(status, snapshots_after=None, rows_scanned=None, reason=None):
    """Print the machine-readable result line the Rust bench scrapes."""
    payload = {
        "status": status,
        "snapshots_after": snapshots_after,
        "rows_scanned": rows_scanned,
        "reason": reason,
    }
    print(f"ROUNDTRIP_RESULT {json.dumps(payload)}")


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
        reason = f"RestCatalog init failed: {type(e).__name__}: {str(e)[:200]}"
        print(f"    FAIL: {reason}")
        if not SHIM:
            print(
                "    ^ raw config-array break (pre-H8-fix); re-run with SHIM=1 to get past it."
            )
        emit("gated", reason=reason)
        return 1

    # Unique table per run so the append round-trip is reliably re-runnable
    # (a fresh, empty table → snapshots after append is deterministically 1).
    ns, tbl = "rw_pyi_probe", f"events_{int(time.time() * 1000)}"
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
    try:
        t = cat.create_table(
            ident, schema=schema, location=f"s3://warehouse/{ns}/{tbl}"
        )
        print(f"    OK; metadata_location={t.metadata_location}")
    except Exception as e:  # noqa: BLE001
        reason = f"create_table failed: {type(e).__name__}: {str(e)[:200]}"
        print(f"    FAIL: {reason}")
        emit("gated", reason=reason)
        return 1

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
        snaps = len(t2.metadata.snapshots)
        rows = t2.scan().to_arrow().num_rows
        print(f"    OK; snapshots after append: {snaps}")
        print(f"    scan row count: {rows}")
        emit("ok", snapshots_after=snaps, rows_scanned=rows)
        return 0
    except Exception as e:  # noqa: BLE001
        reason = f"{type(e).__name__}: {str(e)[:300]}"
        print(f"    GATED: {reason}")
        print(
            "    ^ append rejected by the catalog (old build?). The data file + manifest\n"
            "      + manifest-list may already be in MinIO; only the snapshot commit failed."
        )
        emit("gated", reason=reason)
        return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        emit("error", reason=f"{type(e).__name__}: {str(e)[:200]}")
        sys.exit(3)
