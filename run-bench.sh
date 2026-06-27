#!/usr/bin/env bash
# Run the commit benchmark against whichever catalogs are reachable.
#
# LakeCat is now spec-conformant on both createTable (schema -> server-generated
# metadata) and the bare commit path, so every catalog uses the identical
# `--create` + commit flow; no per-catalog provisioning or commit-suffix.
set -uo pipefail

BENCH="${BENCH:-./target/release/catalog-commit-bench}"
ITER="${ITER:-2000}"
CONC="${CONC:-8}"
DUR="${DUR:-10}"
NS="${NS:-commit_bench}"
TABLE="${TABLE:-commits}"

reachable() { curl -fsS -o /dev/null --max-time 2 "$1" 2>/dev/null; }

run_one() {
  local name="$1" base="$2"; shift 2
  echo "============================================================"
  echo "  $name  ->  $base"
  echo "============================================================"
  "$BENCH" --base-url "$base" --namespace "$NS" --table "$TABLE" \
    --iterations "$ITER" --concurrency "$CONC" --duration-secs "$DUR" "$@"
  echo
}

# --- LakeCat (spec-conformant: standard --create + commit) -------------------
LAKECAT_BASE="${LAKECAT_BASE:-http://127.0.0.1:8181/catalog}"
if reachable "$LAKECAT_BASE/v1/config"; then
  run_one "LakeCat" "$LAKECAT_BASE" --create --idempotency
else
  echo "skip LakeCat: $LAKECAT_BASE not reachable"
fi

# --- Polaris (needs --token; set POLARIS_TOKEN) ------------------------------
POLARIS_BASE="${POLARIS_BASE:-http://127.0.0.1:8182/api/catalog}"
if [[ -n "${POLARIS_TOKEN:-}" ]] && reachable "$POLARIS_BASE/v1/config"; then
  run_one "Polaris" "$POLARIS_BASE" --prefix "${POLARIS_PREFIX:-my_catalog}" \
    --token "$POLARIS_TOKEN" --create
else
  echo "skip Polaris: set POLARIS_TOKEN and ensure $POLARIS_BASE is up"
fi

# --- Gravitino ---------------------------------------------------------------
GRAVITINO_BASE="${GRAVITINO_BASE:-http://127.0.0.1:9001/iceberg}"
if reachable "$GRAVITINO_BASE/v1/config"; then
  run_one "Gravitino" "$GRAVITINO_BASE" --create
else
  echo "skip Gravitino: $GRAVITINO_BASE not reachable"
fi

# --- Unity Catalog -----------------------------------------------------------
# NOTE: released Unity OSS (<= 0.5.0) serves Iceberg REST read-only, so --create
# and the commit path will fail; this only works against a write-capable build
# (PR #1618 / 0.6.0). Left here, gated on reachability, for when that ships.
UNITY_BASE="${UNITY_BASE:-http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg}"
if reachable "$UNITY_BASE/v1/config"; then
  run_one "Unity" "$UNITY_BASE" --prefix "${UNITY_PREFIX:-unity}" \
    ${UNITY_TOKEN:+--token "$UNITY_TOKEN"} --create
else
  echo "skip Unity: $UNITY_BASE not reachable"
fi
