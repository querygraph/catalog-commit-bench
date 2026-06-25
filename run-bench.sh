#!/usr/bin/env bash
# Run the commit benchmark against whichever catalogs are reachable.
#
# Table CREATION differs per catalog (LakeCat needs client-supplied
# location+metadata; the others generate metadata server-side), so this script
# provisions LakeCat explicitly and lets the bench's --create handle the rest.
# Commit measurement is identical across all of them.
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

# --- LakeCat: provision the table in LakeCat's create shape, then commit -----
LAKECAT_BASE="${LAKECAT_BASE:-http://127.0.0.1:8181/catalog}"
if reachable "$LAKECAT_BASE/v1/config"; then
  curl -fsS -X POST "$LAKECAT_BASE/v1/namespaces" \
    -H 'content-type: application/json' \
    -d "{\"namespace\":[\"$NS\"],\"properties\":{}}" >/dev/null 2>&1 || true
  curl -fsS -X POST "$LAKECAT_BASE/v1/namespaces/$NS/tables" \
    -H 'content-type: application/json' \
    -d "{\"name\":\"$TABLE\",\"location\":\"file:///tmp/$NS/$TABLE\",\"metadata-location\":\"file:///tmp/$NS/$TABLE/metadata/00000.json\",\"metadata\":{\"format-version\":3,\"table-uuid\":\"11111111-1111-1111-1111-111111111111\",\"location\":\"file:///tmp/$NS/$TABLE\",\"last-sequence-number\":0,\"last-updated-ms\":1710000000000,\"last-column-id\":1,\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}],\"current-schema-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],\"default-spec-id\":0,\"properties\":{},\"snapshots\":[],\"snapshot-log\":[],\"metadata-log\":[]}}" >/dev/null 2>&1 || true
  # LakeCat is now spec-conformant on the bare commit path, so no --commit-suffix.
  run_one "LakeCat" "$LAKECAT_BASE" --idempotency
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
UNITY_BASE="${UNITY_BASE:-http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg}"
if reachable "$UNITY_BASE/v1/config"; then
  run_one "Unity" "$UNITY_BASE" --prefix "${UNITY_PREFIX:-unity}" \
    ${UNITY_TOKEN:+--token "$UNITY_TOKEN"} --create
else
  echo "skip Unity: $UNITY_BASE not reachable"
fi
