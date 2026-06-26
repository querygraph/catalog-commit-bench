#!/usr/bin/env bash
# One-shot S3 multi-catalog commit-benchmark harness.
#
# (Re)builds LakeCat from source into its Linux Docker image, then runs the
# commit benchmark against every reachable Iceberg REST catalog so all of them
# write metadata.json to the *same* MinIO/S3 — an apples-to-apples comparison.
#
# Catalogs under test (all join the shared external network so they share MinIO):
#   - LakeCat   :8181/catalog        built from ../lakecat (this script owns it)
#   - Nessie    :19120/iceberg       prefix `main`
#   - Gravitino :9002/iceberg
#   - Polaris   :8185/api/catalog    needs $POLARIS_TOKEN (OAuth2 bootstrap)
#
# Prereq (managed separately, e.g. `cd ~/src/boat && docker compose up -d`):
# MinIO + the external `iceberg_lakehouse-net` network + the comparison catalogs.
# This script owns only the from-source LakeCat build/deploy and the bench run.
#
# Usage:
#   ./bench-stack.sh                 # build+deploy LakeCat, bench all reachable
#   SKIP_BUILD=1 ./bench-stack.sh    # skip the LakeCat rebuild, just bench
#   ITER=2000 CONC=8 DUR=10 ./bench-stack.sh
set -euo pipefail
cd "$(dirname "$0")"

ITER="${ITER:-1000}"; CONC="${CONC:-8}"; DUR="${DUR:-6}"
# Fresh namespace per run so `--create` always succeeds cleanly (creating the
# namespace + table); reusing an existing namespace makes some catalogs 409.
NS="${NS:-bench$(date +%s)}"; TABLE="${TABLE:-commits}"
NET="${NET:-iceberg_lakehouse-net}"
FEATURES="${FEATURES:-turso-local,sail-local}"
BENCH=./target/release/catalog-commit-bench

reachable() { curl -fsS -o /dev/null --max-time 2 "$1/v1/config" 2>/dev/null; }

# --- 0. prereqs --------------------------------------------------------------
docker network inspect "$NET" >/dev/null 2>&1 || {
  echo "shared network '$NET' is missing — bring up the catalog stack first" >&2
  echo "  (e.g. 'cd ~/src/boat && docker compose up -d minio nessie gravitino polaris')" >&2
  exit 1
}

# --- 1. build the host bench binary -----------------------------------------
echo "==> building the bench binary"
cargo build --release

# --- 2. build LakeCat for Linux -> image -> (re)start the container ----------
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "==> building LakeCat (features=$FEATURES) for Linux and packaging the image"
  FEATURES="$FEATURES" ./docker/build-lakecat.sh
  docker compose build lakecat
  docker compose up -d lakecat
  echo -n "==> waiting for LakeCat health "
  for _ in $(seq 1 90); do
    reachable http://127.0.0.1:8181/catalog && { echo "up"; break; }
    echo -n "."; sleep 2
  done
fi

# --- 3. ensure the shared S3 warehouse bucket exists -------------------------
docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
  "mc alias set m http://minio:9000 admin password >/dev/null 2>&1 && mc mb -p m/warehouse >/dev/null 2>&1; true" || true

# --- 4. bench every reachable catalog (identical params; all -> same MinIO) --
common=(--namespace "$NS" --table "$TABLE" --create \
  --iterations "$ITER" --concurrency "$CONC" --duration-secs "$DUR")
run_one() {
  local name="$1" base="$2"; shift 2
  echo "============================================================"
  echo "  $name  ->  $base"
  echo "============================================================"
  "$BENCH" --base-url "$base" "${common[@]}" "$@"
  echo
}

# LakeCat: spec-conformant bare commit path; --location pins writes to MinIO.
if reachable http://127.0.0.1:8181/catalog; then
  run_one "LakeCat" "http://127.0.0.1:8181/catalog" --idempotency --location s3://warehouse/lakecat
else echo "skip LakeCat: :8181 not reachable"; fi

if reachable http://127.0.0.1:19120/iceberg; then
  run_one "Nessie" "http://127.0.0.1:19120/iceberg" --prefix main
else echo "skip Nessie: :19120 not reachable"; fi

if reachable http://127.0.0.1:9002/iceberg; then
  run_one "Gravitino" "http://127.0.0.1:9002/iceberg"
else echo "skip Gravitino: :9002 not reachable"; fi

# Polaris: auto-bootstrap an OAuth2 token + S3 catalog when no token is provided.
# (Its /v1/config 401s without a token, so we probe via the bootstrap itself.)
POLARIS_BASE="${POLARIS_BASE:-http://127.0.0.1:8185}"
polaris_token="${POLARIS_TOKEN:-}"
if [[ -z "$polaris_token" ]]; then
  polaris_token="$(./polaris-bootstrap.sh || true)"
fi
if [[ -n "$polaris_token" ]]; then
  run_one "Polaris" "$POLARIS_BASE/api/catalog" \
    --prefix "${POLARIS_CATALOG:-bench}" --token "$polaris_token"
else
  echo "skip Polaris: not reachable (bootstrap failed); bring it up from ~/src/boat"
fi
