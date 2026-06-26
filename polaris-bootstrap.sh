#!/usr/bin/env bash
# Bootstrap a Polaris catalog for the benchmark: fetch an OAuth2 token and create
# an S3 catalog backed by the shared MinIO `warehouse` bucket. Unlike
# Nessie/Gravitino (which auto-serve a warehouse), Polaris needs this post-`up`
# step.
#
# Prints ONLY the bearer token to stdout (diagnostics go to stderr), so callers
# can do:  TOKEN=$(./polaris-bootstrap.sh)
#
# Assumes Polaris is up (e.g. from ~/src/boat) with root/secret client creds and
# the AWS static creds for MinIO; override via the env vars below.
set -euo pipefail

BASE="${POLARIS_BASE:-http://127.0.0.1:8185}"
CATALOG="${POLARIS_CATALOG:-bench}"
CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-secret}"
S3_ENDPOINT="${POLARIS_S3_ENDPOINT:-http://minio:9000}"

token=$(curl -sf -X POST "$BASE/api/catalog/v1/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
echo "polaris: token acquired" >&2

# stsUnavailable=true -> Polaris uses the server's static AWS creds instead of STS
# credential vending, which MinIO does not provide. 409 = catalog already exists.
code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE/api/management/v1/catalogs" \
  -H "Authorization: Bearer $token" -H 'content-type: application/json' \
  -d "{\"catalog\":{\"name\":\"$CATALOG\",\"type\":\"INTERNAL\",\"properties\":{\"default-base-location\":\"s3://warehouse/$CATALOG\"},\"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://warehouse/$CATALOG\"],\"endpoint\":\"$S3_ENDPOINT\",\"stsUnavailable\":true,\"pathStyleAccess\":true}}}")
echo "polaris: create-catalog '$CATALOG' HTTP $code (409 = already exists, fine)" >&2

echo "$token"
