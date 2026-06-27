#!/usr/bin/env bash
# Run the JVM (Spark) side of the rust-vs-jvm bench in a container, reading the
# SAME Parquet from the SAME MinIO as the Rust/DataFusion side. Prints Spark's
# log to stderr and the single `SPARK_RESULT {json}` line to stdout.
#
# The Rust harness invokes this; it can also be run by hand. Requires Docker and
# internet (Spark pulls hadoop-aws + aws-sdk via --packages on first run).
#
# Env (with defaults matching the bench's BenchConfig):
#   SPARK_IMAGE   docker image                (apache/spark:3.5.3)
#   S3_ENDPOINT   MinIO URL from the container (http://host.docker.internal:9000)
#   S3_KEY/S3_SECRET                           (admin / password)
#   S3_PATH       s3a path to the dataset      (s3a://warehouse/cache-scan/)
#   ITERS         warm iterations              (8)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.3}"
S3_ENDPOINT="${S3_ENDPOINT:-http://host.docker.internal:9000}"
S3_KEY="${S3_KEY:-admin}"
S3_SECRET="${S3_SECRET:-password}"
S3_PATH="${S3_PATH:-s3a://warehouse/cache-scan/}"
ITERS="${ITERS:-8}"

# hadoop-aws + aws-sdk-bundle versions that match the Hadoop bundled in
# apache/spark:3.5.3 (Hadoop 3.3.4).
PKGS="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# Persist the Ivy cache across runs so only the FIRST invocation downloads the
# hadoop-aws / aws-sdk jars (every `docker run --rm` is otherwise a clean slate).
IVY_CACHE="${IVY_CACHE:-$HOME/.cache/rvj-ivy}"
mkdir -p "$IVY_CACHE"

exec docker run --rm \
  --add-host host.docker.internal:host-gateway \
  -e HOME=/tmp \
  -e S3_ENDPOINT="$S3_ENDPOINT" \
  -e S3_KEY="$S3_KEY" \
  -e S3_SECRET="$S3_SECRET" \
  -e S3_PATH="$S3_PATH" \
  -e ITERS="$ITERS" \
  -v "$SCRIPT_DIR/spark-query.py:/work/spark-query.py:ro" \
  -v "$IVY_CACHE:/tmp/ivy" \
  "$SPARK_IMAGE" \
  /opt/spark/bin/spark-submit \
    --packages "$PKGS" \
    --conf spark.jars.ivy=/tmp/ivy \
    --conf spark.ui.enabled=false \
    /work/spark-query.py
