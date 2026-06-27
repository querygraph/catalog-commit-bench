#!/usr/bin/env python3
"""Steady-state warm Spark query timing for the rust-vs-jvm bench.

Runs the SAME filter+aggregate query as the Rust/DataFusion side over the SAME
Parquet files in the SAME MinIO, inside ONE long-lived Spark session, N+1 times.
The first (cold) run is discarded; the warm median/p95 of the rest is reported.
JVM startup + JIT warmup + the cold first scan are deliberately excluded — this
is the JVM's best case (see RESULTS.md "Why Rust did not make it fast").

Output: exactly one line on stdout beginning with `SPARK_RESULT ` followed by a
JSON object, so the Rust harness can scrape it from the noisy Spark log stream.

Config is read from the environment (set by the Rust harness via `docker run -e`):
  S3_ENDPOINT  e.g. http://host.docker.internal:9000
  S3_KEY       MinIO access key
  S3_SECRET    MinIO secret key
  S3_PATH      e.g. s3a://warehouse/cache-scan/
  ITERS        warm iterations (cold run is run in addition and discarded)
"""

import json
import os
import statistics
import sys
import time

from pyspark.sql import SparkSession

# The identical query the Rust/DataFusion side runs. Column names match the
# `cache-scan` dataset written by crates/cache-scan: id, measure_a (i64),
# measure_b (f64), grp (low-cardinality string). The WHERE keeps ~all rows
# (measure_a is derived from the top bits of an LCG, hence always >= 0), so the
# query is scan-bound, not pruning-bound.
QUERY = (
    "SELECT grp, count(*) AS n, sum(measure_a) AS s1, avg(measure_b) AS a2 "
    "FROM cache_scan WHERE measure_a > 0 GROUP BY grp ORDER BY grp"
)


def main() -> int:
    endpoint = os.environ.get("S3_ENDPOINT", "http://host.docker.internal:9000")
    key = os.environ.get("S3_KEY", "admin")
    secret = os.environ.get("S3_SECRET", "password")
    path = os.environ.get("S3_PATH", "s3a://warehouse/cache-scan/")
    iters = int(os.environ.get("ITERS", "8"))

    spark = (
        SparkSession.builder.appName("rust-vs-jvm")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Keep the comparison single-threaded-ish and deterministic; let Spark use
        # all cores for the scan (default). Reduce shuffle partitions: 8 groups.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Register the SAME files as a temp view; read once, cache=False so every
    # query re-scans the Parquet (we are measuring scan+aggregate, not Spark's
    # in-memory cache).
    df = spark.read.parquet(path)
    df.createOrReplaceTempView("cache_scan")

    def run_once():
        t0 = time.perf_counter()
        rows = spark.sql(QUERY).collect()
        dt = (time.perf_counter() - t0) * 1000.0
        return dt, rows

    # Cold run (discarded): triggers metadata load, JIT, connection warmup.
    cold_ms, rows = run_once()
    scanned = int(sum(r["n"] for r in rows))
    groups = len(rows)

    warm = []
    for _ in range(max(1, iters)):
        dt, _ = run_once()
        warm.append(dt)
    warm.sort()

    def pct(data, p):
        if not data:
            return 0.0
        idx = min(len(data) - 1, round((p / 100.0) * (len(data) - 1)))
        return data[idx]

    result = {
        "warm_p50_ms": statistics.median(warm),
        "warm_p95_ms": pct(warm, 95),
        "warm_min_ms": warm[0],
        "samples": len(warm),
        "cold_ms": cold_ms,
        "scanned_rows": scanned,
        "groups": groups,
        "engine": "spark",
        "spark_version": spark.version,
    }
    # Single scrape-able line.
    print("SPARK_RESULT " + json.dumps(result))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
