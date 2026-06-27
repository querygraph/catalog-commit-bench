//! Shared building blocks for the `catalog-bench` suite.
//!
//! Every benchmark in the suite is an independent binary, but they all speak the
//! same vocabulary:
//!
//! * [`BenchReport`] / [`Phase`] / [`BenchStatus`] — the JSON a bench prints to
//!   stdout so the [`catalog-bench`](../catalog_bench/index.html) driver can
//!   aggregate results uniformly.
//! * [`BenchConfig`] — the shared environment (MinIO/S3 + catalog base URLs +
//!   warehouse) read from the *same* env vars the existing commit bench and
//!   `docker-compose.yml` already use.
//! * [`percentile`] / [`throughput`] — the latency/throughput math used to fill
//!   in [`Phase`].

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Whether a benchmark is wired up end-to-end (`Ready`) or still a placeholder
/// awaiting a prerequisite (`Scaffold`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchStatus {
    /// The benchmark runs real work and produces measured numbers.
    Ready,
    /// The benchmark compiles and emits a report, but the measured work is not
    /// wired yet (see the report's `notes` for the prerequisite).
    Scaffold,
}

impl BenchStatus {
    /// Short, fixed-width label for table output.
    pub fn label(self) -> &'static str {
        match self {
            BenchStatus::Ready => "ready",
            BenchStatus::Scaffold => "scaffold",
        }
    }
}

impl std::fmt::Display for BenchStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// One measured phase of a benchmark (e.g. "sequential" vs "concurrent").
///
/// Extra, bench-specific fields are allowed and round-trip through `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase {
    pub name: String,
    pub samples: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub throughput_per_s: f64,
    /// Optional bench-specific metrics (p99, bytes/s, conflict rate, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

impl Phase {
    /// Build a phase from a slice of per-sample durations + the wall-clock the
    /// phase took (used for throughput).
    pub fn from_samples(name: impl Into<String>, samples: &[Duration], elapsed: Duration) -> Self {
        Phase {
            name: name.into(),
            samples: samples.len() as u64,
            p50_ms: percentile(samples, 50.0),
            p95_ms: percentile(samples, 95.0),
            throughput_per_s: throughput(samples.len() as u64, elapsed),
            extra: None,
        }
    }
}

/// The machine-readable result a benchmark prints to stdout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub name: String,
    pub status: BenchStatus,
    pub phases: Vec<Phase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

impl BenchReport {
    /// A `Scaffold` report carrying only the prerequisite `notes` (no measured
    /// phases). Used by the placeholder benches.
    pub fn scaffold(name: impl Into<String>, notes: impl Into<String>) -> Self {
        BenchReport {
            name: name.into(),
            status: BenchStatus::Scaffold,
            phases: Vec::new(),
            notes: Some(notes.into()),
        }
    }

    /// Serialize to pretty JSON. Falls back to a minimal hand-written object if
    /// serialization somehow fails, so a bench never panics on output.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| {
            format!(
                "{{\"name\":\"{}\",\"status\":\"{}\",\"phases\":[]}}",
                self.name, self.status
            )
        })
    }

    /// Print the report as JSON to stdout (the channel the driver reads).
    pub fn print_stdout(&self) {
        println!("{}", self.to_json());
    }
}

/// Shared environment for every bench: the MinIO/S3 object store and the catalog
/// endpoints. Read from env vars, reusing the names the existing commit bench,
/// `docker-compose.yml`, and `run-bench.sh` already use.
///
/// | Field | Env var | Default | Origin |
/// |---|---|---|---|
/// | `s3_endpoint` | `AWS_ENDPOINT` | `http://127.0.0.1:9000` | reused (docker-compose) |
/// | `s3_access_key` | `AWS_ACCESS_KEY_ID` | `admin` | reused (docker-compose) |
/// | `s3_secret_key` | `AWS_SECRET_ACCESS_KEY` | `password` | reused (docker-compose) |
/// | `s3_region` | `AWS_REGION` | `us-east-1` | reused (docker-compose) |
/// | `s3_allow_http` | `AWS_ALLOW_HTTP` | `true` | reused (docker-compose) |
/// | `s3_bucket` | `BENCH_S3_BUCKET` | `warehouse` | new (the `s3://warehouse` bucket) |
/// | `warehouse` | `BENCH_WAREHOUSE` | `warehouse` | new (warehouse name) |
/// | `lakecat_base` | `LAKECAT_BASE` | `http://127.0.0.1:8181/catalog` | reused (run-bench.sh) |
/// | `nessie_base` | `NESSIE_BASE` | `http://127.0.0.1:19120/iceberg` | reused (bench-stack.sh value) |
/// | `gravitino_base` | `GRAVITINO_BASE` | `http://127.0.0.1:9001/iceberg` | reused (run-bench.sh) |
/// | `polaris_base` | `POLARIS_BASE` | `http://127.0.0.1:8182/api/catalog` | reused (run-bench.sh) |
#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_region: String,
    pub s3_allow_http: bool,
    pub s3_bucket: String,
    pub warehouse: String,
    pub lakecat_base: String,
    pub nessie_base: String,
    pub gravitino_base: String,
    pub polaris_base: String,
}

impl BenchConfig {
    /// Read the shared config from the environment, applying the documented
    /// defaults for anything unset.
    pub fn from_env() -> Self {
        BenchConfig {
            s3_endpoint: env_or("AWS_ENDPOINT", "http://127.0.0.1:9000"),
            s3_access_key: env_or("AWS_ACCESS_KEY_ID", "admin"),
            s3_secret_key: env_or("AWS_SECRET_ACCESS_KEY", "password"),
            s3_region: env_or("AWS_REGION", "us-east-1"),
            s3_allow_http: env_bool("AWS_ALLOW_HTTP", true),
            s3_bucket: env_or("BENCH_S3_BUCKET", "warehouse"),
            warehouse: env_or("BENCH_WAREHOUSE", "warehouse"),
            lakecat_base: env_or("LAKECAT_BASE", "http://127.0.0.1:8181/catalog"),
            nessie_base: env_or("NESSIE_BASE", "http://127.0.0.1:19120/iceberg"),
            gravitino_base: env_or("GRAVITINO_BASE", "http://127.0.0.1:9001/iceberg"),
            polaris_base: env_or("POLARIS_BASE", "http://127.0.0.1:8182/api/catalog"),
        }
    }

    /// An `s3://<bucket>/<sub>` URI inside the configured warehouse bucket.
    pub fn s3_uri(&self, sub: &str) -> String {
        format!("s3://{}/{}", self.s3_bucket, sub.trim_start_matches('/'))
    }
}

impl Default for BenchConfig {
    fn default() -> Self {
        BenchConfig::from_env()
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

/// The `p`th percentile (0–100) of a set of durations, in milliseconds.
///
/// Uses nearest-rank on a sorted copy; returns `0.0` for an empty input.
pub fn percentile(samples: &[Duration], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut ms: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = p.clamp(0.0, 100.0);
    let idx = ((p / 100.0) * (ms.len() as f64 - 1.0)).round() as usize;
    ms[idx.min(ms.len() - 1)]
}

/// Operations per second for `count` operations over `elapsed`.
pub fn throughput(count: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        0.0
    } else {
        count as f64 / secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_basic() {
        let s: Vec<Duration> = (1..=100).map(|n| Duration::from_millis(n)).collect();
        assert!((percentile(&s, 50.0) - 50.0).abs() < 1.5);
        assert!((percentile(&s, 95.0) - 95.0).abs() < 1.5);
        assert_eq!(percentile(&[], 50.0), 0.0);
    }

    #[test]
    fn throughput_basic() {
        assert_eq!(throughput(100, Duration::from_secs(10)), 10.0);
        assert_eq!(throughput(5, Duration::ZERO), 0.0);
    }

    #[test]
    fn report_roundtrips() {
        let r = BenchReport::scaffold("x", "todo");
        let back: BenchReport = serde_json::from_str(&r.to_json()).unwrap();
        assert_eq!(back.status, BenchStatus::Scaffold);
    }
}
