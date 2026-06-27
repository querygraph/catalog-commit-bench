//! Cold-vs-warm scan benchmark — SCAFFOLD.
//!
//! Intended work: scan an Iceberg table on MinIO twice through Sail — once cold
//! (every object is an S3 miss) and once warm (served from the Foyer object-store
//! cache) — and compare latency + rows/s.
//!
//! Prerequisite: the Sail Foyer cache layer (querygraph/sail branch
//! `feat/object-store-foyer-cache`) plus Sail scan wiring. Until that is wired,
//! this binary compiles, reads the shared [`BenchConfig`], and emits a `Scaffold`
//! [`BenchReport`]. NO Sail dependency is pulled in yet.

use catalog_bench_common::{BenchConfig, BenchReport, Phase};
use clap::Parser;

const NOTES: &str = "Cold vs warm scan via Sail + the Foyer object-store cache. \
Requires: the Sail foyer cache layer (querygraph/sail branch feat/object-store-foyer-cache) \
+ Sail scan wiring. Phases: cold-scan (S3 miss), warm-scan (Foyer hit), measuring p50/p95 \
latency + rows/s.";

#[derive(Parser, Debug, Clone)]
#[command(about = "Cold vs warm Iceberg scan via Sail + Foyer cache (scaffold)")]
struct Args {
    /// Namespace.table to scan once wired.
    #[arg(long, default_value = "cache_bench.scan")]
    table: String,

    /// Number of scan repetitions per (cold/warm) phase.
    #[arg(long, default_value_t = 20)]
    iterations: u64,
}

/// Bench-specific config: the shared environment plus this bench's knobs.
struct Config {
    #[allow(dead_code)]
    shared: BenchConfig,
    #[allow(dead_code)]
    args: Args,
}

/// Where the measured phases will be produced once Sail scan + Foyer are wired.
fn planned_phases(_cfg: &Config) -> Vec<Phase> {
    // TODO(cache-scan): with Sail's scan engine + Foyer object-store cache:
    //   1. cold-scan  — drop/disable the cache, scan the table from S3, record
    //      per-scan latency and rows read -> Phase { name: "cold-scan", ... }.
    //   2. warm-scan  — repeat the scan so Foyer serves the objects from cache,
    //      record latency + rows/s -> Phase { name: "warm-scan", ... }.
    // The speedup = cold p50 / warm p50.
    Vec::new()
}

fn run(cfg: &Config) -> BenchReport {
    let phases = planned_phases(cfg);
    // Scaffold: no measured phases yet; carry the prerequisite in `notes`.
    let mut report = BenchReport::scaffold("cache-scan", NOTES);
    report.phases = phases;
    report
}

fn main() {
    let cfg = Config {
        shared: BenchConfig::from_env(),
        args: Args::parse(),
    };
    run(&cfg).print_stdout();
}
