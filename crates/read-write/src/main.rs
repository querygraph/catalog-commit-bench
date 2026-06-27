//! Read-write workload benchmark — SCAFFOLD.
//!
//! Intended work: a full INSERT + filtered-scan workload through LakeCat + Sail —
//! append real data to an Iceberg table via the catalog, then run filtered scans
//! over it, measuring both write and read phases end-to-end.
//!
//! Prerequisite: verification of Sail's write path through the LakeCat REST
//! catalog (a true Iceberg append, not just the metadata commit the `write-data`
//! bench does today). Until then this binary compiles, reads the shared
//! [`BenchConfig`], and emits a `Scaffold` [`BenchReport`]. NO Sail dependency is
//! pulled in yet.

use catalog_bench_common::{BenchConfig, BenchReport, Phase};
use clap::Parser;

const NOTES: &str = "Full INSERT + filtered-scan workload through LakeCat + Sail. \
Requires: verification of Sail's write path through the LakeCat REST catalog.";

#[derive(Parser, Debug, Clone)]
#[command(about = "INSERT + filtered-scan workload through LakeCat + Sail (scaffold)")]
struct Args {
    /// Namespace.table to write + scan once wired.
    #[arg(long, default_value = "rw_bench.events")]
    table: String,

    /// Rows to INSERT per batch.
    #[arg(long, default_value_t = 10_000)]
    rows: u64,

    /// Filter predicate for the read phase.
    #[arg(long, default_value = "value > 0")]
    predicate: String,

    /// Iterations per phase.
    #[arg(long, default_value_t = 20)]
    iterations: u64,
}

struct Config {
    #[allow(dead_code)]
    shared: BenchConfig,
    #[allow(dead_code)]
    args: Args,
}

/// Where the measured phases will be produced once Sail's write path is verified.
fn planned_phases(_cfg: &Config) -> Vec<Phase> {
    // TODO(read-write): with a verified Sail write path through LakeCat:
    //   1. insert — append `rows` real rows as an Iceberg data file + commit
    //      through LakeCat -> Phase { name: "insert", ... } (rows/s + commit p50/p95).
    //   2. scan   — run the filtered scan over the freshly written table through
    //      Sail -> Phase { name: "filtered-scan", ... } (latency p50/p95 + rows/s).
    Vec::new()
}

fn run(cfg: &Config) -> BenchReport {
    let phases = planned_phases(cfg);
    let mut report = BenchReport::scaffold("read-write", NOTES);
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
