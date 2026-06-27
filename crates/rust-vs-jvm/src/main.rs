//! Rust-vs-JVM scan benchmark — SCAFFOLD.
//!
//! Intended work: have Sail (Rust) and a JVM engine (Spark or Trino) scan the
//! *same* Iceberg tables in the *same* MinIO, applying predicates so partition/file
//! pruning is exercised, and compare latency + rows/s.
//!
//! Prerequisite: a JVM engine added to the docker stack. Until then this binary
//! compiles, reads the shared [`BenchConfig`], and emits a `Scaffold`
//! [`BenchReport`]. NO Sail dependency is pulled in yet.

use catalog_bench_common::{BenchConfig, BenchReport, Phase};
use clap::Parser;

const NOTES: &str = "Sail (Rust) vs Spark/Trino (JVM) scanning the same Iceberg tables in the \
same MinIO with predicates (pruning). Requires: a JVM engine added to the docker stack.";

#[derive(Parser, Debug, Clone)]
#[command(about = "Sail (Rust) vs Spark/Trino (JVM) Iceberg scan (scaffold)")]
struct Args {
    /// Namespace.table to scan once wired.
    #[arg(long, default_value = "rvj_bench.scan")]
    table: String,

    /// Predicate to push down (exercises pruning), e.g. "value > 100".
    #[arg(long, default_value = "value > 100")]
    predicate: String,

    /// Scan repetitions per engine.
    #[arg(long, default_value_t = 20)]
    iterations: u64,
}

struct Config {
    #[allow(dead_code)]
    shared: BenchConfig,
    #[allow(dead_code)]
    args: Args,
}

/// Where the measured phases will be produced once both engines are wired.
fn planned_phases(_cfg: &Config) -> Vec<Phase> {
    // TODO(rust-vs-jvm): with Sail + a JVM engine in the docker stack:
    //   1. sail-scan — run the predicate scan through Sail, record latency + rows/s
    //      -> Phase { name: "sail-scan", ... }.
    //   2. jvm-scan  — run the identical scan through Spark/Trino against the same
    //      MinIO tables -> Phase { name: "jvm-scan", ... }.
    // Both must read the same files and apply the same pruning predicate.
    Vec::new()
}

fn run(cfg: &Config) -> BenchReport {
    let phases = planned_phases(cfg);
    let mut report = BenchReport::scaffold("rust-vs-jvm", NOTES);
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
