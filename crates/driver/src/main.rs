//! `catalog-bench` — the suite driver.
//!
//! ```text
//! catalog-bench list                 # show every bench + its status
//! catalog-bench run commit -- ...    # run one bench (args after `--` pass through)
//! catalog-bench run all              # run every Ready bench, aggregate the reports
//! catalog-bench run all --include-scaffold
//! ```
//!
//! Each bench is an independent binary that prints a [`BenchReport`] as JSON to
//! stdout; the driver builds+spawns the sibling binary via `cargo run -q -p <pkg>`,
//! captures that JSON, and pretty-prints a summary.

use std::process::{Command, ExitCode};

use catalog_bench_common::{BenchReport, BenchStatus};
use clap::{Parser, Subcommand};

/// Static description of one benchmark in the suite.
struct BenchSpec {
    /// Short name used on the CLI (`run <name>`).
    name: &'static str,
    /// Cargo package + binary name of the sibling crate.
    package: &'static str,
    /// Whether the bench does real measured work yet.
    status: BenchStatus,
    /// One-line description for `list`.
    description: &'static str,
}

/// The suite registry. Order is the display order.
const BENCHES: &[BenchSpec] = &[
    BenchSpec {
        name: "commit",
        package: "catalog-bench-commit",
        status: BenchStatus::Ready,
        description:
            "Iceberg REST commit-path latency + throughput (LakeCat/Polaris/Gravitino/Nessie).",
    },
    BenchSpec {
        name: "write-data",
        package: "catalog-bench-write-data",
        status: BenchStatus::Ready,
        description: "Real Parquet data files -> MinIO/S3 + a LakeCat commit (partial-real).",
    },
    BenchSpec {
        name: "cache-scan",
        package: "catalog-bench-cache-scan",
        status: BenchStatus::Ready,
        description: "Cold vs warm Parquet scan via Sail's Foyer object-store cache (MinIO/S3).",
    },
    BenchSpec {
        name: "rust-vs-jvm",
        package: "catalog-bench-rust-vs-jvm",
        status: BenchStatus::Ready,
        description:
            "Sail/DataFusion (Rust) vs Spark (JVM): same filter+aggregate over same MinIO Parquet \
(Spark phase needs Docker; falls back to Rust-only + container recipe otherwise).",
    },
    BenchSpec {
        name: "read-write",
        package: "catalog-bench-read-write",
        status: BenchStatus::Scaffold,
        description: "Full INSERT + filtered-scan workload through LakeCat + Sail (scaffold).",
    },
];

fn find_bench(name: &str) -> Option<&'static BenchSpec> {
    BENCHES.iter().find(|b| b.name == name)
}

#[derive(Parser, Debug)]
#[command(name = "catalog-bench", about = "Driver for the catalog-bench suite")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// List every benchmark with its status and description.
    List,
    /// Run one benchmark by name, or `all`. Args after `--` pass through to the bench.
    Run {
        /// Bench name (see `list`) or `all`.
        name: String,
        /// Also attempt Scaffold benches (they normally just emit a placeholder).
        #[arg(long)]
        include_scaffold: bool,
        /// Build in release mode (slower to compile, realistic numbers).
        #[arg(long)]
        release: bool,
        /// Arguments forwarded verbatim to the bench binary (after `--`).
        #[arg(last = true)]
        passthrough: Vec<String>,
    },
}

/// The result of attempting to run a single bench.
enum Outcome {
    /// Bench ran and produced a report.
    Ran(BenchReport),
    /// Bench was skipped (scaffold, not requested).
    Skipped(&'static str),
    /// Bench failed; the string is a human-readable reason.
    Failed(String),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::List => {
            cmd_list();
            ExitCode::SUCCESS
        }
        Cmd::Run {
            name,
            include_scaffold,
            release,
            passthrough,
        } => cmd_run(&name, include_scaffold, release, &passthrough),
    }
}

fn cmd_list() {
    println!("catalog-bench suite — {} benchmarks\n", BENCHES.len());
    println!("  {:<13} {:<9} DESCRIPTION", "NAME", "STATUS");
    println!("  {:<13} {:<9} -----------", "----", "------");
    for b in BENCHES {
        println!("  {:<13} {:<9} {}", b.name, b.status.label(), b.description);
    }
    println!("\nRun one:  catalog-bench run <name> -- <bench args>");
    println!("Run all:  catalog-bench run all [--include-scaffold]");
}

fn cmd_run(name: &str, include_scaffold: bool, release: bool, passthrough: &[String]) -> ExitCode {
    if name == "all" {
        return run_all(include_scaffold, release, passthrough);
    }

    let Some(spec) = find_bench(name) else {
        eprintln!("error: unknown bench '{name}'. Known benches:");
        for b in BENCHES {
            eprintln!("  - {}", b.name);
        }
        return ExitCode::FAILURE;
    };

    match run_one(spec, release, passthrough) {
        Outcome::Ran(report) => {
            print_report(&report);
            ExitCode::SUCCESS
        }
        Outcome::Skipped(reason) => {
            println!("skipped {}: {reason}", spec.name);
            ExitCode::SUCCESS
        }
        Outcome::Failed(err) => {
            eprintln!("error: bench '{}' failed: {err}", spec.name);
            ExitCode::FAILURE
        }
    }
}

fn run_all(include_scaffold: bool, release: bool, passthrough: &[String]) -> ExitCode {
    let mut results: Vec<(&'static BenchSpec, Outcome)> = Vec::new();
    for spec in BENCHES {
        if spec.status == BenchStatus::Scaffold && !include_scaffold {
            results.push((spec, Outcome::Skipped("scaffold (use --include-scaffold)")));
            continue;
        }
        eprintln!("==> running {} ({})", spec.name, spec.status.label());
        results.push((spec, run_one(spec, release, passthrough)));
    }
    print_summary(&results);
    // `run all` is best-effort: a single bench failure does not fail the whole
    // invocation, so the summary is always shown. Exit non-zero only if every
    // attempted (non-skipped) bench failed.
    let attempted = results
        .iter()
        .filter(|(_, o)| !matches!(o, Outcome::Skipped(_)))
        .count();
    let failed = results
        .iter()
        .filter(|(_, o)| matches!(o, Outcome::Failed(_)))
        .count();
    if attempted > 0 && failed == attempted {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Build + spawn a sibling bench binary and capture its `BenchReport`.
fn run_one(spec: &BenchSpec, release: bool, passthrough: &[String]) -> Outcome {
    // Re-use the cargo that invoked us (set when run via `cargo run`); fall back
    // to PATH lookup otherwise.
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut cmd = Command::new(cargo);
    cmd.args(["run", "-q", "-p", spec.package]);
    if release {
        cmd.arg("--release");
    }
    cmd.arg("--");
    cmd.args(passthrough);

    // Child inherits our environment (so shared BenchConfig env vars flow through).
    let output = match cmd.output() {
        Ok(o) => o,
        Err(e) => {
            return Outcome::Failed(format!(
                "could not spawn `cargo run -p {}`: {e}",
                spec.package
            ))
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tail = last_lines(&stderr, 8);
        return Outcome::Failed(format!(
            "{} exited with {} — stderr tail:\n{tail}",
            spec.package, output.status
        ));
    }

    match parse_report(&stdout) {
        Some(report) => Outcome::Ran(report),
        None => Outcome::Failed(format!(
            "{} produced no parseable BenchReport on stdout (got {} bytes)",
            spec.package,
            stdout.len()
        )),
    }
}

/// Parse a `BenchReport` from a bench's stdout. The bench prints exactly one JSON
/// object, but be lenient and scan for the first `{` so stray lines don't break us.
fn parse_report(stdout: &str) -> Option<BenchReport> {
    let trimmed = stdout.trim();
    if let Ok(r) = serde_json::from_str::<BenchReport>(trimmed) {
        return Some(r);
    }
    let start = trimmed.find('{')?;
    serde_json::from_str::<BenchReport>(&trimmed[start..]).ok()
}

fn last_lines(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

fn print_report(report: &BenchReport) {
    println!("\nbench: {}  [{}]", report.name, report.status.label());
    if let Some(notes) = &report.notes {
        println!("  notes: {notes}");
    }
    if report.phases.is_empty() {
        println!("  (no measured phases)");
        return;
    }
    println!(
        "  {:<16} {:>10} {:>10} {:>10} {:>14}",
        "PHASE", "SAMPLES", "P50(ms)", "P95(ms)", "THROUGHPUT/s"
    );
    for p in &report.phases {
        println!(
            "  {:<16} {:>10} {:>10.3} {:>10.3} {:>14.1}",
            p.name, p.samples, p.p50_ms, p.p95_ms, p.throughput_per_s
        );
    }
}

fn print_summary(results: &[(&'static BenchSpec, Outcome)]) {
    println!("\n=== catalog-bench: combined summary ===\n");
    println!("  {:<13} {:<9} RESULT", "NAME", "STATUS");
    println!("  {:<13} {:<9} ------", "----", "------");
    for (spec, outcome) in results {
        let result = match outcome {
            Outcome::Ran(report) => summarize_phases(report),
            Outcome::Skipped(reason) => format!("skipped ({reason})"),
            Outcome::Failed(err) => format!("FAILED ({})", first_line(err)),
        };
        println!("  {:<13} {:<9} {}", spec.name, spec.status.label(), result);
    }
    // Detail block for benches that actually ran.
    for (_, outcome) in results {
        if let Outcome::Ran(report) = outcome {
            print_report(report);
        }
    }
}

fn summarize_phases(report: &BenchReport) -> String {
    if report.phases.is_empty() {
        return "ran (no phases)".to_string();
    }
    let parts: Vec<String> = report
        .phases
        .iter()
        .map(|p| {
            format!(
                "{} p50={:.2}ms {:.1}/s",
                p.name, p.p50_ms, p.throughput_per_s
            )
        })
        .collect();
    parts.join("; ")
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}
