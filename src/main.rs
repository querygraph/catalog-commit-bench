//! Commit-path benchmark for Iceberg REST catalogs.
//!
//! Every supported catalog (LakeCat, Apache Polaris, Apache Gravitino, Unity
//! Catalog OSS) speaks the Iceberg REST Catalog protocol, so this driver talks
//! pure REST and is catalog-agnostic: point `--base-url`/`--prefix`/`--token`
//! at any of them.
//!
//! It isolates the *catalog commit transaction* by issuing `set-properties`
//! commits (no data files): each commit still forces the catalog through
//! update validation, a new metadata write, the metadata-pointer CAS, and
//! durable persistence — which is exactly the cost LakeCat's design is about,
//! and the part TPC-style engine benchmarks never measure.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde_json::{json, Value};

#[derive(Parser, Debug, Clone)]
#[command(about = "Iceberg REST catalog commit benchmark")]
struct Args {
    /// Base URL up to and including any catalog-specific path prefix, e.g.
    /// LakeCat: http://127.0.0.1:3000/catalog  Polaris: http://127.0.0.1:8181/api/catalog
    #[arg(long)]
    base_url: String,

    /// Iceberg REST prefix segment (warehouse/catalog/metalake). May be empty.
    #[arg(long, default_value = "")]
    prefix: String,

    #[arg(long, default_value = "commit_bench")]
    namespace: String,

    #[arg(long, default_value = "commits")]
    table: String,

    /// Optional bearer token (Authorization: Bearer ...).
    #[arg(long)]
    token: Option<String>,

    /// Create the namespace and table before benchmarking.
    #[arg(long)]
    create: bool,

    /// Warmup commits (not measured).
    #[arg(long, default_value_t = 50)]
    warmup: u64,

    /// Optional explicit table location for createTable (e.g. s3://warehouse/lakecat).
    /// Lets a catalog that doesn't derive a warehouse location write metadata to
    /// the same object store as the others (used for LakeCat -> MinIO).
    #[arg(long)]
    location: Option<String>,

    /// Sequential commits to measure for the latency phase.
    #[arg(long, default_value_t = 1000)]
    iterations: u64,

    /// Concurrent writers for the throughput phase.
    #[arg(long, default_value_t = 8)]
    concurrency: u64,

    /// Duration of the concurrent throughput phase, seconds.
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,

    /// Send a LakeCat-style Idempotency-Key on each commit (ignored by catalogs
    /// that do not implement it).
    #[arg(long)]
    idempotency: bool,

    /// Path suffix appended to the table URL for a commit. The Iceberg REST spec
    /// uses a bare `POST .../tables/{table}` (default ""), so Polaris, Gravitino,
    /// and Unity need no suffix. LakeCat mounts commit at `.../tables/{table}/commit`,
    /// so pass `--commit-suffix /commit` for it.
    #[arg(long, default_value = "")]
    commit_suffix: String,

    /// Emit a machine-readable JSON summary instead of text.
    #[arg(long)]
    json: bool,
}

struct Catalog {
    http: reqwest::Client,
    base_url: String,
    prefix: String,
    token: Option<String>,
    commit_suffix: String,
    location: Option<String>,
}

impl Catalog {
    fn table_path(&self, ns: &str, table: &str) -> String {
        self.scoped(&format!("namespaces/{ns}/tables/{table}"))
    }
    fn tables_path(&self, ns: &str) -> String {
        self.scoped(&format!("namespaces/{ns}/tables"))
    }
    fn namespaces_path(&self) -> String {
        self.scoped("namespaces")
    }
    fn scoped(&self, tail: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        if self.prefix.is_empty() {
            format!("{base}/v1/{tail}")
        } else {
            format!("{base}/v1/{}/{tail}", self.prefix)
        }
    }
    fn req(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    async fn create_namespace(&self, ns: &str) -> Result<()> {
        let resp = self
            .req(self.http.post(self.namespaces_path()))
            .json(&json!({ "namespace": [ns], "properties": {} }))
            .send()
            .await?;
        // 200/201 created, 409 already exists are both fine.
        if resp.status().is_success() || resp.status().as_u16() == 409 {
            return Ok(());
        }
        bail!("create namespace failed: {} {}", resp.status(), resp.text().await.unwrap_or_default());
    }

    async fn create_table(&self, ns: &str, table: &str) -> Result<()> {
        let mut body = json!({
            "name": table,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": false, "type": "long"}
                ]
            },
            "properties": {"bench.counter": "0"},
            // Optional in the spec, but Nessie requires it; harmless elsewhere.
            "stage-create": false
        });
        if let Some(loc) = &self.location {
            body["location"] = json!(loc);
        }
        let resp = self
            .req(self.http.post(self.tables_path(ns)))
            .json(&body)
            .send()
            .await?;
        if resp.status().is_success() || resp.status().as_u16() == 409 {
            return Ok(());
        }
        bail!("create table failed: {} {}", resp.status(), resp.text().await.unwrap_or_default());
    }

    async fn load_table_uuid(&self, ns: &str, table: &str) -> Result<String> {
        let resp = self
            .req(self.http.get(self.table_path(ns, table)))
            .send()
            .await
            .context("loadTable request")?;
        if !resp.status().is_success() {
            bail!("loadTable failed: {} {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        let v: Value = resp.json().await?;
        v.pointer("/metadata/table-uuid")
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("response missing metadata.table-uuid")
    }

    /// Issue one set-properties commit. Returns Ok(true) on success, Ok(false)
    /// on a 409 commit conflict, Err on anything else.
    async fn commit(&self, ns: &str, table: &str, uuid: &str, counter: u64, idem: bool) -> Result<bool> {
        let body = json!({
            "requirements": [{"type": "assert-table-uuid", "uuid": uuid}],
            "updates": [{
                "action": "set-properties",
                "updates": {"bench.counter": counter.to_string()}
            }]
        });
        let url = format!("{}{}", self.table_path(ns, table), self.commit_suffix);
        let mut rb = self.req(self.http.post(url)).json(&body);
        if idem {
            rb = rb.header("Idempotency-Key", format!("bench-{uuid}-{counter}"));
        }
        let resp = rb.send().await?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            Ok(true)
        } else if status == 409 {
            Ok(false)
        } else {
            bail!("commit failed: {} {}", status, resp.text().await.unwrap_or_default());
        }
    }
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted_ms.len() as f64 - 1.0)).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cat = Arc::new(Catalog {
        http: reqwest::Client::builder().pool_max_idle_per_host(64).build()?,
        base_url: args.base_url.clone(),
        prefix: args.prefix.clone(),
        token: args.token.clone(),
        commit_suffix: args.commit_suffix.clone(),
        location: args.location.clone(),
    });

    if args.create {
        cat.create_namespace(&args.namespace).await?;
        cat.create_table(&args.namespace, &args.table).await?;
    }
    let uuid = cat.load_table_uuid(&args.namespace, &args.table).await?;

    // Warmup.
    for i in 0..args.warmup {
        cat.commit(&args.namespace, &args.table, &uuid, 1_000_000 + i, args.idempotency).await?;
    }

    // --- Sequential latency phase ---
    let mut lat_ms: Vec<f64> = Vec::with_capacity(args.iterations as usize);
    let seq_start = Instant::now();
    for i in 0..args.iterations {
        let t = Instant::now();
        cat.commit(&args.namespace, &args.table, &uuid, i, args.idempotency).await?;
        lat_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let seq_elapsed = seq_start.elapsed().as_secs_f64();
    lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let seq_throughput = args.iterations as f64 / seq_elapsed;

    // --- Concurrent throughput phase ---
    let stop = Arc::new(tokio::sync::Notify::new());
    let ok = Arc::new(AtomicU64::new(0));
    let conflict = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let conc_start = Instant::now();
    let mut handles = Vec::new();
    for w in 0..args.concurrency {
        let (cat, ns, table, uuid) = (cat.clone(), args.namespace.clone(), args.table.clone(), uuid.clone());
        let (ok, conflict) = (ok.clone(), conflict.clone());
        let idem = args.idempotency;
        handles.push(tokio::spawn(async move {
            let mut n = w * 10_000_000;
            while Instant::now() < deadline {
                n += 1;
                match cat.commit(&ns, &table, &uuid, n, idem).await {
                    Ok(true) => { ok.fetch_add(1, Ordering::Relaxed); }
                    Ok(false) => { conflict.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => { /* transient; keep going */ }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let _ = stop; // reserved for future signal-based stop
    let conc_elapsed = conc_start.elapsed().as_secs_f64();
    let ok_n = ok.load(Ordering::Relaxed);
    let conflict_n = conflict.load(Ordering::Relaxed);
    let conc_throughput = ok_n as f64 / conc_elapsed;
    let conflict_rate = if ok_n + conflict_n > 0 {
        conflict_n as f64 / (ok_n + conflict_n) as f64
    } else {
        0.0
    };

    if args.json {
        let out = json!({
            "target": { "base_url": args.base_url, "prefix": args.prefix,
                        "namespace": args.namespace, "table": args.table,
                        "idempotency": args.idempotency },
            "sequential": {
                "iterations": args.iterations,
                "elapsed_secs": seq_elapsed,
                "throughput_commits_per_s": seq_throughput,
                "latency_ms": {
                    "p50": percentile(&lat_ms, 50.0),
                    "p90": percentile(&lat_ms, 90.0),
                    "p99": percentile(&lat_ms, 99.0),
                    "max": lat_ms.last().copied().unwrap_or(0.0),
                }
            },
            "concurrent": {
                "writers": args.concurrency,
                "duration_secs": conc_elapsed,
                "ok": ok_n, "conflicts": conflict_n,
                "throughput_commits_per_s": conc_throughput,
                "conflict_rate": conflict_rate,
            }
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("catalog-commit-bench  ->  {}  (prefix='{}', table={}/{})",
            args.base_url, args.prefix, args.namespace, args.table);
        println!("  idempotency header: {}", args.idempotency);
        println!();
        println!("Sequential commit latency ({} commits):", args.iterations);
        println!("  throughput : {:>8.1} commits/s", seq_throughput);
        println!("  p50        : {:>8.3} ms", percentile(&lat_ms, 50.0));
        println!("  p90        : {:>8.3} ms", percentile(&lat_ms, 90.0));
        println!("  p99        : {:>8.3} ms", percentile(&lat_ms, 99.0));
        println!("  max        : {:>8.3} ms", lat_ms.last().copied().unwrap_or(0.0));
        println!();
        println!("Concurrent throughput ({} writers, {:.1}s):", args.concurrency, conc_elapsed);
        println!("  committed  : {ok_n}");
        println!("  conflicts  : {conflict_n}  (rate {:.2}%)", conflict_rate * 100.0);
        println!("  throughput : {:>8.1} commits/s", conc_throughput);
    }
    Ok(())
}
