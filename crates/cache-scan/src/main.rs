//! Cold-vs-warm scan benchmark — Sail's Foyer object-store cache over MinIO/S3.
//!
//! Measures the read advantage of Sail's local Foyer cache by fully scanning a
//! set of Parquet files on object storage three ways:
//!
//!   * **no-cache** — read every file through the raw `AmazonS3` store (no cache).
//!   * **cold**     — wrap the raw store in a FRESH [`CachingObjectStore`] (empty
//!     Foyer cache) and read every file once, populating the cache from MinIO.
//!   * **warm**     — read every file again through the SAME (now populated)
//!     caching store, so reads are served from the in-memory Foyer cache.
//!
//! "Read/scan" means fully decoding every row group of every file into Arrow
//! [`RecordBatch`]es and counting rows + bytes — a real table read, not a HEAD.
//!
//! The reader is the `parquet` async reader (`ParquetObjectReader` +
//! `ParquetRecordBatchStreamBuilder`) over the object store directly. That keeps
//! the dependency tree pinned to the exact `object_store 0.13.2` that
//! `sail-object-store` uses, so `CachingObjectStore`'s `Arc<dyn ObjectStore>`
//! typechecks against the reader, while still routing every byte through the
//! Foyer cache.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use catalog_bench_common::{percentile, BenchConfig, BenchReport, BenchStatus, Phase};
use clap::Parser;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use sail_object_store::{CacheConfig, CachingObjectStore};

const NOTES_CAVEAT: &str =
    "Cold = first read through a FRESH Foyer cache (fetched from MinIO + cached); \
warm = re-read through the now-populated Foyer cache; no-cache = read through the \
raw S3 store. throughput_per_s is rows/s; extra carries MB/s + bytes + wall_ms. \
CAVEAT: a LOCAL MinIO on loopback has tiny per-request latency, so this UNDERSTATES \
the cache win — against remote S3 (tens of ms per request, far higher for many \
small range reads) the warm-vs-cold and warm-vs-no-cache speedups are dramatically \
larger.";

#[derive(Parser, Debug, Clone)]
#[command(about = "Cold vs warm Parquet scan via Sail's Foyer object-store cache")]
struct Args {
    /// Object-store prefix (under the bucket) holding the dataset.
    #[arg(long, default_value = "cache-scan")]
    prefix: String,

    /// Number of Parquet files to write / scan.
    #[arg(long, default_value_t = 16)]
    files: usize,

    /// Rows per Parquet file.
    #[arg(long, default_value_t = 200_000)]
    rows: usize,

    /// Rows per row group (controls row-group count per file).
    #[arg(long, default_value_t = 50_000)]
    row_group: usize,

    /// Repetitions of the no-cache baseline phase (p50 over per-file reads).
    #[arg(long, default_value_t = 3)]
    no_cache_iters: usize,

    /// Repetitions of the warm phase (p50 over per-file reads).
    #[arg(long, default_value_t = 5)]
    warm_iters: usize,

    /// Rewrite the dataset even if it already exists.
    #[arg(long)]
    rewrite: bool,
}

/// A loaded file: its object-store path and byte size.
#[derive(Clone)]
struct DataFile {
    path: ObjPath,
    size: u64,
}

/// Outcome of scanning every file once: per-file read durations + totals.
struct ScanPass {
    per_file: Vec<Duration>,
    total_rows: u64,
    total_bytes: u64,
    wall: Duration,
}

fn build_s3(cfg: &BenchConfig) -> Result<Arc<dyn ObjectStore>> {
    // Path-style addressing (MinIO default): leave virtual-hosted style off.
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&cfg.s3_endpoint)
        .with_region(&cfg.s3_region)
        .with_bucket_name(&cfg.s3_bucket)
        .with_access_key_id(&cfg.s3_access_key)
        .with_secret_access_key(&cfg.s3_secret_key)
        .with_allow_http(cfg.s3_allow_http)
        .with_virtual_hosted_style_request(false)
        .build()
        .context("building AmazonS3 object store for MinIO")?;
    Ok(Arc::new(s3))
}

/// Ensure the warehouse bucket exists; create it via the `aws` CLI if missing.
/// Best-effort: a working bucket makes the probe succeed and we return early.
async fn ensure_bucket(store: &Arc<dyn ObjectStore>, cfg: &BenchConfig) -> Result<()> {
    let probe = ObjPath::from("cache-scan-bucket-probe");
    match store.head(&probe).await {
        Ok(_) => return Ok(()),
        Err(object_store::Error::NotFound { .. }) => return Ok(()), // bucket exists, object doesn't
        Err(_) => {}
    }
    // Could not reach the bucket; try to create it via the AWS CLI.
    let status = std::process::Command::new("aws")
        .args([
            "--endpoint-url",
            &cfg.s3_endpoint,
            "s3api",
            "create-bucket",
            "--bucket",
            &cfg.s3_bucket,
        ])
        .env("AWS_ACCESS_KEY_ID", &cfg.s3_access_key)
        .env("AWS_SECRET_ACCESS_KEY", &cfg.s3_secret_key)
        .env("AWS_REGION", &cfg.s3_region)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .status();
    match status {
        Ok(s) if s.success() => Ok(()),
        // create-bucket on an existing bucket returns non-zero; re-probe to confirm.
        _ => match store.head(&ObjPath::from("cache-scan-bucket-probe")).await {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(anyhow!("bucket '{}' unreachable: {e}", cfg.s3_bucket)),
        },
    }
}

fn dataset_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("measure_a", DataType::Int64, false),
        Field::new("measure_b", DataType::Float64, false),
        Field::new("grp", DataType::Utf8, false),
    ]))
}

/// Build one record batch of `n` rows starting at logical row `base`.
fn build_batch(schema: &Arc<Schema>, base: u64, n: usize) -> Result<RecordBatch> {
    let mut id = Vec::with_capacity(n);
    let mut a = Vec::with_capacity(n);
    let mut b = Vec::with_capacity(n);
    let mut g = Vec::with_capacity(n);
    // Deterministic pseudo-random via a simple LCG (no rand dep).
    let mut state = base.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
    for i in 0..n {
        let row = base + i as u64;
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        id.push(row as i64);
        a.push((state >> 33) as i64);
        b.push(((state >> 11) as f64) / 1_000_000.0);
        g.push(format!("g{}", state % 8));
    }
    let grp_refs: Vec<&str> = g.iter().map(String::as_str).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(id)),
            Arc::new(Int64Array::from(a)),
            Arc::new(Float64Array::from(b)),
            Arc::new(StringArray::from(grp_refs)),
        ],
    )
    .context("building record batch")
}

/// List the `.parquet` files under the prefix, sorted by path.
async fn list_files(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<Vec<DataFile>> {
    let p = ObjPath::from(prefix);
    let mut stream = store.list(Some(&p));
    let mut files = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.context("listing dataset objects")?;
        if meta.location.as_ref().ends_with(".parquet") {
            files.push(DataFile {
                path: meta.location,
                size: meta.size,
            });
        }
    }
    files.sort_by(|x, y| x.path.as_ref().cmp(y.path.as_ref()));
    Ok(files)
}

/// Write the dataset to the RAW store (idempotent unless `rewrite`).
async fn ensure_dataset(raw: &Arc<dyn ObjectStore>, args: &Args) -> Result<(Vec<DataFile>, bool)> {
    let existing = list_files(raw, &args.prefix).await?;
    if !args.rewrite && existing.len() >= args.files {
        return Ok((existing, false));
    }
    let schema = dataset_schema();
    eprintln!(
        "writing dataset: {} files x {} rows ({} rows/group) -> s3://.../{}/ ...",
        args.files, args.rows, args.row_group, args.prefix
    );
    for f in 0..args.files {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None)
                .context("creating parquet writer")?;
            let mut written = 0usize;
            let base = (f as u64) * (args.rows as u64);
            while written < args.rows {
                let n = args.row_group.min(args.rows - written);
                let batch = build_batch(&schema, base + written as u64, n)?;
                writer.write(&batch).context("writing row group")?;
                written += n;
            }
            writer.close().context("closing parquet writer")?;
        }
        let path = ObjPath::from(format!("{}/part-{f:04}.parquet", args.prefix));
        raw.put(&path, PutPayload::from(buf))
            .await
            .with_context(|| format!("uploading {path}"))?;
    }
    let files = list_files(raw, &args.prefix).await?;
    Ok((files, true))
}

/// Fully scan one file through `store`, decoding all row groups. Returns (rows, dur).
async fn scan_file(store: &Arc<dyn ObjectStore>, file: &DataFile) -> Result<(u64, Duration)> {
    let start = Instant::now();
    let reader =
        ParquetObjectReader::new(store.clone(), file.path.clone()).with_file_size(file.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| format!("opening parquet stream for {}", file.path))?;
    let mut stream = builder
        .build()
        .with_context(|| format!("building parquet stream for {}", file.path))?;
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.with_context(|| format!("decoding {}", file.path))?;
        rows += batch.num_rows() as u64;
    }
    Ok((rows, start.elapsed()))
}

/// Scan every file once, collecting per-file durations + totals.
async fn scan_all(store: &Arc<dyn ObjectStore>, files: &[DataFile]) -> Result<ScanPass> {
    let wall_start = Instant::now();
    let mut per_file = Vec::with_capacity(files.len());
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    for f in files {
        let (rows, dur) = scan_file(store, f).await?;
        per_file.push(dur);
        total_rows += rows;
        total_bytes += f.size;
    }
    Ok(ScanPass {
        per_file,
        total_rows,
        total_bytes,
        wall: wall_start.elapsed(),
    })
}

/// Build a `Phase` whose throughput is rows/s and whose `extra` carries MB/s etc.
fn make_phase(
    name: &str,
    per_file: &[Duration],
    total_rows: u64,
    total_bytes: u64,
    wall: Duration,
) -> Phase {
    let secs = wall.as_secs_f64().max(f64::MIN_POSITIVE);
    let rows_per_s = total_rows as f64 / secs;
    let mb = total_bytes as f64 / (1024.0 * 1024.0);
    let extra = serde_json::json!({
        "wall_ms": wall.as_secs_f64() * 1000.0,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "mb": mb,
        "mb_per_s": mb / secs,
        "files": per_file.len(),
    });
    Phase {
        name: name.to_string(),
        samples: per_file.len() as u64,
        p50_ms: percentile(per_file, 50.0),
        p95_ms: percentile(per_file, 95.0),
        throughput_per_s: rows_per_s,
        extra: Some(extra),
    }
}

/// Run `iters` passes over `files`, aggregating per-file durations and summing
/// rows/bytes/wall across all passes. Used for the repeated no-cache + warm phases.
async fn repeat_scan(
    store: &Arc<dyn ObjectStore>,
    files: &[DataFile],
    iters: usize,
) -> Result<ScanPass> {
    let mut per_file = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut wall = Duration::ZERO;
    for _ in 0..iters.max(1) {
        let pass = scan_all(store, files).await?;
        per_file.extend(pass.per_file);
        total_rows += pass.total_rows;
        total_bytes += pass.total_bytes;
        wall += pass.wall;
    }
    Ok(ScanPass {
        per_file,
        total_rows,
        total_bytes,
        wall,
    })
}

fn print_human_table(report: &BenchReport, speedups: &str) {
    eprintln!("\n=== cache-scan: cold vs warm Foyer object-store cache ===\n");
    eprintln!(
        "  {:<10} {:>8} {:>10} {:>10} {:>14} {:>10}",
        "PHASE", "SAMPLES", "P50(ms)", "P95(ms)", "ROWS/s", "MB/s"
    );
    eprintln!(
        "  {:-<10} {:->8} {:->10} {:->10} {:->14} {:->10}",
        "", "", "", "", "", ""
    );
    for p in &report.phases {
        let mbps = p
            .extra
            .as_ref()
            .and_then(|e| e.get("mb_per_s"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        eprintln!(
            "  {:<10} {:>8} {:>10.3} {:>10.3} {:>14.0} {:>10.1}",
            p.name, p.samples, p.p50_ms, p.p95_ms, p.throughput_per_s, mbps
        );
    }
    eprintln!("\n  {speedups}\n");
}

fn safe_ratio(num: f64, den: f64) -> f64 {
    if den > 0.0 {
        num / den
    } else {
        0.0
    }
}

async fn run(args: Args) -> Result<BenchReport> {
    let cfg = BenchConfig::from_env();
    let raw = build_s3(&cfg)?;
    ensure_bucket(&raw, &cfg).await?;

    let (files, wrote) = ensure_dataset(&raw, &args).await?;
    if files.is_empty() {
        return Err(anyhow!("no parquet files under prefix '{}'", args.prefix));
    }
    let total_mb: f64 = files.iter().map(|f| f.size as f64).sum::<f64>() / (1024.0 * 1024.0);
    eprintln!(
        "dataset {}: {} files, {:.1} MB total ({})",
        args.prefix,
        files.len(),
        total_mb,
        if wrote { "freshly written" } else { "reused" }
    );

    // Phase 1: no-cache baseline through the raw S3 store.
    eprintln!(
        "phase: no-cache baseline ({} iters)...",
        args.no_cache_iters
    );
    let no_cache = repeat_scan(&raw, &files, args.no_cache_iters).await?;

    // Phases 2+3: a single fresh Foyer cache, read cold then warm.
    let cached: Arc<dyn ObjectStore> =
        Arc::new(CachingObjectStore::new(raw.clone(), CacheConfig::default()));
    eprintln!("phase: cold (fresh Foyer cache, populating from MinIO)...");
    let cold = scan_all(&cached, &files).await?;
    eprintln!(
        "phase: warm (same Foyer cache, now populated; {} iters)...",
        args.warm_iters
    );
    let warm = repeat_scan(&cached, &files, args.warm_iters).await?;

    let no_cache_phase = make_phase(
        "no-cache",
        &no_cache.per_file,
        no_cache.total_rows,
        no_cache.total_bytes,
        no_cache.wall,
    );
    let cold_phase = make_phase(
        "cold",
        &cold.per_file,
        cold.total_rows,
        cold.total_bytes,
        cold.wall,
    );
    let warm_phase = make_phase(
        "warm",
        &warm.per_file,
        warm.total_rows,
        warm.total_bytes,
        warm.wall,
    );

    let warm_vs_cold = safe_ratio(cold_phase.p50_ms, warm_phase.p50_ms);
    let warm_vs_no_cache = safe_ratio(no_cache_phase.p50_ms, warm_phase.p50_ms);
    let speedups = format!(
        "speedup: warm vs cold = {warm_vs_cold:.2}x, warm vs no-cache = {warm_vs_no_cache:.2}x \
(per-file p50: no-cache={:.3}ms cold={:.3}ms warm={:.3}ms) — cache {}",
        no_cache_phase.p50_ms,
        cold_phase.p50_ms,
        warm_phase.p50_ms,
        if warm_phase.p50_ms < cold_phase.p50_ms {
            "ENGAGED (warm < cold)"
        } else {
            "did not show a warm<cold delta"
        },
    );

    let report = BenchReport {
        name: "cache-scan".to_string(),
        status: BenchStatus::Ready,
        phases: vec![no_cache_phase, cold_phase, warm_phase],
        notes: Some(format!("{speedups}. {NOTES_CAVEAT}")),
    };
    print_human_table(&report, &speedups);
    Ok(report)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(report) => {
            report.print_stdout();
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("cache-scan failed: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
