//! `read-write` — a full INSERT + filtered-scan round-trip through the live
//! LakeCat Iceberg REST catalog and Sail, measured.
//!
//! This is an **exploratory** bench: LakeCat does not (yet) accept a stock
//! Iceberg snapshot **append** through its REST catalog, so the bench is built to
//! SURFACE exactly what works and what is gated rather than fake a number.
//!
//! ## What it does (all real, all measured)
//!
//! 1. **PHASE 0 — stock-client probe.** Before anything else it POSTs an Iceberg
//!    `add-snapshot` commit (the heart of any real append) to LakeCat and records
//!    the exact response. On the live build this is rejected with
//!    `apply_table_updates: add-snapshot` — the precise catalog gap (see notes).
//!    A companion finding (verified out-of-band with stock pyiceberg 0.11.1, see
//!    RESULTS.md) is that LakeCat's `GET /v1/config` serializes the Iceberg
//!    `defaults`/`overrides` map fields as JSON **arrays** of `{key,value}`, which
//!    a stock client cannot parse at all — so a stock `RestCatalog` never even
//!    initializes against LakeCat without a response-rewriting shim.
//!
//! 2. **PHASE 1 — WRITE (the INSERT LakeCat DOES accept).** It builds Arrow
//!    batches with the cache-scan column shape (`id i64, measure_a i64,
//!    measure_b f64, grp string`), encodes each to a **real Parquet data file**,
//!    and `PUT`s it to the MinIO `warehouse` bucket — then issues a LakeCat
//!    **catalog commit** recording that file. Because `add-snapshot` is gated, the
//!    accepted commit is a `set-properties` commit (validation -> a fresh durable
//!    `metadata.json` on S3 -> the metadata-pointer CAS), the same accepted catalog
//!    mutation the `commit`/`write-data` benches measure. Per-file write + commit
//!    latency (p50/p95) is reported; the data files land in MinIO for the read.
//!
//! 3. **PHASE 2 — READ (filtered scan, Rust / Sail path).** It runs a filtered
//!    scan `WHERE measure_a > <median>` over the freshly written Parquet via
//!    **DataFusion** (the engine inside Sail), routing every byte through Sail's
//!    [`CachingObjectStore`] so it can report **cold** (fresh Foyer cache) vs
//!    **warm** (populated cache) vs **no-cache** (raw S3). This is the
//!    *Rust-direct-files* read path: it reads the data files the write phase
//!    produced directly, NOT via the catalog's planner — because the
//!    `add-snapshot` gap means LakeCat holds no queryable snapshot to plan. Read
//!    latency (p50/p95) + rows/s are reported per phase.
//!
//! The emitted `BenchReport` carries the write phases, the read phases, and a
//! `notes` string documenting precisely what ran vs what is gated. Status is
//! `Ready` (a real write + real read round-trip runs) with the catalog
//! snapshot-append gap flagged front-and-centre.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use catalog_bench_common::{percentile, throughput, BenchConfig, BenchReport, BenchStatus, Phase};
use clap::Parser;
use datafusion::functions_aggregate::expr_fn::{count, max, min};
use datafusion::prelude::{col, lit, SessionContext};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use sail_object_store::{CacheConfig, CachingObjectStore};
use serde_json::{json, Value};
use url::Url;

const GATE_SUMMARY: &str = "GATE (LakeCat Iceberg-write compatibility, surfaced by this bench): \
(1) `GET /v1/config` serializes the Iceberg `defaults`/`overrides` MAP fields as JSON ARRAYS of \
{key,value} (also `config:[]` on load/create) — stock pyiceberg/Spark `RestCatalog` cannot parse \
it and fail before the first call (verified: pyiceberg 0.11.1 ConfigResponse pydantic dict_type \
error); (2) LakeCat advertises non-canonical `endpoints` strings (baked-in `/catalog` base + \
`{warehouse}` instead of `{prefix}`), so a stock client's endpoint-capability check rejects \
createTable; (3) the core blocker — a real snapshot APPEND is rejected by the catalog: \
`add-snapshot` is `This feature is not implemented: TableUpdate not yet supported by \
apply_table_updates: add-snapshot` (HTTP 400). pyiceberg DID write the Parquet data file + \
manifest + manifest-list to MinIO before the commit was rejected, so the data plane works; only \
the catalog control-plane snapshot registration is gated. => true Iceberg snapshot-append is \
`requires-fix` in LakeCat/Sail (apply_table_updates needs add-snapshot + set-snapshot-ref). \
This bench therefore measures the INSERT LakeCat DOES accept (real Parquet data files -> MinIO + \
an accepted set-properties catalog commit that writes a durable metadata.json), then runs the \
filtered read over those data files via Sail/DataFusion (Rust-direct-files, cold/warm), NOT via \
the catalog planner (no snapshot exists to plan).";

const READ_CAVEAT: &str =
    "Read path = Rust-direct-files: DataFusion (the engine inside Sail) over \
the Parquet the write phase produced, every byte routed through Sail's Foyer CachingObjectStore \
(cold = fresh cache populating from MinIO, warm = re-read from the populated cache, no-cache = raw \
S3). CAVEAT: local MinIO on loopback has tiny per-request latency, so the warm-vs-cold cache win \
is a LOWER BOUND vs remote S3.";

#[derive(Parser, Debug, Clone)]
#[command(about = "INSERT + filtered-scan round-trip through LakeCat + Sail (exploratory)")]
struct Args {
    /// LakeCat Iceberg REST base URL (defaults to $LAKECAT_BASE).
    #[arg(long)]
    base_url: Option<String>,

    /// Namespace to create/use.
    #[arg(long, default_value = "rw_bench")]
    namespace: String,

    /// Table to create/use.
    #[arg(long, default_value = "events")]
    table: String,

    /// Object-store prefix (under the bucket) the data files are written to + scanned.
    #[arg(long, default_value = "read-write")]
    prefix: String,

    /// Number of Parquet data files to write (each = one INSERT + one commit).
    #[arg(long, default_value_t = 16)]
    files: u64,

    /// Rows per Parquet data file.
    #[arg(long, default_value_t = 200_000)]
    rows: u64,

    /// Rows per row group (controls per-file row-group count + pruning granularity).
    #[arg(long, default_value_t = 50_000)]
    row_group: u64,

    /// Repetitions of the read-no-cache baseline (p50 over runs).
    #[arg(long, default_value_t = 3)]
    no_cache_iters: usize,

    /// Repetitions of the read-warm phase (cold is always exactly one).
    #[arg(long, default_value_t = 5)]
    warm_iters: usize,
}

// ----------------------------------------------------------------------------
// LakeCat Iceberg REST client (the subset this bench needs).
// ----------------------------------------------------------------------------

struct Catalog {
    http: reqwest::Client,
    base_url: String,
    location: Option<String>,
}

impl Catalog {
    fn scoped(&self, tail: &str) -> String {
        format!("{}/v1/{tail}", self.base_url.trim_end_matches('/'))
    }

    async fn create_namespace(&self, ns: &str) -> Result<()> {
        let resp = self
            .http
            .post(self.scoped("namespaces"))
            .json(&json!({ "namespace": [ns], "properties": {} }))
            .send()
            .await?;
        if resp.status().is_success() || resp.status().as_u16() == 409 {
            return Ok(());
        }
        bail!(
            "create namespace failed: {} {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }

    async fn create_table(&self, ns: &str, table: &str) -> Result<()> {
        let mut body = json!({
            "name": table,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": false, "type": "long"},
                    {"id": 2, "name": "measure_a", "required": false, "type": "long"},
                    {"id": 3, "name": "measure_b", "required": false, "type": "double"},
                    {"id": 4, "name": "grp", "required": false, "type": "string"}
                ]
            },
            "properties": {"bench.files": "0"},
            "stage-create": false
        });
        if let Some(loc) = &self.location {
            body["location"] = json!(loc);
        }
        let resp = self
            .http
            .post(self.scoped(&format!("namespaces/{ns}/tables")))
            .json(&body)
            .send()
            .await?;
        if resp.status().is_success() || resp.status().as_u16() == 409 {
            return Ok(());
        }
        bail!(
            "create table failed: {} {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }

    async fn load_table_uuid(&self, ns: &str, table: &str) -> Result<String> {
        let resp = self
            .http
            .get(self.scoped(&format!("namespaces/{ns}/tables/{table}")))
            .send()
            .await
            .context("loadTable request")?;
        if !resp.status().is_success() {
            bail!(
                "loadTable failed: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        let v: Value = resp.json().await?;
        v.pointer("/metadata/table-uuid")
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("response missing metadata.table-uuid")
    }

    /// Count the snapshots LakeCat reports for the table (proves whether any
    /// append actually stuck — expected 0 while `add-snapshot` is gated).
    async fn snapshot_count(&self, ns: &str, table: &str) -> Result<usize> {
        let resp = self
            .http
            .get(self.scoped(&format!("namespaces/{ns}/tables/{table}")))
            .send()
            .await?;
        let v: Value = resp.json().await?;
        Ok(v.pointer("/metadata/snapshots")
            .and_then(Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0))
    }

    /// The accepted catalog mutation: a `set-properties` commit recording the
    /// just-written data file. Returns Ok(true) on success, Ok(false) on a 409
    /// commit conflict, Err otherwise.
    async fn commit_file(
        &self,
        ns: &str,
        table: &str,
        uuid: &str,
        n: u64,
        file_uri: &str,
    ) -> Result<bool> {
        let body = json!({
            "requirements": [{"type": "assert-table-uuid", "uuid": uuid}],
            "updates": [{
                "action": "set-properties",
                "updates": {"bench.files": n.to_string(), "bench.last_file": file_uri}
            }]
        });
        let url = self.scoped(&format!("namespaces/{ns}/tables/{table}/commit"));
        let resp = self.http.post(url).json(&body).send().await?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            Ok(true)
        } else if status == 409 {
            Ok(false)
        } else {
            bail!(
                "commit failed: {} {}",
                status,
                resp.text().await.unwrap_or_default()
            );
        }
    }

    /// PHASE 0 probe: attempt a stock Iceberg `add-snapshot` commit (the heart of
    /// a real append) and return `(http_status, body)`. This is the structural
    /// catalog-side test — LakeCat rejects `add-snapshot` before any manifest IO,
    /// so a minimal snapshot reproduces the gate exactly.
    async fn probe_stock_append(&self, ns: &str, table: &str, uuid: &str) -> Result<(u16, String)> {
        let body = json!({
            "requirements": [{"type": "assert-table-uuid", "uuid": uuid}],
            "updates": [{
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 1_i64,
                    "sequence-number": 1_i64,
                    "timestamp-ms": 1_000_000_000_000_i64,
                    "manifest-list": "s3://warehouse/probe/metadata/snap-probe.avro",
                    "summary": {"operation": "append"},
                    "schema-id": 0
                }
            }]
        });
        let url = self.scoped(&format!("namespaces/{ns}/tables/{table}/commit"));
        let resp = self.http.post(url).json(&body).send().await?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        Ok((status, text))
    }
}

// ----------------------------------------------------------------------------
// Data generation (same column shape + generator as the cache-scan dataset).
// ----------------------------------------------------------------------------

fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("measure_a", DataType::Int64, false),
        Field::new("measure_b", DataType::Float64, false),
        Field::new("grp", DataType::Utf8, false),
    ]))
}

/// Build one record batch of `n` rows starting at logical row `base`
/// (deterministic LCG, no `rand` dep — identical scheme to `cache-scan`).
fn build_batch(schema: &SchemaRef, base: u64, n: u64) -> Result<arrow::record_batch::RecordBatch> {
    let n = n as usize;
    let mut id = Vec::with_capacity(n);
    let mut a = Vec::with_capacity(n);
    let mut b = Vec::with_capacity(n);
    let mut g = Vec::with_capacity(n);
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
    arrow::record_batch::RecordBatch::try_new(
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

/// Encode one file's worth of rows (split into row groups) to in-memory Parquet.
fn encode_file(schema: &SchemaRef, base: u64, rows: u64, row_group: u64) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, schema.clone(), None).context("parquet writer")?;
        let mut written = 0u64;
        while written < rows {
            let take = row_group.min(rows - written);
            let batch = build_batch(schema, base + written, take)?;
            writer.write(&batch).context("writing row group")?;
            written += take;
        }
        writer.close().context("closing parquet writer")?;
    }
    Ok(buf)
}

// ----------------------------------------------------------------------------
// Object store.
// ----------------------------------------------------------------------------

fn build_raw_s3(cfg: &BenchConfig) -> Result<Arc<dyn ObjectStore>> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&cfg.s3_endpoint)
        .with_region(&cfg.s3_region)
        .with_bucket_name(&cfg.s3_bucket)
        .with_access_key_id(&cfg.s3_access_key)
        .with_secret_access_key(&cfg.s3_secret_key)
        .with_allow_http(cfg.s3_allow_http)
        // MinIO uses path-style addressing, not virtual-hosted buckets.
        .with_virtual_hosted_style_request(false)
        .build()
        .context("building AmazonS3 object store for MinIO")?;
    Ok(Arc::new(s3))
}

// ----------------------------------------------------------------------------
// Write phase.
// ----------------------------------------------------------------------------

struct WriteOutcome {
    write_durs: Vec<Duration>,
    commit_durs: Vec<Duration>,
    commit_conflicts: u64,
    total_bytes: u64,
    total_rows: u64,
    wall: Duration,
}

async fn write_phase(
    raw: &Arc<dyn ObjectStore>,
    cat: &Catalog,
    args: &Args,
    uuid: &str,
) -> Result<WriteOutcome> {
    let schema = dataset_schema();
    let mut write_durs = Vec::with_capacity(args.files as usize);
    let mut commit_durs = Vec::with_capacity(args.files as usize);
    let mut commit_conflicts = 0u64;
    let mut total_bytes = 0u64;
    let mut total_rows = 0u64;
    let prefix = args.prefix.trim_matches('/');

    let wall_start = Instant::now();
    for f in 0..args.files {
        let base = f * args.rows;
        let bytes = encode_file(&schema, base, args.rows, args.row_group)?;
        total_bytes += bytes.len() as u64;
        total_rows += args.rows;
        let key = format!("{prefix}/part-{f:04}.parquet");
        let file_uri = format!("s3://{}/{}", cat_bucket(cat), key);

        // --- real S3 write (the INSERT data lands in MinIO) ---
        let t = Instant::now();
        raw.put(&ObjPath::from(key.clone()), PutPayload::from(bytes))
            .await
            .with_context(|| format!("PUT {key}"))?;
        write_durs.push(t.elapsed());

        // --- accepted catalog commit (set-properties; add-snapshot is gated) ---
        let t = Instant::now();
        let ok = cat
            .commit_file(&args.namespace, &args.table, uuid, f + 1, &file_uri)
            .await?;
        commit_durs.push(t.elapsed());
        if !ok {
            commit_conflicts += 1;
        }
    }

    Ok(WriteOutcome {
        write_durs,
        commit_durs,
        commit_conflicts,
        total_bytes,
        total_rows,
        wall: wall_start.elapsed(),
    })
}

/// The bucket the catalog location points at (for building the recorded file URI).
fn cat_bucket(cat: &Catalog) -> String {
    cat.location
        .as_deref()
        .and_then(|l| l.strip_prefix("s3://"))
        .and_then(|rest| rest.split('/').next())
        .unwrap_or("warehouse")
        .to_string()
}

// ----------------------------------------------------------------------------
// Read phase (filtered scan via DataFusion over the object store).
// ----------------------------------------------------------------------------

/// Build a DataFusion session over `store`, registering the Parquet directory at
/// `prefix` as the table `rw`.
async fn build_ctx(
    store: Arc<dyn ObjectStore>,
    cfg: &BenchConfig,
    prefix: &str,
) -> Result<SessionContext> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };

    let ctx = SessionContext::new();
    let authority = Url::parse(&format!("s3://{}", cfg.s3_bucket))
        .with_context(|| format!("parsing object-store url for bucket {}", cfg.s3_bucket))?;
    ctx.register_object_store(&authority, store);

    let table_uri = format!("s3://{}/{}/", cfg.s3_bucket, prefix.trim_matches('/'));
    let table_path =
        ListingTableUrl::parse(&table_uri).with_context(|| format!("parsing {table_uri}"))?;
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(dataset_schema());
    let table = ListingTable::try_new(config).context("creating listing table")?;
    ctx.register_table("rw", Arc::new(table))
        .context("registering rw table")?;
    Ok(ctx)
}

/// The data-driven filter threshold: the midpoint of measure_a's [min, max].
/// `measure_a` is a uniform 31-bit value, so the midpoint keeps ~half the rows —
/// a real filter that exercises predicate pushdown + row-group pruning.
async fn filter_threshold(ctx: &SessionContext) -> Result<i64> {
    let batches = ctx
        .table("rw")
        .await
        .context("opening rw table")?
        .aggregate(
            vec![],
            vec![
                min(col("measure_a")).alias("lo"),
                max(col("measure_a")).alias("hi"),
            ],
        )
        .context("min/max aggregate")?
        .collect()
        .await
        .context("executing min/max")?;
    let batch = batches.first().context("empty min/max result")?;
    let lo = batch
        .column_by_name("lo")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .context("min column not Int64")?
        .value(0);
    let hi = batch
        .column_by_name("hi")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .context("max column not Int64")?
        .value(0);
    Ok(lo + (hi - lo) / 2)
}

/// Run the filtered scan once: `WHERE measure_a > threshold`, returning
/// (wall, matched_rows).
async fn run_scan(ctx: &SessionContext, threshold: i64) -> Result<(Duration, u64)> {
    let start = Instant::now();
    let batches = ctx
        .table("rw")
        .await
        .context("opening rw table")?
        .filter(col("measure_a").gt(lit(threshold)))
        .context("applying filter")?
        .aggregate(vec![], vec![count(lit(1i64)).alias("n")])
        .context("building count")?
        .collect()
        .await
        .context("executing filtered scan")?;
    let elapsed = start.elapsed();
    let matched = batches
        .first()
        .and_then(|b| b.column_by_name("n"))
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0) as u64)
        .unwrap_or(0);
    Ok((elapsed, matched))
}

struct ReadPass {
    per_run: Vec<Duration>,
    matched: u64,
    wall: Duration,
}

async fn measure_read(ctx: &SessionContext, threshold: i64, iters: usize) -> Result<ReadPass> {
    let wall_start = Instant::now();
    let mut per_run = Vec::with_capacity(iters);
    let mut matched = 0u64;
    for i in 0..iters.max(1) {
        let (dur, m) = run_scan(ctx, threshold).await?;
        if i == 0 {
            matched = m;
        } else if m != matched {
            return Err(anyhow!(
                "filtered scan not stable: {m} vs {matched} matched rows"
            ));
        }
        per_run.push(dur);
    }
    Ok(ReadPass {
        per_run,
        matched,
        wall: wall_start.elapsed(),
    })
}

fn read_phase(name: &str, p: &ReadPass) -> Phase {
    let total = p.matched.saturating_mul(p.per_run.len() as u64);
    Phase {
        name: name.to_string(),
        samples: p.per_run.len() as u64,
        p50_ms: percentile(&p.per_run, 50.0),
        p95_ms: percentile(&p.per_run, 95.0),
        throughput_per_s: throughput(total, p.wall),
        extra: Some(json!({
            "matched_rows": p.matched,
            "wall_ms": p.wall.as_secs_f64() * 1000.0,
        })),
    }
}

// ----------------------------------------------------------------------------
// Driver.
// ----------------------------------------------------------------------------

fn safe_ratio(num: f64, den: f64) -> f64 {
    if den > 0.0 {
        num / den
    } else {
        0.0
    }
}

async fn run(args: Args) -> Result<BenchReport> {
    let cfg = BenchConfig::from_env();
    let base_url = args
        .base_url
        .clone()
        .unwrap_or_else(|| cfg.lakecat_base.clone());
    let location = cfg.s3_uri(&format!("{}/{}", args.namespace, args.table));

    let cat = Catalog {
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .build()?,
        base_url: base_url.clone(),
        location: Some(location.clone()),
    };
    let raw = build_raw_s3(&cfg)?;

    // Set up the namespace + table.
    cat.create_namespace(&args.namespace).await?;
    cat.create_table(&args.namespace, &args.table).await?;
    let uuid = cat.load_table_uuid(&args.namespace, &args.table).await?;

    // --- PHASE 0: stock-append probe (self-document the catalog gap). ---
    eprintln!("phase 0: probing stock Iceberg add-snapshot append against LakeCat...");
    let (probe_status, probe_body) = cat
        .probe_stock_append(&args.namespace, &args.table, &uuid)
        .await?;
    let append_supported = (200..300).contains(&probe_status);
    eprintln!(
        "  add-snapshot append -> HTTP {probe_status} ({})",
        if append_supported {
            "ACCEPTED".to_string()
        } else {
            format!("GATED: {}", probe_body.trim())
        }
    );

    // --- PHASE 1: write data files + accepted catalog commits. ---
    eprintln!(
        "phase 1: writing {} files x {} rows -> s3://{}/{}/ + a LakeCat commit each...",
        args.files, args.rows, cfg.s3_bucket, args.prefix
    );
    let w = write_phase(&raw, &cat, &args, &uuid).await?;
    let snaps = cat.snapshot_count(&args.namespace, &args.table).await?;
    eprintln!(
        "  wrote {} rows / {:.1} MB; commit conflicts {}; table snapshots after writes: {}",
        w.total_rows,
        w.total_bytes as f64 / (1024.0 * 1024.0),
        w.commit_conflicts,
        snaps
    );

    let write_data_phase = Phase {
        name: "data-write".to_string(),
        samples: w.write_durs.len() as u64,
        p50_ms: percentile(&w.write_durs, 50.0),
        p95_ms: percentile(&w.write_durs, 95.0),
        throughput_per_s: throughput(w.write_durs.len() as u64, w.wall),
        extra: Some(json!({
            "rows_per_file": args.rows,
            "total_rows": w.total_rows,
            "total_bytes": w.total_bytes,
            "bytes_per_s": if w.wall.as_secs_f64() > 0.0 {
                w.total_bytes as f64 / w.wall.as_secs_f64()
            } else { 0.0 },
        })),
    };
    let commit_phase = Phase {
        name: "catalog-commit".to_string(),
        samples: w.commit_durs.len() as u64,
        p50_ms: percentile(&w.commit_durs, 50.0),
        p95_ms: percentile(&w.commit_durs, 95.0),
        throughput_per_s: throughput(w.commit_durs.len() as u64, w.wall),
        extra: Some(json!({
            "form": "set-properties (add-snapshot gated)",
            "conflicts": w.commit_conflicts,
            "snapshots_after": snaps,
        })),
    };

    // --- PHASE 2: filtered read via Sail/DataFusion, cold vs warm vs no-cache. ---
    eprintln!("phase 2: filtered Sail/DataFusion scan (computing threshold)...");
    let no_cache_ctx = build_ctx(raw.clone(), &cfg, &args.prefix).await?;
    let threshold = filter_threshold(&no_cache_ctx).await?;
    eprintln!("  predicate: WHERE measure_a > {threshold} (median of measure_a)");

    eprintln!("  read-no-cache ({} iters)...", args.no_cache_iters);
    let no_cache = measure_read(&no_cache_ctx, threshold, args.no_cache_iters).await?;
    if no_cache.matched == 0 {
        return Err(anyhow!(
            "filtered scan matched 0 rows under prefix '{}' — was the write phase skipped?",
            args.prefix
        ));
    }

    let cached: Arc<dyn ObjectStore> =
        Arc::new(CachingObjectStore::new(raw.clone(), CacheConfig::default()));
    let cached_ctx = build_ctx(cached, &cfg, &args.prefix).await?;
    eprintln!("  read-cold (fresh Foyer cache, populating from MinIO)...");
    let cold = measure_read(&cached_ctx, threshold, 1).await?;
    eprintln!(
        "  read-warm (same Foyer cache, now populated; {} iters)...",
        args.warm_iters
    );
    let warm = measure_read(&cached_ctx, threshold, args.warm_iters).await?;

    let read_no_cache = read_phase("read-no-cache", &no_cache);
    let read_cold = read_phase("read-cold", &cold);
    let read_warm = read_phase("read-warm", &warm);

    let warm_vs_cold = safe_ratio(read_cold.p50_ms, read_warm.p50_ms);
    let warm_vs_no_cache = safe_ratio(read_no_cache.p50_ms, read_warm.p50_ms);

    let report = BenchReport {
        name: "read-write".to_string(),
        status: BenchStatus::Ready,
        phases: vec![
            write_data_phase,
            commit_phase,
            read_no_cache,
            read_cold,
            read_warm,
        ],
        notes: Some(format!(
            "Round-trip: wrote {rows} rows ({mb:.1} MB) as {files} Parquet data files to MinIO + \
{files} accepted LakeCat set-properties commits (durable metadata.json each), then a filtered \
scan WHERE measure_a > {threshold} matched {matched} rows, read cold/warm via Sail's Foyer cache \
(warm vs cold = {wvc:.2}x, warm vs no-cache = {wvn:.2}x at p50). Table snapshots after all writes: \
{snaps} (0 == the append gate below; the data is queryable directly, not as a catalog snapshot). \
{gate} {read_caveat}",
            rows = w.total_rows,
            mb = w.total_bytes as f64 / (1024.0 * 1024.0),
            files = args.files,
            threshold = threshold,
            matched = no_cache.matched,
            wvc = warm_vs_cold,
            wvn = warm_vs_no_cache,
            snaps = snaps,
            gate = GATE_SUMMARY,
            read_caveat = READ_CAVEAT,
        )),
    };

    print_human(&report, probe_status, append_supported);
    Ok(report)
}

fn print_human(report: &BenchReport, probe_status: u16, append_supported: bool) {
    eprintln!("\n=== read-write: INSERT + filtered-scan round-trip (LakeCat + Sail) ===\n");
    eprintln!(
        "  stock Iceberg add-snapshot append: HTTP {probe_status} -> {}",
        if append_supported {
            "SUPPORTED"
        } else {
            "GATED (apply_table_updates: add-snapshot) — write = data files + set-properties commit"
        }
    );
    eprintln!();
    eprintln!(
        "  {:<16} {:>8} {:>10} {:>10} {:>16}",
        "PHASE", "SAMPLES", "P50(ms)", "P95(ms)", "THRUPUT/s"
    );
    eprintln!(
        "  {:-<16} {:->8} {:->10} {:->10} {:->16}",
        "", "", "", "", ""
    );
    for p in &report.phases {
        eprintln!(
            "  {:<16} {:>8} {:>10.3} {:>10.3} {:>16.0}",
            p.name, p.samples, p.p50_ms, p.p95_ms, p.throughput_per_s
        );
    }
    eprintln!();
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
            eprintln!("read-write failed: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
