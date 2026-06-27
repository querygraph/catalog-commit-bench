//! Write-path benchmark — PARTIAL-REAL.
//!
//! Each iteration does **real** work:
//!   1. build a small Arrow `RecordBatch` (a few int/string/float columns × N rows),
//!   2. encode it to a **real Parquet file** in memory, and
//!   3. `PUT` that object to the shared MinIO/S3 `warehouse` bucket via `object_store`.
//!
//! It then performs a catalog **commit against LakeCat** (the same Iceberg REST
//! `set-properties` commit the `commit` bench measures), recording the freshly
//! written file's path in a table property.
//!
//! What is REAL: the Arrow→Parquet encode, the S3 object write (bytes actually
//! land in MinIO), and the LakeCat metadata commit (validation → new
//! `metadata.json` → pointer CAS → durable persist).
//!
//! What is SIMPLIFIED: this is **not** a full Iceberg `add-data-file` *append* —
//! the Parquet file is written to object storage but is not registered into the
//! table's manifest list, because a true append commit through LakeCat depends on
//! Sail's write path (see the `read-write` bench's prerequisite). So the data file
//! exists in S3 but is not yet queryable as table data; the commit advances catalog
//! metadata only. This is noted in the emitted `BenchReport`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use catalog_bench_common::{percentile, throughput, BenchConfig, BenchReport, BenchStatus, Phase};
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};

#[derive(Parser, Debug, Clone)]
#[command(about = "Write real Parquet files to S3/MinIO, then commit to LakeCat")]
struct Args {
    /// LakeCat Iceberg REST base URL (defaults to $LAKECAT_BASE).
    #[arg(long)]
    base_url: Option<String>,

    /// Iceberg REST prefix segment (may be empty).
    #[arg(long, default_value = "")]
    prefix: String,

    #[arg(long, default_value = "write_bench")]
    namespace: String,

    #[arg(long, default_value = "writes")]
    table: String,

    /// Optional bearer token.
    #[arg(long)]
    token: Option<String>,

    /// Create the namespace + table before benchmarking.
    #[arg(long, default_value_t = true)]
    create: bool,

    /// Explicit table location (defaults to s3://<bucket>/write_bench).
    #[arg(long)]
    location: Option<String>,

    /// Number of (write-file + commit) iterations to measure.
    #[arg(long, default_value_t = 200)]
    iterations: u64,

    /// Rows per Parquet file.
    #[arg(long, default_value_t = 1000)]
    rows: u64,

    /// Warmup iterations (not measured).
    #[arg(long, default_value_t = 5)]
    warmup: u64,
}

/// Minimal Iceberg REST client — the subset the write commit needs.
struct Catalog {
    http: reqwest::Client,
    base_url: String,
    prefix: String,
    token: Option<String>,
    location: Option<String>,
}

impl Catalog {
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
            .req(self.http.post(self.scoped("namespaces")))
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
                    {"id": 2, "name": "name", "required": false, "type": "string"},
                    {"id": 3, "name": "value", "required": false, "type": "double"}
                ]
            },
            "properties": {"bench.files": "0"},
            "stage-create": false
        });
        if let Some(loc) = &self.location {
            body["location"] = json!(loc);
        }
        let resp = self
            .req(
                self.http
                    .post(self.scoped(&format!("namespaces/{ns}/tables"))),
            )
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
            .req(
                self.http
                    .get(self.scoped(&format!("namespaces/{ns}/tables/{table}"))),
            )
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

    /// Record the just-written data file as a table property via a set-properties
    /// commit. SIMPLIFIED (see module docs): not a manifest add-data-file append.
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
                "updates": {
                    "bench.files": n.to_string(),
                    "bench.last_file": file_uri
                }
            }]
        });
        let url = self.scoped(&format!("namespaces/{ns}/tables/{table}"));
        let resp = self.req(self.http.post(url)).json(&body).send().await?;
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
}

/// Build a small Arrow batch: `rows` rows of (id: i64, name: utf8, value: f64).
fn make_batch(rows: u64, seed: u64) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ids: Int64Array = (0..rows).map(|r| (seed * rows + r) as i64).collect();
    let names: StringArray = (0..rows).map(|r| Some(format!("row-{seed}-{r}"))).collect();
    let values: Float64Array = (0..rows)
        .map(|r| (seed as f64) + (r as f64) * 0.5)
        .collect();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
    )
    .context("building record batch")
}

/// Encode an Arrow batch to an in-memory Parquet file.
fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(buf)
}

fn build_store(cfg: &BenchConfig) -> Result<object_store::aws::AmazonS3> {
    AmazonS3Builder::new()
        .with_endpoint(&cfg.s3_endpoint)
        .with_access_key_id(&cfg.s3_access_key)
        .with_secret_access_key(&cfg.s3_secret_key)
        .with_region(&cfg.s3_region)
        .with_bucket_name(&cfg.s3_bucket)
        .with_allow_http(cfg.s3_allow_http)
        // MinIO uses path-style addressing, not virtual-hosted buckets.
        .with_virtual_hosted_style_request(false)
        .build()
        .context("building S3/MinIO object store")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = BenchConfig::from_env();

    let base_url = args
        .base_url
        .clone()
        .unwrap_or_else(|| cfg.lakecat_base.clone());
    let location = args
        .location
        .clone()
        .unwrap_or_else(|| cfg.s3_uri(&args.namespace));

    let store = build_store(&cfg)?;
    let cat = Catalog {
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(64)
            .build()?,
        base_url,
        prefix: args.prefix.clone(),
        token: args.token.clone(),
        location: Some(location.clone()),
    };

    if args.create {
        cat.create_namespace(&args.namespace).await?;
        cat.create_table(&args.namespace, &args.table).await?;
    }
    let uuid = cat.load_table_uuid(&args.namespace, &args.table).await?;

    // Warmup (writes + commits, not measured).
    for i in 0..args.warmup {
        let batch = make_batch(args.rows, 9_000_000 + i)?;
        let bytes = encode_parquet(&batch)?;
        let key = format!("{}/data/warmup-{i}.parquet", args.namespace);
        store
            .put(&ObjPath::from(key), PutPayload::from(bytes))
            .await?;
    }

    let mut write_durs: Vec<Duration> = Vec::with_capacity(args.iterations as usize);
    let mut commit_durs: Vec<Duration> = Vec::with_capacity(args.iterations as usize);
    let mut total_bytes: u64 = 0;

    let phase_start = Instant::now();
    for i in 0..args.iterations {
        let batch = make_batch(args.rows, i)?;
        let bytes = encode_parquet(&batch)?;
        total_bytes += bytes.len() as u64;
        let key = format!("{}/data/part-{i:08}.parquet", args.namespace);
        let file_uri = cfg.s3_uri(&key);

        // --- real S3 write ---
        let t = Instant::now();
        store
            .put(&ObjPath::from(key.clone()), PutPayload::from(bytes))
            .await
            .with_context(|| format!("PUT {file_uri}"))?;
        write_durs.push(t.elapsed());

        // --- catalog commit (simplified: set-properties, not append) ---
        let t = Instant::now();
        cat.commit_file(&args.namespace, &args.table, &uuid, i, &file_uri)
            .await?;
        commit_durs.push(t.elapsed());
    }
    let phase_elapsed = phase_start.elapsed();

    let files = write_durs.len() as u64;
    let files_per_s = throughput(files, phase_elapsed);
    let bytes_per_s = if phase_elapsed.as_secs_f64() > 0.0 {
        total_bytes as f64 / phase_elapsed.as_secs_f64()
    } else {
        0.0
    };

    eprintln!(
        "catalog-bench-write-data -> {} ({} files × {} rows)",
        cat.base_url, files, args.rows
    );
    eprintln!("  parquet bytes written : {total_bytes}");
    eprintln!("  files/s               : {files_per_s:>8.1}");
    eprintln!(
        "  write  p50/p95 ms     : {:.3} / {:.3}",
        percentile(&write_durs, 50.0),
        percentile(&write_durs, 95.0)
    );
    eprintln!(
        "  commit p50/p95 ms     : {:.3} / {:.3}",
        percentile(&commit_durs, 50.0),
        percentile(&commit_durs, 95.0)
    );

    let report = BenchReport {
        name: "write-data".to_string(),
        status: BenchStatus::Ready,
        phases: vec![
            Phase {
                name: "parquet-write".to_string(),
                samples: files,
                p50_ms: percentile(&write_durs, 50.0),
                p95_ms: percentile(&write_durs, 95.0),
                throughput_per_s: files_per_s,
                extra: Some(json!({
                    "rows_per_file": args.rows,
                    "total_bytes": total_bytes,
                    "bytes_per_s": bytes_per_s,
                })),
            },
            Phase {
                name: "catalog-commit".to_string(),
                samples: commit_durs.len() as u64,
                p50_ms: percentile(&commit_durs, 50.0),
                p95_ms: percentile(&commit_durs, 95.0),
                throughput_per_s: throughput(commit_durs.len() as u64, phase_elapsed),
                extra: None,
            },
        ],
        notes: Some(
            "REAL: Arrow->Parquet encode + S3 PUT to MinIO + a LakeCat set-properties commit. \
             SIMPLIFIED: the Parquet file is written to object storage but NOT registered into \
             the table manifest (this is not a full Iceberg add-data-file append, which needs \
             Sail's write path); the commit advances catalog metadata only."
                .to_string(),
        ),
    };
    report.print_stdout();
    Ok(())
}
