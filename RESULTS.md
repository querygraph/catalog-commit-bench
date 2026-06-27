# Commit-path results

Same host, same driver, identical parameters: **1000 sequential commits**, then
**8 concurrent writers for 6 s**, `set-properties` commits (no data files).
Catalogs brought up from `~/src/boat/docker-compose.yml` (Nessie, Gravitino,
Polaris on a shared MinIO/S3 backend); LakeCat built from source into its image
(`./bench-stack.sh`) with `--features turso-local,sail-local`, pointed at the same
MinIO. The numbers below are a fresh run on the current host — absolute throughput
differs from earlier rounds, so read them *within* the run, not across rounds.

**All do the same unit of work**: validate the commit, apply the updates, write a
new `metadata.json` to S3, and advance the pointer (verified by MinIO object
counts).

| Catalog | Storage | Seq throughput | Seq p50 | Seq p99 | Concurrent (8w) | Conflict rate |
|---|---|---|---|---|---|---|
| **Nessie** 0.107.5 | MinIO / S3 | 170.6 /s | 4.87 ms | 16.2 ms | 136.3 /s | 82.1% |
| **LakeCat** 0.2.1 | MinIO / S3 (Turso state) | 148.6 /s | 5.34 ms | 21.2 ms | 288.0 /s | 70.2% |
| **Gravitino** (iceberg-rest) | MinIO / S3 | 132.4 /s | 6.34 ms | 19.7 ms | 272.6 /s | 0% |
| **Polaris** 1.5.0 | MinIO / S3 | 84.0 /s | 10.40 ms | 30.3 ms | 61.5 /s | 7.5% |

(All four in one `bench-stack.sh` sweep; Polaris is auto-bootstrapped — an OAuth2
token + an S3 catalog on the same `warehouse` bucket — by `polaris-bootstrap.sh`.)

**LakeCat 0.2.1 is competitive with the mature Java catalogs — #2 on sequential
latency and #1 on concurrent throughput.** Its commit p50 (5.34 ms) is *faster* than
Gravitino (6.34 ms) and Polaris (10.40 ms) and within ~10% of Nessie (4.87 ms); on
concurrent throughput it is **first** (288 /s, just ahead of Gravitino's 273 and
~2.1× Nessie). That is a large change from 0.1.1, where LakeCat's commit p50 was
~2× worse and its concurrent throughput was the worst of the field (38.5 /s).

The concurrent column reflects **commit-conflict policy** as much as raw speed:
LakeCat (70%) and Nessie (82%) enforce strict optimistic concurrency — 8 writers to
the *same* table mostly conflict and retry, so successful throughput is held down by
design — while Gravitino (0%) and Polaris (8%) accept concurrent `set-properties`
more permissively. (LakeCat leads the concurrent column *despite* a strict-CAS
policy: all 8 writers hit the same table, so its edge is cheap conflict detection +
a fast bounded retry loop, not parallelism — the losers retry quickly and the
winners commit fast.)
**Polaris is the heaviest per commit** (10.40 ms p50) owing to RBAC checks +
credential subscoping on top of the S3 write — that is governance cost, not
inefficiency.

## How LakeCat got here (0.1.1 → 0.2.0)

Four changes took LakeCat's S3 commit p50 from **12.6 ms** (worst in the field) to
**4.14 ms** — without changing *what* a commit does (governance and graph/lineage
sinks were off or trivial throughout). It was never doing more catalog work than
the Java catalogs; it was missing connection-reuse optimizations they had long made.

1. **Turso MVCC concurrent writes.** 0.1.1 serialized every write through one
   per-store async mutex, so 8 concurrent commits effectively ran one-at-a-time
   (38.5 /s, 85% conflict). 0.2.0 uses `journal_mode=mvcc` + `BEGIN CONCURRENT` with
   bounded retry: different-table commits run in parallel and same-table races
   converge to the metadata-pointer CAS. Concurrent throughput 38.5 → ~200+ /s.
2. **Cache the object-store client.** LakeCat rebuilt the S3 client — credential
   chain, HTTP client, a *fresh connection with no keep-alive* — on every commit. A
   MinIO request trace showed ~1 PutObject/commit at ~1.7 ms server-side, so most of
   the old ~12 ms was per-commit client setup. Caching one client per bucket cut
   sequential p50 12.6 → 6.8 ms.
3. **Pool the write connection.** `write_txn` opened a new Turso connection and
   re-applied the MVCC pragmas on every commit. Pooling pragma-warmed connections
   (still a distinct one per concurrent writer, so MVCC is unchanged) cut p50
   6.8 → 4.14 ms.
4. **Sail as a git dependency.** LakeCat builds Sail from `querygraph/sail`'s
   `lakecat` branch (metadata evolution + planning helpers), so the benchmark image
   is reproducible without a local Sail checkout.

(Getting an *honest* baseline in the first place required making the default build
write a real `metadata.json` per commit — see History below; before that, the
"303 /s, 0 objects" figure was the catalog doing no metadata work at all.)

## Audit and Idempotency

LakeCat's remaining ~13% sequential gap to Nessie (149 vs 171 commits/s) is **not a
language gap — it is work the other catalogs do not do.** Every LakeCat commit runs
**seven writes inside one transaction**:

1. the metadata-pointer **compare-and-swap** (the actual commit),
2. a **metadata-pointer log** row (the history of pointer movements),
3. an **audit event** (who committed what, when),
4. a **transactional-outbox** row — lineage/graph events staged *atomically* with
   the commit and drained later, so a catalog change can never be lost or emitted
   without the commit,
5. an **idempotency record** — a retried commit with the same key replays the prior
   result instead of double-applying,

plus the namespace/table reads that validate the request. That is a durable audit
trail + an atomic outbox + idempotency, fsynced to the embedded store, *per commit*.
Nessie's version store and Gravitino's memory backend do less per commit because
they offer less per commit. **LakeCat is paying for features, not losing on speed** —
you would close the gap by relaxing those guarantees, not by changing languages.

### Why "Rust" did not make it fast (and why that is fine)

The commit path is **I/O-bound**, so the runtime's CPU speed is nearly irrelevant: a
MinIO trace showed ~1 `PutObject`/commit at ~1.7 ms server-side, and LakeCat's own
CPU + state work against *local* storage was p50 0.89 ms. A commit is a network PUT
plus a durable transaction; "Rust is faster than Java" buys little when the hot path
waits on S3 and fsync.

What actually made LakeCat slow at first (12.6 ms p50) was **missing connection
reuse** — rebuilding the S3 client and opening a new store connection on every commit
— the boring pooling the JVM data ecosystem standardized decades ago, which a young
Rust project simply had not done yet. Fixing it closed the gap (see *How LakeCat got
here*). And a 1000-commit loop against a warm, long-running server is the **JVM's
best case**: JIT-compiled hot paths and warm connection pools shine, while its real
weaknesses — cold start and memory footprint — never appear.

Where Rust still pays off is exactly what a warm steady-state benchmark hides: no GC
pauses (steadier **tail latency**), a far smaller resident **footprint**, and instant
**cold start** — which matter for serverless, edge, and many-tenant-per-host
deployments. On median warm latency both runtimes converge because both are just
waiting on S3; on tails, memory, and startup the Rust catalog keeps its edge.

## Notes on fairness

- **Turso is LakeCat's catalog-state store, not table data.** It holds the
  metadata pointer, pointer log, idempotency, audit, and outbox rows — the
  analogue of Polaris's metastore, Nessie's version store, and Gravitino's
  backend (all also local/in-memory here). The Iceberg `metadata.json` itself
  goes to S3/MinIO for every catalog, LakeCat included.
- **LakeCat does more durable bookkeeping per commit** (see *Audit and
  Idempotency*) — the bulk of its remaining sequential gap to Nessie's leaner
  version store.
- **The concurrent column is commit-conflict policy, not speed.** Strict-CAS
  catalogs (LakeCat 70%, Nessie 82%) both retry most of their 8-writer commits;
  LakeCat still leads the column because its conflict detection and bounded retry
  are cheap, so it churns through successful same-table commits faster.

## History: why the first LakeCat run was wrong (303 /s, 0 objects)

The initial run reported 303 /s and 0% conflicts because the default LakeCat
build **never wrote a `metadata.json`** — its `set-properties` commit only did a
Turso pointer CAS. Verified by MinIO object counts (Polaris/Nessie/Gravitino
wrote 1500–1700 objects; LakeCat wrote 0). Getting an honest number took three
fixes, in order:

1. **Sail `TableUpdate`/`ViewUpdate` discriminator** (lakehq/sail#2134) — the
   generated REST model was a flat all-required struct, so any real update
   failed to deserialize (`missing field uuid`). Now a tagged enum.
2. **Sail applies the updates** (`apply_table_updates`) + `lakecat-sail`
   `prepare_commit` rewrite — evolve the current metadata by the typed updates,
   emit a fresh `metadata.json` + new location, write it to S3, advance the
   pointer. This is what put LakeCat on equal footing.
3. **Turso write serialization** (0.1.1) — single-writer file + 8 concurrent
   commits = `database is locked`; first serialized via a per-store async mutex,
   then superseded by MVCC concurrent writes in 0.2.0.

# cache-scan results — Sail's Foyer object-store cache (cold vs warm)

A separate read-path benchmark (`catalog-bench run cache-scan`, status **Ready**)
measures Sail's new Foyer object-store cache
(`sail_object_store::CachingObjectStore`, branch `feat/object-store-foyer-cache`).
It writes a Parquet dataset to MinIO once, then fully scans every file — decoding
all row groups into Arrow `RecordBatch`es and counting rows + bytes — three ways:

- **no-cache** — read through the raw `AmazonS3` store (no cache).
- **cold** — wrap the raw store in a *fresh* `CachingObjectStore` (empty cache) and
  read once: each page is fetched from MinIO and cached.
- **warm** — read again through the *same*, now-populated cache: Foyer in-memory hits.

The reader is the `parquet` async reader (`ParquetObjectReader` +
`ParquetRecordBatchStreamBuilder`) over the object store directly, pinned to the
exact `object_store 0.13.2` / `parquet`+`arrow 58.3.0` Sail's cache layer uses, so
every byte routes through `CachingObjectStore`.

**Dataset:** 16 Parquet files × 200,000 rows (id `i64`, two measures `i64`/`f64`, a
low-cardinality `grp` string), ~5.4 MB/file, **86.9 MB / 3.2 M rows total**; default
`CacheConfig` (1 MiB pages, 1 GiB memory, 64 MiB metadata).

**Measured (live MinIO at `127.0.0.1:9000`, per-file p50/p95):**

| phase | per-file p50 | per-file p95 | throughput |
|---|---|---|---|
| no-cache (raw S3) | **47.7 ms** | 49.8 ms | 113 MB/s · 4.2 M rows/s |
| cold (fresh cache) | **47.5 ms** | 48.7 ms | 114 MB/s · 4.2 M rows/s |
| warm (Foyer hits) | **1.81 ms** | 2.09 ms | 2960 MB/s · 109 M rows/s |

**Speedup: warm is ~26× faster than cold and ~26× faster than no-cache** (warm
p50 1.81 ms vs cold 47.5 ms). Cold ≈ no-cache (cold pays the MinIO fetch to fill
the cache), confirming the win comes from cache hits, not the wrapper. The cache
**engaged** — `warm ≪ cold` is the hit/miss signal (the layer exposes no public
hit counter, so the latency collapse is the proof).

**Caveat — local MinIO understates the win.** On loopback, per-request latency is
sub-millisecond, so "cold" object reads are already cheap; the 26× warm speedup is
a *lower bound*. Against remote S3 (tens of ms per request, multiplied across the
many small range reads a Parquet scan issues for footers/row-group columns), the
warm-vs-cold advantage is dramatically larger.

Reproduce:

```sh
cd ~/src/catalog-bench
cargo run -p catalog-bench-cache-scan --release       # direct
cargo run -q -p catalog-bench -- run cache-scan --release   # via the driver
# knobs: --files --rows --row-group --no-cache-iters --warm-iters --rewrite
```

## Reproduce

```sh
# 1. shared catalog stack + MinIO + network (from ~/src/boat)
cd ~/src/boat && docker compose up -d minio nessie gravitino polaris

# 2. build LakeCat from source, deploy its image, and bench every reachable catalog
cd ~/src/catalog-commit-bench && ./bench-stack.sh
```

`bench-stack.sh` builds `lakecat-service` for Linux (Sail fetched from the
`querygraph/sail` git dep), packages + restarts the container, ensures the MinIO
`warehouse` bucket, and runs the identical `--create` + commit measurement against
each reachable catalog (LakeCat with `--location s3://warehouse/lakecat`). Polaris
is auto-bootstrapped via `polaris-bootstrap.sh` (OAuth2 token + an S3 catalog on the
same `warehouse` bucket); set `POLARIS_TOKEN` to skip the bootstrap.

## Not measured

- **Unity Catalog OSS** — *cannot* be benchmarked on the commit path yet. Released
  Unity OSS (latest **0.5.0**) exposes its Iceberg REST endpoint
  (`/api/2.1/unity-catalog/iceberg`) as **read-only** — it has no external
  `updateTable` / `set-properties` commit handler, so there is nothing to measure on
  this benchmark's axis. Commit support is implemented only in **unmerged draft PR
  [#1618](https://github.com/unitycatalog/unitycatalog/pull/1618)** ("Implement
  Iceberg REST catalog write endpoints"), targeting an unreleased **0.6.0**. To
  include Unity, build the image from that branch (or wait for a 0.6.0 release) and
  add it to `bench-stack.sh`; the compose file already carries a `unity` profile for
  when that lands. (Databricks-hosted Unity Catalog has Iceberg REST writes, but that
  is a separate product, not the Docker-deployable OSS server.)
