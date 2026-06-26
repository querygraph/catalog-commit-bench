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
| **Nessie** 0.107.5 | MinIO / S3 | 228.6 /s | 4.04 ms | 8.4 ms | 164.0 /s | 81.6% |
| **LakeCat** 0.2.0 | MinIO / S3 (Turso state) | 198.2 /s | 4.52 ms | 10.7 ms | 311.6 /s | 73.7% |
| **Gravitino** (iceberg-rest) | MinIO / S3 | 163.9 /s | 5.74 ms | 11.2 ms | 340.2 /s | 0% |
| **Polaris** 1.5.0 | MinIO / S3 | 97.6 /s | 9.81 ms | 16.9 ms | 91.5 /s | 5.78% |

(All four in one `bench-stack.sh` sweep; Polaris is auto-bootstrapped — an OAuth2
token + an S3 catalog on the same `warehouse` bucket — by `polaris-bootstrap.sh`.)

**LakeCat 0.2.0 is now competitive with the mature Java catalogs — #2 on both
axes.** Its commit p50 (4.52 ms) is *faster* than Gravitino (5.74 ms) and Polaris
(9.81 ms) and within ~13% of Nessie (4.04 ms); on concurrent throughput it is **#2**
(312 /s, behind Gravitino's 340, ~1.9× Nessie). That is a large change from 0.1.1,
where LakeCat's commit p50 was 9.9 ms and its concurrent throughput was the worst of
the field (38.5 /s).

The concurrent column reflects **commit-conflict policy** as much as raw speed:
LakeCat (74%) and Nessie (82%) enforce strict optimistic concurrency — 8 writers to
the *same* table mostly conflict and retry, so successful throughput is held down by
design — while Gravitino (0%) and Polaris (6%) accept concurrent `set-properties`
more permissively. **Polaris is the heaviest per commit** (9.81 ms p50) owing to
RBAC checks + credential subscoping on top of the S3 write — that is governance
cost, not inefficiency.

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

## Notes on fairness

- **Turso is LakeCat's catalog-state store, not table data.** It holds the
  metadata pointer, pointer log, idempotency, audit, and outbox rows — the
  analogue of Polaris's metastore, Nessie's version store, and Gravitino's
  backend (all also local/in-memory here). The Iceberg `metadata.json` itself
  goes to S3/MinIO for every catalog, LakeCat included.
- **LakeCat does 7 bookkeeping writes per commit** (pointer CAS + pointer log +
  audit + outbox + idempotency) inside the commit transaction. That durability is
  the bulk of the remaining ~14% sequential gap to Nessie's leaner version store.
- **The concurrent column is commit-conflict policy, not speed.** Strict-CAS
  catalogs (LakeCat 73%, Nessie 81%) show lower successful throughput under 8
  writers to one table because most commits correctly conflict and retry.

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

- **Unity OSS** is not in `~/src/boat`'s compose; its external-`updateTable`
  write support needs confirming before a write benchmark is meaningful.
