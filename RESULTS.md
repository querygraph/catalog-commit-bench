> ## ⚠️ Correction: the LakeCat row below is NOT comparable
>
> Verified empirically (MinIO object counts after a benchmark run): a standard
> Iceberg commit writes a new `metadata.json` to object storage and advances the
> pointer. Polaris, Nessie, and Gravitino each did this — thousands of S3 objects.
> **LakeCat in the default/deferred build wrote ZERO objects** — its
> `set-properties` commit only does the Turso pointer CAS + audit/outbox;
> Iceberg metadata materialization is deferred to Sail, which is absent without
> the `sail-local` feature. So LakeCat's numbers measure a *strictly lighter*
> operation (catalog-state CAS, no metadata-file write) and must not be compared
> head-to-head with the others. A fair LakeCat number requires building
> `lakecat-service` with `--features turso-local,sail-local` so the real Sail
> engine builds and writes a new `metadata.json` to S3 per commit. See "Storage
> and the metadata-write asymmetry" below.

# Commit-path results

Same host, same driver, identical parameters: **1000 sequential commits**, then
**8 concurrent writers for 6 s**, `set-properties` commits (no data files).
Catalogs brought up from `~/src/boat/docker-compose.yml` (Nessie + Gravitino on
a shared MinIO/S3 backend); LakeCat from this repo's image.

| Catalog | Storage backend | Seq throughput | Seq p50 | Seq p99 | Concurrent (8w) | Conflict rate |
|---|---|---|---|---|---|---|
| **LakeCat** 0.1.1 ⚠️ | Turso CAS only, **no metadata write** | 303 /s | 2.4 ms | 13.2 ms | 379 /s | 0% |
| **Gravitino** (iceberg-rest) | MinIO / S3 | 116 /s | 8.1 ms | 16.9 ms | 220 /s | 0% |
| **Nessie** 0.107.5 | MinIO / S3 | 98 /s | 9.7 ms | 23.0 ms | 107 /s | **80.6%** |
| **Polaris** 1.5.0 | MinIO / S3 | 57 /s | 16.6 ms | 34.7 ms | 57 /s | 11.8% |

## Storage and the metadata-write asymmetry

- **Turso is LakeCat's catalog-state store, not table data.** It holds the
  metadata pointer, pointer log, idempotency, audit, and outbox rows — the
  analogue of Polaris's metastore, Nessie's version store, and Gravitino's
  backend (all also local/in-memory here). That part is a fair equivalent.
- **The Iceberg `metadata.json` belongs in object storage for every catalog.**
  Polaris/Nessie/Gravitino write one to MinIO per commit (verified: 1597 / 1693
  S3 objects). LakeCat in the deferred build writes **none** (0 objects) — it
  defers metadata materialization to Sail. So its number is catalog-state CAS
  only and is not comparable. Pointing LakeCat at MinIO did not change this:
  there was no metadata write to redirect.
- **Fair LakeCat run (TODO):** build with `--features turso-local,sail-local`
  and commit with an `s3://` table location so the real Sail engine builds and
  PUTs a new `metadata.json` to MinIO each commit, exactly like the others.
- **Conflict models differ, and that dominates the concurrent column.** Nessie
  enforces strict serializable commits: 8 writers committing to the *same* table
  mostly conflict (80.6%) and would retry, so its successful-commit rate is
  lower by design. LakeCat and Gravitino accept the concurrent `set-properties`
  commits without conflict under `assert-table-uuid`. So "concurrent
  throughput" compares *commit-serialization policy* as much as raw speed —
  Nessie's number reflects correctness strictness, not a slow path.
- **Sequential throughput is the cleaner cross-catalog signal.** There,
  contention is removed and each number is per-commit catalog cost (plus the
  storage caveat above): LakeCat 303 > Gravitino 116 > Nessie 98 > Polaris 57 /s.
- **Polaris is the heaviest per commit** — it runs RBAC checks and credential
  subscoping in addition to the S3 metadata write, which shows in its 16.6 ms
  p50. That is governance cost, the same category of work LakeCat does inline.

## Reproduce

```sh
# catalogs (from ~/src/boat)
docker network create iceberg_lakehouse-net 2>/dev/null
cd ~/src/boat && docker compose up -d minio nessie gravitino
docker run --rm --network iceberg_lakehouse-net --entrypoint sh minio/mc -c \
  "mc alias set m http://minio:9000 admin password && mc mb -p m/warehouse"

# polaris (from ~/src/boat) — needs a catalog bootstrap step
cd ~/src/boat && docker compose up -d polaris && TOKEN=$(./polaris-bootstrap.sh | tail -1)

# lakecat (from this repo)
./docker/build-lakecat.sh && docker compose build lakecat && docker compose up -d lakecat

# bench (identical params)
P="--namespace bench --table commits --create --iterations 1000 --concurrency 8 --duration-secs 6"
./target/release/catalog-commit-bench --base-url http://127.0.0.1:8181/catalog $P
./target/release/catalog-commit-bench --base-url http://127.0.0.1:19120/iceberg --prefix main $P
./target/release/catalog-commit-bench --base-url http://127.0.0.1:9002/iceberg $P
./target/release/catalog-commit-bench --base-url http://127.0.0.1:8185/api/catalog --prefix bench --token "$TOKEN" $P
```

## Not measured

- **Unity OSS** is not in `~/src/boat`'s compose; its external-`updateTable`
  write support needs confirming before a write benchmark is meaningful.
