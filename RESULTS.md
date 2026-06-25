# Commit-path results

Same host, same driver, identical parameters: **1000 sequential commits**, then
**8 concurrent writers for 6 s**, `set-properties` commits (no data files).
Catalogs brought up from `~/src/boat/docker-compose.yml` (Nessie, Gravitino,
Polaris on a shared MinIO/S3 backend); LakeCat from this repo's image, built with
`--features turso-local,sail-local` and pointed at the same MinIO.

**All four now do the same unit of work**: validate the commit, apply the
updates, write a new `metadata.json` to S3, and advance the pointer (verified by
MinIO object counts — LakeCat wrote 1283 objects across this run).

| Catalog | Storage | Seq throughput | Seq p50 | Seq p99 | Concurrent (8w) | Conflict rate |
|---|---|---|---|---|---|---|
| **Gravitino** (iceberg-rest) | MinIO / S3 | 116 /s | 8.1 ms | 16.9 ms | 220 /s | 0% |
| **Nessie** 0.107.5 | MinIO / S3 | 98 /s | 9.7 ms | 23.0 ms | 107 /s | 80.6% |
| **LakeCat** 0.1.1 (sail-local) | MinIO / S3 (Turso state) | 92.5 /s | 9.9 ms | 21.3 ms | 38.5 /s | 85.2% |
| **Polaris** 1.5.0 | MinIO / S3 | 57 /s | 16.6 ms | 34.7 ms | 57 /s | 11.8% |

Sequential throughput is the clean cross-catalog signal: **Gravitino 116 >
Nessie 98 ≈ LakeCat 92.5 > Polaris 57 /s**. LakeCat now sits right alongside
Nessie — the earlier "303 /s, 0 conflicts" was an artifact of not writing
metadata at all (see history below).

The concurrent column reflects **commit-conflict policy**, not raw speed: LakeCat
(85.2%) and Nessie (80.6%) enforce strict optimistic concurrency — 8 writers to
the *same* table mostly conflict and would retry — so their successful-commit
rate is lower by design. Gravitino (0%) and Polaris (11.8%) accept concurrent
`set-properties` more permissively.

## How the fair LakeCat run was obtained (two fixes)

The first LakeCat run was **not comparable** (303 /s, 0 metadata objects): the
default build never materialized a `metadata.json`. Two fixes made it do the
real work:

1. **Sail applies the updates** — `sail_iceberg::spec::metadata::apply_table_updates`
   evolves the current `TableMetadata` by the REST updates; `lakecat-sail`'s
   `prepare_commit` now parses the updates into the typed `TableUpdate` enum,
   applies them, and emits a fresh `metadata.json` + new metadata-location, so
   LakeCat writes a real object to S3 per commit and advances the pointer.
2. **Turso writes are serialized** — the local Turso file is single-writer, so 8
   concurrent commit transactions hit `database is locked`. `lakecat-store` now
   serializes write transactions through a per-store async mutex (+ best-effort
   WAL/busy_timeout). The concurrent phase now runs cleanly.

## Notes on fairness

- **Turso is LakeCat's catalog-state store, not table data.** It holds the
  metadata pointer, pointer log, idempotency, audit, and outbox rows — the
  analogue of Polaris's metastore, Nessie's version store, and Gravitino's
  backend (all also local/in-memory here). The Iceberg `metadata.json` itself
  goes to S3/MinIO for every catalog, LakeCat included.
- **Polaris is heaviest per commit** — RBAC checks and credential subscoping on
  top of the S3 write (16.6 ms p50). That's governance cost.
- **The concurrent column is commit-conflict policy, not speed.** Strict-CAS
  catalogs (LakeCat 85%, Nessie 81%) show low successful throughput under 8
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
   pointer. This is what put LakeCat on equal footing (and dropped 303 → 92.5).
3. **Turso write serialization** — single-writer file + 8 concurrent commits =
   `database is locked`; serialized via a per-store async mutex.

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
