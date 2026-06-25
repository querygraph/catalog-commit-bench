# Commit-path results

Same host, same driver, identical parameters: **1000 sequential commits**, then
**8 concurrent writers for 6 s**, `set-properties` commits (no data files).
Catalogs brought up from `~/src/boat/docker-compose.yml` (Nessie + Gravitino on
a shared MinIO/S3 backend); LakeCat from this repo's image.

| Catalog | Storage backend | Seq throughput | Seq p50 | Seq p99 | Concurrent (8w) | Conflict rate |
|---|---|---|---|---|---|---|
| **LakeCat** 0.1.1 | local file:// (Turso) | **303 /s** | 2.4 ms | 13.2 ms | **379 /s** | 0% |
| **Gravitino** (iceberg-rest) | MinIO / S3 | 116 /s | 8.1 ms | 16.9 ms | 220 /s | 0% |
| **Nessie** 0.107.5 | MinIO / S3 | 98 /s | 9.7 ms | 23.0 ms | 107 /s | **80.6%** |
| **Polaris** 1.5.0 | MinIO / S3 | 57 /s | 16.6 ms | 34.7 ms | 57 /s | 11.8% |

## Reading the numbers honestly

- **Not a pure apples-to-apples on storage.** LakeCat writes metadata to a local
  `file://` store (Turso spine); Nessie and Gravitino write table metadata to
  MinIO over S3. A meaningful part of LakeCat's lower latency is local-fs vs an
  S3 round-trip per commit, not just catalog code. To isolate catalog overhead,
  LakeCat would need an S3 metadata backend too.
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
