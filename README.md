# catalog-commit-bench

A catalog-agnostic benchmark for the **commit path** of Iceberg REST catalogs —
measured here across **LakeCat, Apache Nessie, Apache Gravitino, and Apache
Polaris**. (Unity Catalog OSS is *not yet measurable*: its Iceberg REST endpoint is
read-only until the commit endpoints in PR #1618 / 0.6.0 ship — see
[RESULTS.md](RESULTS.md) → "Not measured".)

TPC-DS/TPC-H measure query engines; they touch the catalog only incidentally. This
driver isolates the part those benchmarks ignore: the catalog **commit transaction**
— update validation, writing the new `metadata.json`, the metadata-pointer
compare-and-swap, and durable persistence. It issues `set-properties` commits (no
data files), so each request exercises the catalog's commit machinery without
engine or data-write noise. Every target speaks the same Iceberg REST protocol, so
one binary benchmarks all of them; only the base URL, prefix, and auth differ.

See **[RESULTS.md](RESULTS.md)** for a measured cross-catalog comparison.

## What it measures

1. **Sequential latency** — `--iterations` commits in series; reports throughput
   and p50/p90/p99/max latency. The clean per-commit cost.
2. **Concurrent throughput** — `--concurrency` writers committing for
   `--duration-secs`; reports committed/s and the 409 conflict rate. The
   CAS-contention behavior.

## Impartiality: one object store, one unit of work

A commit-path comparison is only fair if every catalog does the **same work** to
the **same storage**. The harness is built around that:

- **Same object store.** Every catalog writes its Iceberg `metadata.json` to the
  **same MinIO/S3 bucket** (`s3://warehouse`). Each catalog's own state store
  (Turso for LakeCat, the version store for Nessie, the metastore for
  Polaris/Gravitino) is its private metadata-pointer bookkeeping — the analogue
  across all of them — but the Iceberg metadata object itself lands in the shared
  MinIO for everyone. Without this, you would be comparing object stores, not
  catalogs.
- **Same unit of work.** A `set-properties` commit: validate → apply the update →
  write a fresh `metadata.json` to S3 → advance the pointer. No data files, no
  engine. Verify it with MinIO object counts — every catalog should write ~1
  object per commit (a catalog that writes 0 is doing no real metadata work).
- **Same parameters, same host/network.** Identical `--iterations` /
  `--concurrency` / `--duration-secs`, and all containers on one Docker network so
  latency is not confounded by cross-host RTT.
- **Same request shape.** All use the standard `POST namespaces` / `POST tables` /
  bare `POST tables/{t}` commit. LakeCat takes `--location s3://warehouse/lakecat`
  because it does not derive an S3 warehouse location on its own; the others get
  their S3 warehouse from their config.

## Docker setup for impartial runs with MinIO

Everything shares one **external** Docker network and one **MinIO**, so the
catalogs and the benchmark resolve each other by service name and write to the same
bucket.

```
                       iceberg_lakehouse-net  (external docker network)
   ┌───────────┐   ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌──────────────┐
   │  lakecat  │   │  nessie  │   │ gravitino │   │ polaris  │   │ catalog-     │
   │  :8181    │   │  :19120  │   │  :9001    │   │  :8181   │   │ commit-bench │
   └─────┬─────┘   └────┬─────┘   └─────┬─────┘   └────┬─────┘   └──────┬───────┘
         └──────────────┴───── s3://warehouse ─────────┴────────────────┘
                              ┌──────────────┐
                              │ minio :9000  │   admin / password, path-style
                              └──────────────┘
```

### 1. Create the shared network

```sh
docker network create iceberg_lakehouse-net
```

### 2. Bring up MinIO + the comparison catalogs

The comparison catalogs (MinIO, Nessie, Gravitino, Polaris) come from the sibling
`boat` stack. Every service joins `iceberg_lakehouse-net` and is configured to use
`s3://warehouse/` on MinIO — same credentials (`admin`/`password`), path-style
access, region `us-east-1`, endpoint `http://minio:9000`. For example, Nessie:

```yaml
NESSIE_CATALOG_DEFAULT_WAREHOUSE: warehouse
NESSIE_CATALOG_WAREHOUSES_WAREHOUSE_LOCATION: s3://warehouse/
NESSIE_CATALOG_SERVICE_S3_DEFAULT_OPTIONS_ENDPOINT: http://minio:9000
```

Bring them up and create the bucket:

```sh
cd ~/src/boat && docker compose up -d minio nessie gravitino polaris
#   (polaris is auto-bootstrapped by polaris-bootstrap.sh — see step 4)
docker run --rm --network iceberg_lakehouse-net --entrypoint sh minio/mc -c \
  "mc alias set m http://minio:9000 admin password && mc mb -p m/warehouse"
```

### 3. The benchmark's own compose (LakeCat + the driver)

This repo's `docker-compose.yml` runs LakeCat — built from source into a Linux
image — on the same network, with its `object_store` pointed at the same MinIO. The
LakeCat `environment` block (`AWS_ENDPOINT: http://minio:9000`, `admin`/`password`)
is what makes its `metadata.json` writes hit the shared bucket:

```yaml
services:
  # LakeCat: joins the shared network and points its object_store at the same
  # MinIO the other catalogs use, so its Iceberg metadata.json writes hit S3 too.
  # Turso stays LakeCat's local catalog-state store (the analogue of the others'
  # metastores). Create tables with --location s3://warehouse/lakecat.
  lakecat:
    build:
      context: ./docker/lakecat
    image: lakecat-service:bench
    networks: [lakehouse-net]
    ports: ["8181:8181"]
    environment:
      LAKECAT_BIND_ADDR: 0.0.0.0:8181
      LAKECAT_WAREHOUSE: local
      LAKECAT_TURSO_PATH: /data/lakecat.db
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
      AWS_ENDPOINT: http://minio:9000
      AWS_ALLOW_HTTP: "true"
    volumes:
      - lakecat-data:/data

  # Comparison catalogs are usually run from ~/src/boat, but are also available
  # here behind profiles for a self-contained stack:
  polaris:                       # docker compose --profile polaris up -d polaris
    image: apache/polaris:latest
    ports: ["8182:8181"]
    profiles: ["polaris"]
    environment:
      POLARIS_BOOTSTRAP_CREDENTIALS: "default-realm,root,s3cr3t"
  gravitino:                     # docker compose --profile gravitino up -d gravitino
    image: apache/gravitino-iceberg-rest:latest
    ports: ["9001:9001"]
    profiles: ["gravitino"]
    environment:
      GRAVITINO_ICEBERG_REST_CATALOG_BACKEND: memory
      GRAVITINO_ICEBERG_REST_CATALOG_WAREHOUSE: /tmp/gravitino-warehouse
  unitycatalog:                  # read-only Iceberg REST until PR #1618 / 0.6.0
    image: unitycatalog/unitycatalog:0.5.0
    ports: ["8080:8080"]         # server 8080 (UI 3000); not in the comparison yet
    profiles: ["unity"]

  # The benchmark itself, as a container (or run the host binary).
  bench:                         # docker compose run --rm bench --base-url ... --create
    build:
      context: .
      dockerfile: docker/bench.Dockerfile
    image: catalog-commit-bench:latest
    profiles: ["bench"]
    entrypoint: ["/usr/local/bin/catalog-commit-bench"]

volumes:
  lakecat-data:

networks:
  # Shared (external) with the ~/src/boat catalog stack so every catalog reaches
  # the same MinIO. Create it once: `docker network create iceberg_lakehouse-net`.
  lakehouse-net:
    name: iceberg_lakehouse-net
    external: true
```

**Why LakeCat is built from source.** LakeCat depends on Sail as a Cargo *git*
dependency on `querygraph/sail#lakecat` (fetched at build time); Grust (0.11.0) and
TypeSec are published crates. `docker/build-lakecat.sh` compiles `lakecat-service`
for Linux inside a Rust container with `~/src` mounted (so `../lakecat` is the build
root and Sail is fetched over the network), stages the ELF, and
`docker compose build lakecat` packages it into the slim runtime image.

### 4. Build, deploy, and run — one command

```sh
./bench-stack.sh
```

`bench-stack.sh` builds `lakecat-service` for Linux, packages + (re)starts the
LakeCat container, ensures the `warehouse` bucket exists, and benchmarks every
reachable catalog with **identical** parameters — LakeCat with
`--location s3://warehouse/lakecat` and idempotency, Nessie/Gravitino with their
prefixes, Polaris when `POLARIS_TOKEN` is set. Tune with env vars:

```sh
ITER=2000 CONC=8 DUR=10 ./bench-stack.sh      # heavier run
SKIP_BUILD=1 ./bench-stack.sh                 # skip the LakeCat rebuild
POLARIS_TOKEN=... ./bench-stack.sh            # include Polaris
```

## Build the driver alone

```sh
cargo build --release
```

## Manual run recipes

All use the same standard endpoints; they differ only in URL prefix, auth, and
(for LakeCat) the `--location` that pins writes to MinIO. Identical params:

```sh
P="--namespace bench --table commits --create --iterations 1000 --concurrency 8 --duration-secs 6"
```

### LakeCat
```sh
catalog-commit-bench --base-url http://127.0.0.1:8181/catalog \
  --location s3://warehouse/lakecat --idempotency $P
```

### Apache Nessie
```sh
catalog-commit-bench --base-url http://127.0.0.1:19120/iceberg --prefix main $P
```

### Apache Gravitino
```sh
catalog-commit-bench --base-url http://127.0.0.1:9002/iceberg $P
```

### Apache Polaris
OAuth2: `polaris-bootstrap.sh` fetches a token and creates an S3 catalog on the
shared MinIO `warehouse` bucket; the prefix is the catalog name.
```sh
TOKEN=$(./polaris-bootstrap.sh)
catalog-commit-bench --base-url http://127.0.0.1:8185/api/catalog \
  --prefix bench --token "$TOKEN" $P
```

### Unity Catalog (OSS) — not yet supported on the commit path
Released Unity OSS (0.5.0) serves its Iceberg REST endpoint **read-only**, so the
commit benchmark has nothing to exercise. The commit handler lands only in unmerged
draft PR [#1618](https://github.com/unitycatalog/unitycatalog/pull/1618) (unreleased
0.6.0). Against a **write-capable build** of that branch the recipe would be a bearer
token on the bare commit path:
```sh
catalog-commit-bench --base-url http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg \
  --prefix unity --token "$UC_TOKEN" $P
```

## Bootstrap caveats (the externals are not turnkey)

- **Polaris** needs an OAuth2 token + an S3 catalog (it does not auto-serve a
  warehouse). `polaris-bootstrap.sh` automates both — client creds `root`/`secret`,
  catalog `bench` on `s3://warehouse/bench`, `stsUnavailable`+`pathStyle` for MinIO
  — and `bench-stack.sh` calls it automatically. Override creds via
  `POLARIS_CLIENT_ID`/`POLARIS_CLIENT_SECRET`, or pass a ready `POLARIS_TOKEN`.
- **Gravitino** uses the `apache/gravitino-iceberg-rest` image; confirm your tag
  serves the REST API on the expected port (older tags differ).
- **Unity (OSS)** released builds (≤ 0.5.0) serve Iceberg REST **read-only** — no
  external `updateTable` commit handler exists, so it is left out of the comparison.
  Commit support is in unmerged PR #1618 (unreleased 0.6.0); build from that branch
  to include it.

## Fairness notes

- `set-properties` is the lowest-common-denominator commit every conformant catalog
  accepts; it writes no data files, so the number is **catalog overhead**, not
  storage throughput.
- **Sequential latency is the clean cross-catalog signal.** The concurrent column
  reflects **commit-conflict policy** as much as speed: strict-CAS catalogs (LakeCat,
  Nessie) show lower successful throughput under 8 writers to one table because most
  commits correctly conflict and retry; permissive catalogs (Gravitino) show 0%.
- Put the catalog and the driver on the same host/network for the latency phase;
  cross-AZ RTT will dominate otherwise.
- `--idempotency` only affects catalogs that implement an idempotency key
  (LakeCat); others ignore the header.

## Key flags

| Flag | Meaning |
|---|---|
| `--base-url` | Up to and including any catalog-specific prefix path |
| `--prefix` | Iceberg REST prefix segment (warehouse/catalog/metalake); may be empty |
| `--location` | Explicit `createTable` location, e.g. `s3://warehouse/lakecat` (points writes at the shared MinIO) |
| `--create` | Create the namespace + table before benchmarking |
| `--idempotency` | Send a LakeCat-style `Idempotency-Key` per commit |
| `--token` | Bearer token (`Authorization: Bearer ...`) |
| `--iterations` / `--concurrency` / `--duration-secs` | Sequential count / concurrent writers / concurrent duration |
| `--json` | Machine-readable summary |
