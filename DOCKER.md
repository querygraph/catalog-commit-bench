# Docker harness — build & operations

How the containerized benchmark is built and run. For the **impartiality design**
and the **shared-MinIO setup** (the external network, the comparison catalogs, why
every catalog writes to one bucket), see **[README.md](README.md) → "Docker setup
for impartial runs with MinIO"**. This file covers the LakeCat-from-source build
and day-to-day operation.

**LakeCat is fully wired and verified here.** Polaris, Gravitino, and Unity are
included behind compose profiles with best-effort upstream images; each needs a
bootstrap/auth step that is not hands-off (see *Bootstrap caveats*).

## Layout assumption

The harness lives in `~/src/catalog-commit-bench` alongside the sibling repos it
needs to build LakeCat:

```
~/src/
  catalog-commit-bench/   <- this repo
  lakecat/                <- built from source
  grust/                  <- LakeCat path dependency (resolved via the ~/src mount)
```

Sail is **not** required as a local checkout: LakeCat depends on it as a Cargo
**git** dependency on `querygraph/sail#lakecat`, fetched during the build. TypeSec
is the published crate.

## Building LakeCat for Linux

LakeCat's workspace mixes a git dependency (Sail) and a local path dependency
(Grust), so we build `lakecat-service` for Linux inside a Rust container with
`~/src` mounted — `../grust` resolves through the mount, Sail is fetched over the
container's network, and the output is a real Linux ELF for the slim runtime image:

```sh
./docker/build-lakecat.sh          # compile lakecat-service (Linux) and stage the binary
docker compose build lakecat       # package the staged ELF into lakecat-service:bench
docker compose up -d lakecat       # run it on the shared network, pointed at MinIO
```

`docker/build-lakecat.sh` details:
- runs `cargo build -p lakecat-service --release --features "$FEATURES"` (default
  `FEATURES=turso-local,sail-local`; `sail-local` makes each commit write a real
  `metadata.json`) in a `rust:1-bookworm` container;
- mounts `~/src` read-write at `/src`, plus two named volumes —
  `catalog-bench-cargo-registry` (crates.io cache) and `catalog-bench-cargo-git`
  (the querygraph/sail git checkout) — so rebuilds are incremental;
- needs the container's default-bridge network to fetch the Sail git dep, and
  `protobuf-compiler` (installed in-container) for Sail/DataFusion;
- writes output to `.linux-target/` and stages the binary at
  `docker/lakecat/lakecat-service` (both gitignored).

The runtime image (`docker/lakecat/Dockerfile`) is a `debian:bookworm-slim` with
`ca-certificates`, `curl`, and `libpython3.11` (Sail links libpython), the staged
binary, a `/data` volume for the Turso DB, and a `/catalog/v1/config` healthcheck.

## Running the benchmark

### One-shot (recommended)

```sh
./bench-stack.sh
```

Builds LakeCat → image → (re)starts the container → ensures the MinIO `warehouse`
bucket → benchmarks every reachable catalog with identical parameters (LakeCat with
`--location s3://warehouse/lakecat` + idempotency; Nessie/Gravitino with their
prefixes; Polaris when `POLARIS_TOKEN` is set). Tunables: `ITER`, `CONC`, `DUR`,
`SKIP_BUILD=1`, `POLARIS_TOKEN`, `POLARIS_PREFIX`.

### Manual

```sh
docker compose up -d lakecat
./run-bench.sh                     # benchmarks every reachable catalog
# or one target, host binary:
cargo build --release
./target/release/catalog-commit-bench \
  --base-url http://127.0.0.1:8181/catalog --location s3://warehouse/lakecat \
  --create --idempotency --iterations 1000 --concurrency 8 --duration-secs 6
# or via the bench container on the shared network:
docker compose run --rm bench --base-url http://lakecat:8181/catalog \
  --location s3://warehouse/lakecat --create
```

Enable an external catalog by profile (or run them from `~/src/boat`):

```sh
docker compose --profile gravitino up -d gravitino
docker compose --profile polaris   up -d polaris
docker compose --profile unity     up -d unitycatalog
```

## Bootstrap caveats (the externals are not turnkey)

- **Polaris** bootstraps a root principal on first run and prints its
  client_id/secret to the logs; exchange them via its OAuth2 token endpoint and
  pass the result as `POLARIS_TOKEN`. Prefix = catalog name. Spec-conformant commit
  path.
- **Gravitino** uses the `apache/gravitino-iceberg-rest` image with a memory
  backend. Confirm your tag serves the REST API on the expected port; older tags
  differ. Spec-conformant.
- **Unity (OSS)** has historically focused on Iceberg *reads* for external clients;
  verify it accepts external `updateTable` commits before trusting write numbers.

These three are scaffolded honestly: the service definitions and run-script hooks
are correct, but their first-run bootstrap/auth is upstream-specific and must be
completed before the commit numbers mean anything. LakeCat is the one path proven
end-to-end in this harness.

## What it measures

See **[README.md](README.md)**. The commit phase (`set-properties` commits:
validation → metadata write → pointer CAS → durable persist) is identical across
all catalogs; only creation, auth, and prefixes differ — and every catalog's
`metadata.json` goes to the same MinIO bucket.
