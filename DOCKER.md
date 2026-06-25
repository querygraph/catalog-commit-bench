# Reproducible benchmark harness (Docker Compose)

This brings up Iceberg REST catalogs and runs the commit benchmark against them.
**LakeCat is fully wired and verified.** Polaris, Gravitino, and Unity are
included behind compose profiles with best-effort configs; each needs a
bootstrap/auth step that is not hands-off (see *Bootstrap caveats*).

## Layout assumption

The harness lives in `~/src/catalog-commit-bench` alongside the sibling repos it
needs to build LakeCat:

```
~/src/
  catalog-commit-bench/   <- this repo
  lakecat/  sail/  grust/  typesec/
```

## LakeCat (verified path)

LakeCat's workspace has local path deps on `../sail` (not published), so we
build it for Linux inside a Rust container with `~/src` mounted — the siblings
resolve through the mount, the output is a real Linux ELF:

```sh
./docker/build-lakecat.sh          # builds lakecat-service for Linux, stages the binary
docker compose build lakecat
docker compose up -d lakecat
```

Then benchmark it (host binary or the bench container):

```sh
cargo build --release
./target/release/catalog-commit-bench \
  --base-url http://127.0.0.1:8181/catalog --create \
  --iterations 2000 --concurrency 8 --duration-secs 10 --idempotency
```

LakeCat is now spec-conformant on the bare commit path, so no `--commit-suffix`
is needed. Table creation still uses LakeCat's client-supplied-metadata shape;
`run-bench.sh` provisions the table for you with the right body.

## The whole sweep

```sh
docker compose up -d lakecat
./run-bench.sh                     # benchmarks every reachable catalog
```

`run-bench.sh` provisions LakeCat explicitly (its create-table shape differs),
then runs the identical commit measurement against each catalog that is up. Set
`POLARIS_TOKEN`, `UNITY_TOKEN`, and the `*_BASE`/`*_PREFIX` env vars to include
the externals.

## Bootstrap caveats (the externals are not turnkey)

- **Polaris** — `docker compose --profile polaris up -d polaris`. Polaris
  bootstraps a root principal on first run and prints its client_id/secret to
  the logs; exchange them via its OAuth2 token endpoint and pass the result as
  `POLARIS_TOKEN`. Prefix = catalog name. Spec-conformant commit path.
- **Gravitino** — `docker compose --profile gravitino up -d gravitino`. Uses the
  `apache/gravitino-iceberg-rest` image with a memory backend. Confirm your tag
  serves the REST API on `:9001`; older tags differ. Spec-conformant.
- **Unity (OSS)** — `docker compose --profile unity up -d unitycatalog`. Unity
  OSS has historically focused on Iceberg *reads* for external clients; verify
  it accepts external `updateTable` commits before trusting write numbers.

These three are scaffolded honestly: the service definitions and the run-script
hooks are correct, but their first-run bootstrap/auth is upstream-specific and
must be completed before the commit numbers mean anything. LakeCat is the one
path proven end-to-end in this harness.

## What it measures

See `README.md`. The commit phase (set-properties commits: validation → metadata
write → pointer CAS → durable persist) is identical across all four; only
creation, auth, and prefixes differ.
