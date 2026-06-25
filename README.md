# catalog-commit-bench

A catalog-agnostic benchmark for the **commit path** of Iceberg REST catalogs.

TPC-DS/TPC-H measure query engines; they touch the catalog only incidentally.
This driver isolates the part those benchmarks ignore: the catalog commit
transaction — update validation, the new metadata write, the metadata-pointer
compare-and-swap, and durable persistence. It issues `set-properties` commits
(no data files), so each request exercises the catalog's commit machinery
without engine or storage-write noise.

Because every target speaks the Iceberg REST Catalog protocol, the same binary
runs against **LakeCat, Apache Polaris, Apache Gravitino, and Unity Catalog**
— only the base URL, prefix, auth, and (for LakeCat) the commit path differ.

## Phases

1. **Sequential latency** — `--iterations` commits in series; reports
   throughput and p50/p90/p99/max latency. This is the clean per-commit cost.
2. **Concurrent throughput** — `--concurrency` writers committing for
   `--duration-secs`; reports committed/s and the 409 conflict rate. This is the
   CAS-contention behavior.

## Build

```sh
cargo build --release
```

## Run recipes

All four use the same standard endpoints (`POST namespaces`, `POST tables`,
`GET tables/{t}`). They differ only in URL prefix, auth, and the commit path.

### LakeCat
LakeCat serves under `/catalog` and is spec-conformant on both `createTable` and
the bare commit path, so it uses the standard invocation:

```sh
LAKECAT_BIND_ADDR=127.0.0.1:8181 LAKECAT_WAREHOUSE=local \
  cargo run -p lakecat-service --features turso-local   # in the lakecat repo

catalog-commit-bench \
  --base-url http://127.0.0.1:8181/catalog \
  --create --idempotency \
  --iterations 2000 --concurrency 8 --duration-secs 10
```

### Apache Polaris
Polaris is spec-conformant (bare commit path). It uses OAuth2 — fetch a token
first, then pass it. The prefix is the catalog name.

```sh
catalog-commit-bench \
  --base-url http://127.0.0.1:8181/api/catalog \
  --prefix my_catalog --token "$POLARIS_TOKEN" --create \
  --iterations 2000 --concurrency 8
```

### Apache Gravitino
Gravitino exposes an Iceberg REST service; the prefix is the metalake/catalog
route it is configured with. Bare commit path.

```sh
catalog-commit-bench \
  --base-url http://127.0.0.1:9001/iceberg \
  --prefix "" --create \
  --iterations 2000 --concurrency 8
```

### Unity Catalog (OSS)
Unity exposes an Iceberg REST catalog surface; bare commit path, bearer token.
Note: UC OSS has historically focused on Iceberg *reads* for external clients —
confirm your build accepts external `updateTable` commits before trusting the
write numbers.

```sh
catalog-commit-bench \
  --base-url http://127.0.0.1:8080/api/2.1/unity-catalog/iceberg \
  --prefix unity --token "$UC_TOKEN" --create \
  --iterations 2000 --concurrency 8
```

## Fairness notes

- `set-properties` + `assert-table-uuid` is the lowest-common-denominator commit
  every conformant catalog accepts; it never writes data files, so the number is
  catalog overhead, not storage throughput.
- The concurrent phase measures commit *serialization* throughput. With
  `set-properties` + `assert-table-uuid`, commits rarely conflict, so the
  conflict rate mostly reflects how the catalog serializes pointer movement. A
  future `--mode snapshot-append` would force genuine write-write conflicts.
- Put the catalog and the driver on the same host/network for the latency phase;
  cross-AZ RTT will dominate otherwise.
- `--idempotency` only affects catalogs that implement an idempotency key
  (LakeCat); others ignore the header.

## Conformance note (LakeCat)

Building this benchmark surfaced two Iceberg REST conformance gaps in LakeCat,
both since fixed: it only accepted `updateTable` at `.../tables/{table}/commit`
(now also accepts the bare `POST .../tables/{table}`), and `createTable`
required a client-supplied metadata document (now also accepts a standard schema
and generates the metadata server-side). `--commit-suffix` therefore exists only
for catalogs that might still mount commit on a sub-path; LakeCat no longer needs
it.
