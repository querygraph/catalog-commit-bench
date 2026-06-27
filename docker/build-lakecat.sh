#!/usr/bin/env bash
# Build lakecat-service for Linux and stage the binary for the runtime image.
#
# As of LakeCat 0.2.1, the only external source dependency is Sail, consumed as a
# Cargo *git* dependency on the querygraph/sail `lakecat` branch (public), so the
# build needs network access to fetch it — not a ../sail path mount. Grust and
# TypeSec are now published crates (Grust 0.11.0), so no sibling checkout is
# required; we still build inside a Linux Rust container with ~/src mounted so
# `../lakecat` is the build root and the output is a real Linux ELF for the slim
# runtime image. Reproducible and arch-correct.
#
#   FEATURES=turso-local,sail-local docker/build-lakecat.sh
#
# sail-local is required for an honest commit benchmark: it makes each commit
# apply the REST updates and write a fresh metadata.json (to S3 when the runtime
# is configured for MinIO/S3), instead of a bare pointer CAS.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_repo="$(cd "$here/.." && pwd)"
src_root="${SRC_ROOT:-$(cd "$bench_repo/.." && pwd)}"   # dir containing lakecat (grust/sail/typesec come from registries)

[[ -d "$src_root/lakecat" ]] || { echo "lakecat not found under $src_root (set SRC_ROOT)" >&2; exit 1; }

target_dir="$bench_repo/.linux-target"
mkdir -p "$target_dir"

features="${FEATURES:-turso-local,sail-local}"
echo "Building lakecat-service (release, features=$features) for Linux via container ..."
# - cargo registry volume caches crates.io deps; cargo git volume caches the
#   querygraph/sail git checkout across builds.
# - sail-local pulls in Sail/DataFusion which need protoc at build time.
# - the default bridge network gives the container internet to fetch the git dep.
docker run --rm \
  -v "$src_root":/src \
  -v catalog-bench-cargo-registry:/usr/local/cargo/registry \
  -v catalog-bench-cargo-git:/usr/local/cargo/git \
  -w /src/lakecat \
  rust:1-bookworm \
  sh -c "apt-get update >/dev/null && apt-get install -y protobuf-compiler python3-dev libpython3.11-dev >/dev/null && \
    cargo build -p lakecat-service --release --features '$features' \
    --target-dir /src/$(basename "$bench_repo")/.linux-target"

bin="$target_dir/release/lakecat-service"
[[ -f "$bin" ]] || { echo "binary not found: $bin" >&2; exit 1; }
cp "$bin" "$here/lakecat/lakecat-service"
echo "Staged $(du -h "$here/lakecat/lakecat-service" | cut -f1) Linux binary at docker/lakecat/lakecat-service"
echo "Now run: docker compose build lakecat && docker compose up -d lakecat"
