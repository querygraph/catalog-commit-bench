#!/usr/bin/env bash
# Build lakecat-service for Linux and stage the binary for the runtime image.
#
# LakeCat's workspace has local path deps on ../sail (not on crates.io), so we
# build inside a Linux Rust container with ~/src mounted: the siblings resolve
# through the mount (no multi-GB build context), and the output is a real Linux
# ELF that runs in the slim runtime image. Reproducible and arch-correct.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_repo="$(cd "$here/.." && pwd)"
src_root="${SRC_ROOT:-$(cd "$bench_repo/.." && pwd)}"   # dir containing lakecat + sail + grust + typesec

[[ -d "$src_root/lakecat" ]] || { echo "lakecat not found under $src_root (set SRC_ROOT)" >&2; exit 1; }

target_dir="$bench_repo/.linux-target"
mkdir -p "$target_dir"

features="${FEATURES:-turso-local}"
echo "Building lakecat-service (release, features=$features) for Linux via container ..."
# sail-local pulls in Sail/DataFusion which need protoc at build time.
docker run --rm \
  -v "$src_root":/src \
  -v catalog-bench-cargo-registry:/usr/local/cargo/registry \
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
