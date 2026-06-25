# Runtime image for lakecat-service.
#
# LakeCat's workspace has local path dependencies on ../sail (not published to
# crates.io), so a self-contained in-container `cargo build` would need the
# sibling repos in the build context. Instead we build the binary on the host
# (where the siblings exist) via docker/build-lakecat.sh and package it here.
FROM debian:bookworm-slim

# libpython3.11 is required at runtime when the binary is built with the
# sail-local feature (Sail links libpython); harmless for deferred builds.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libpython3.11 \
    && rm -rf /var/lib/apt/lists/*

COPY lakecat-service /usr/local/bin/lakecat-service

ENV LAKECAT_BIND_ADDR=0.0.0.0:8181 \
    LAKECAT_WAREHOUSE=local \
    LAKECAT_TURSO_PATH=/data/lakecat.db

RUN mkdir -p /data
EXPOSE 8181

HEALTHCHECK --interval=5s --timeout=3s --retries=20 \
    CMD curl -fsS http://localhost:8181/catalog/v1/config || exit 1

ENTRYPOINT ["/usr/local/bin/lakecat-service"]
