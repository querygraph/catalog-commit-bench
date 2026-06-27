# Runtime image for lakecat-service.
#
# LakeCat depends on Sail as a Cargo *git* dependency (querygraph/sail#lakecat),
# fetched at build time rather than published to crates.io, so we build the
# binary for Linux via docker/build-lakecat.sh (which has network access to fetch
# the git dep) and package the staged ELF here.
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
