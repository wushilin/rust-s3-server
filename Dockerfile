# rusts3 container image.
#
#   docker build -t rusts3:latest .
#   docker run -d -p 8002:8002 -p 8003:8003 -v rusts3-data:/data rusts3:latest
#
# Configuration is the shipped config.docker.yaml, whose every value is a
# {{RUSTS3_NAME:default}} placeholder expanded from the environment at startup —
# so the image runs unconfigured, and any field can be overridden with -e.
# Mount your own file over /etc/rusts3/config.yaml to go further.

# ── build ───────────────────────────────────────────────────────────────────
# librocksdb-sys compiles RocksDB from source, so the builder needs a C++
# toolchain and clang for bindgen. This is the slow part of the build; the
# dependency layer is cached separately from the source so ordinary code
# changes do not recompile RocksDB.
FROM docker.io/library/rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev llvm-dev libc++-dev cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Dependencies first, against a stub main, so the expensive RocksDB build is
# cached until Cargo.toml/Cargo.lock actually change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
    && echo 'fn main() {}' > src/bin/rusts3.rs \
    && echo '' > src/lib.rs \
    && cargo build --release --bin rusts3 \
    && rm -rf src

COPY src ./src
# Cargo skips a rebuild when only mtimes moved, and the stub above left stale
# fingerprints for this crate; touching the real entry points forces it.
RUN touch src/lib.rs src/bin/rusts3.rs \
    && cargo build --release --bin rusts3 \
    && strip target/release/rusts3

# ── runtime ─────────────────────────────────────────────────────────────────
FROM docker.io/library/debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /home/rusts3 rusts3 \
    && mkdir -p /data /etc/rusts3 \
    && chown -R rusts3:rusts3 /data

COPY --from=builder /build/target/release/rusts3 /usr/local/bin/rusts3
COPY config.docker.yaml /etc/rusts3/config.yaml

# Everything durable lives here: buckets, the IAM database, scan history, logs.
VOLUME ["/data"]
# 8002 S3 API, 8003 management console.
EXPOSE 8002 8003

USER rusts3
WORKDIR /data

# The S3 API answers /minio/health/live unauthenticated, which is exactly what
# a health check needs — it proves the listener is up without credentials.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/rusts3", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/rusts3"]
CMD ["run", "-c", "/etc/rusts3/config.yaml"]
