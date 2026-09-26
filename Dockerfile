# rusts3 container image — runtime only, no in-container compile.
#
# Two binaries ship: the server (rusts3) and its CLI client (rs3), so a running
# container can be driven from inside without pulling a second image. Both are
# built outside the image and copied in from dist/linux-<arch>/, where <arch>
# is Docker's TARGETARCH (amd64, arm64) — so one Dockerfile serves a multi-arch
# `buildx` build as well as a single-arch local one. librocksdb-sys compiles
# RocksDB from source, which is slow, so it happens once against a warm cargo
# cache rather than on every image build:
#
#   cargo build --release --bin rusts3
#   cargo build --release --manifest-path client/Cargo.toml
#   mkdir -p dist/linux-amd64 && cp target/release/rusts3 client/target/release/rs3 dist/linux-amd64/
#   podman build -t rusts3:latest .
#   podman run -d -p 8002:8002 -p 8003:8003 -v rusts3-data:/data rusts3:latest
#
# (build-and-publish-docker.sh does all of that for you; the release workflow
# does the same with the static musl binaries for both architectures.)
#
# Base is Debian trixie (glibc 2.41), pinned by digest. A host-built glibc
# binary runs unmodified because trixie's libc is newer than typical build
# hosts; a static musl binary runs anywhere.
#
# Configuration is the shipped config.docker.yaml, whose every value is a
# {{RUSTS3_NAME:default}} placeholder expanded from the environment at startup —
# so the image runs unconfigured, and any field can be overridden with -e.
# Mount your own file over /etc/rusts3/config.yaml to go further.

FROM docker.io/library/debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd

RUN apt-get update && apt-get install -y --no-install-recommends \
        fish vim coreutils findutils grep sed gawk less procps util-linux \
        curl bash tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /home/rusts3 rusts3 \
    && mkdir -p /data /etc/rusts3 \
    && chown -R rusts3:rusts3 /data

ARG TARGETARCH
COPY dist/linux-${TARGETARCH}/rusts3 /usr/local/bin/rusts3
COPY dist/linux-${TARGETARCH}/rs3 /usr/local/bin/rs3
COPY config.docker.yaml /etc/rusts3/config.yaml

# Everything durable lives here: buckets, the IAM database, scan history, logs.
VOLUME ["/data"]
# 8002 S3 API, 8003 management console.
EXPOSE 8002 8003

USER rusts3
WORKDIR /data

# The S3 API answers /minio/health/live unauthenticated, which is exactly what
# a health check needs — it proves the listener is up without credentials.
# healthcheck reads the same config the server did to learn the real port, so
# it needs the config path explicitly (its default is relative to WORKDIR).
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/rusts3", "healthcheck", "-c", "/etc/rusts3/config.yaml"]

ENTRYPOINT ["/usr/local/bin/rusts3"]
CMD ["run", "-c", "/etc/rusts3/config.yaml"]
