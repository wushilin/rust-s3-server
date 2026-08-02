#!/usr/bin/env bash
#
# Build the release binary, package it into the runtime image with podman, and
# publish it to Docker Hub as both :<version> and :latest.
#
# The version is read from Cargo.toml's [package] section. Pushing a tag that
# already exists in the registry overwrites it — that is intentional, so
# re-running this for an already-published version republishes it.
#
#   ./build-and-publish-docker.sh
#
set -euo pipefail

# Image repository to publish to. Override with IMAGE=... ./build-and-publish-docker.sh
IMAGE="${IMAGE:-docker.io/wushilin/rusts3}"

# Run from the repo root regardless of where the script is invoked from.
cd "$(dirname "$0")"

# Version from the [package] section of Cargo.toml (first version= after it).
VERSION="$(awk -F'"' '/^\[package\]/{p=1} p && /^version[[:space:]]*=/{print $2; exit}' Cargo.toml)"
if [[ -z "${VERSION}" ]]; then
    echo "error: could not detect version from Cargo.toml" >&2
    exit 1
fi

echo ">> Building rusts3 ${VERSION}"

# The binaries are built on the host (not in the container) so the slow RocksDB
# compile reuses the warm cargo cache instead of running on every image build.
cargo build --release --bin rusts3
strip target/release/rusts3

# The CLI client is a separate crate under client/ (not a workspace member), so
# it needs its own build. It ships in the image alongside the server.
echo ">> Building rs3 client"
cargo build --release --manifest-path client/Cargo.toml
strip client/target/release/rs3

echo ">> Building image ${IMAGE}:${VERSION} (and :latest)"
# --format docker so the Dockerfile's HEALTHCHECK is preserved (OCI drops it).
podman build --format docker \
    -t "${IMAGE}:${VERSION}" \
    -t "${IMAGE}:latest" \
    .

echo ">> Publishing ${IMAGE}:${VERSION}"
podman push "${IMAGE}:${VERSION}"

echo ">> Publishing ${IMAGE}:latest"
podman push "${IMAGE}:latest"

echo ">> Done: ${IMAGE}:${VERSION} and ${IMAGE}:latest published"
