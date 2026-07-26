#!/usr/bin/env bash
#
# Example: run rusts3 with podman, configured entirely through environment
# variables.
#
# The image ships config.docker.yaml, where every field is a
# {{RUSTS3_NAME:default}} placeholder expanded from the environment at startup.
# So you configure the server purely with -e flags — no config file to template
# or mount. Anything you don't set falls back to the default baked into the
# image. The full list of variables is at the bottom of this file.
#
set -euo pipefail

IMAGE="${IMAGE:-docker.io/wushilin/rusts3:latest}"
NAME="${NAME:-rusts3}"

# Durable state (buckets, IAM db, logs) lives on this named volume at /data.
VOLUME="${VOLUME:-rusts3-data}"

# Host ports to publish: S3 API and management UI.
S3_PORT="${S3_PORT:-8002}"
UI_PORT="${UI_PORT:-8003}"

# Replace any existing container of the same name.
podman rm -f "${NAME}" >/dev/null 2>&1 || true

podman run -d \
    --name "${NAME}" \
    -p "${S3_PORT}:8002" \
    -p "${UI_PORT}:8003" \
    -v "${VOLUME}:/data" \
    -e RUSTS3_ADMIN_USER="admin" \
    -e RUSTS3_ADMIN_PASSWORD="change-me-please" \
    -e RUSTS3_ACCESS_KEY="my-access-key" \
    -e RUSTS3_SECRET_KEY="my-secret-key" \
    -e RUSTS3_LOG_LEVEL="info" \
    -e RUSTS3_DURABILITY="full" \
    "${IMAGE}"

echo "rusts3 is starting as container '${NAME}'"
echo "  S3 API:       http://localhost:${S3_PORT}"
echo "  Management UI: http://localhost:${UI_PORT}  (login: admin / change-me-please)"
echo
echo "  Health:  curl -s http://localhost:${S3_PORT}/minio/health/live -o /dev/null -w '%{http_code}\\n'"
echo "  Logs:    podman logs -f ${NAME}"
echo "  Stop:    podman rm -f ${NAME}"

# ─────────────────────────────────────────────────────────────────────────────
# All supported environment variables (shown as VARIABLE=default):
#
#   server
#     RUSTS3_BIND_ADDRESS=0.0.0.0     RUSTS3_PORT=8002     RUSTS3_DATA_DIR=/data
#   storage
#     RUSTS3_META_CACHE_CAPACITY=200000   RUSTS3_DURABILITY=full   (full|relaxed)
#     RUSTS3_REBUILD_READER_THREADS=0     RUSTS3_REBUILD_QUEUE_BOUND=1000
#     RUSTS3_REBUILD_BATCH_SIZE=1000
#   logging
#     RUSTS3_LOG_DIR=logs   RUSTS3_LOG_LEVEL=info   RUSTS3_BANDWIDTH_REPORT=true
#     RUSTS3_LOG_ROTATION_MB=100   RUSTS3_LOG_KEEP_FILES=5   RUSTS3_LOG_COMPRESS=true
#   auth
#     RUSTS3_AUTH_ENABLED=true
#     RUSTS3_ADMIN_USER=admin         RUSTS3_ADMIN_PASSWORD=rusts3admin
#     RUSTS3_ACCESS_KEY=rusts3admin   RUSTS3_SECRET_KEY=rusts3admin
#     RUSTS3_PUBLIC_HOSTNAME=         RUSTS3_PUBLIC_SCHEME=http
#   sweeper
#     RUSTS3_SWEEP_INTERVAL_SECS=300      RUSTS3_INTENT_BATCH_SIZE=100
#     RUSTS3_INTENT_GRACE_SECS=3600       RUSTS3_STAGING_EXPIRY_SECS=86400
#     RUSTS3_MULTIPART_EXPIRY_SECS=2592000  (0 disables)
#     RUSTS3_TRASH_EXPIRY_SECS=86400      RUSTS3_RECLAIM_INTERVAL_SECS=300
#   ui
#     RUSTS3_UI_ENABLED=true   RUSTS3_UI_BIND_ADDRESS=0.0.0.0   RUSTS3_UI_PORT=8003
# ─────────────────────────────────────────────────────────────────────────────
