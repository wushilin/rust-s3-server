#!/usr/bin/env bash
#
# Runs the object_store compatibility suite against a throwaway rusts3:
#   1. build & start rusts3 on a temp data dir and free ports
#   2. wait for health, create the test bucket via the UI API (curl, no SigV4)
#   3. run `cargo test`, pointing the object_store client at the server via env
#   4. tear the server and temp files down on exit
#
# Any extra args are forwarded to `cargo test` (e.g. a test name to filter, or
# `-- --nocapture`).
#
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"

S3_PORT="${S3_PORT:-18502}"
UI_PORT="${UI_PORT:-18503}"
ADMIN=admin
PW=compatadmin
AK=compatak
SK=compatsecret
BUCKET=compat-bucket

DATA="$(mktemp -d)"
CFG="$(mktemp)"
COOKIES="$(mktemp)"
cleanup() {
    [[ -n "${SRV:-}" ]] && kill "$SRV" 2>/dev/null || true
    [[ -n "${SRV:-}" ]] && wait "$SRV" 2>/dev/null || true
    rm -rf "$DATA" "$CFG" "$COOKIES"
}
trap cleanup EXIT

cat > "$CFG" <<YAML
server:
  bind_address: "127.0.0.1"
  bind_port: $S3_PORT
  base_dir: "$DATA"
logging:
  level: "warn"
stats:
  enabled: false
auth:
  enabled: true
  users:
    - user: "$ADMIN"
      password: "$PW"
      api_keys:
        - ak: "$AK"
          secret: "$SK"
ui:
  enabled: true
  bind_address: "127.0.0.1"
  bind_port: $UI_PORT
YAML

echo ">> building rusts3"
( cd "$ROOT" && cargo build --bin rusts3 )
BIN="$ROOT/target/debug/rusts3"

echo ">> starting rusts3 (data=$DATA, s3=$S3_PORT ui=$UI_PORT)"
"$BIN" run -c "$CFG" > "$DATA/server.log" 2>&1 &
SRV=$!

echo ">> waiting for health"
for _ in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:$S3_PORT/minio/health/live" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$SRV" 2>/dev/null; then
        echo "!! server exited during startup; log:" >&2
        cat "$DATA/server.log" >&2
        exit 1
    fi
    sleep 0.25
done

echo ">> creating bucket '$BUCKET' via the console API"
curl -sf -c "$COOKIES" -X POST -H 'content-type: application/json' \
    -d "{\"username\":\"$ADMIN\",\"password\":\"$PW\"}" \
    "http://127.0.0.1:$UI_PORT/api/login" >/dev/null
curl -sf -b "$COOKIES" -X POST -H 'content-type: application/json' \
    -d "{\"name\":\"$BUCKET\"}" \
    "http://127.0.0.1:$UI_PORT/api/buckets" >/dev/null

echo ">> running object_store compatibility tests"
export RUSTS3_COMPAT_ENDPOINT="http://127.0.0.1:$S3_PORT"
export RUSTS3_COMPAT_BUCKET="$BUCKET"
export RUSTS3_COMPAT_ACCESS_KEY="$AK"
export RUSTS3_COMPAT_SECRET_KEY="$SK"
cargo test "$@"
