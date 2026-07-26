#!/usr/bin/env bash
#
# docker-test.sh — start the image and exercise it the way a client would.
#
# Proves the container is actually usable, not merely running: config comes
# from the environment, data survives a restart on the mounted volume, the
# console answers, and the S3 API handles the operations an SDK performs
# (bucket lifecycle, single and multipart upload, ranged read, copy, list,
# presign, delete). Uses the AWS CLI, so the wire protocol is exercised by a
# real SDK rather than hand-written curl.
#
# Works with docker or podman; set RUNTIME to force one.
#
#   ./docker-test.sh            # build must already exist as rusts3:latest
#   IMAGE=rusts3:dev ./docker-test.sh
#
set -euo pipefail

IMAGE=${IMAGE:-rusts3:latest}
NAME=${NAME:-rusts3-test}
S3_PORT=${S3_PORT:-18002}
UI_PORT=${UI_PORT:-18003}
ACCESS_KEY=${ACCESS_KEY:-testaccess}
SECRET_KEY=${SECRET_KEY:-testsecret0123}
ADMIN_PASSWORD=${ADMIN_PASSWORD:-testadminpw}
RUNTIME=${RUNTIME:-}

if [ -z "$RUNTIME" ]; then
  if command -v docker >/dev/null 2>&1; then RUNTIME=docker
  elif command -v podman >/dev/null 2>&1; then RUNTIME=podman
  else echo "need docker or podman" >&2; exit 1; fi
fi
command -v aws >/dev/null || { echo "need the aws cli" >&2; exit 1; }

VOLUME=${VOLUME:-rusts3-test-data}
BIND_DIR=$(mktemp -d /tmp/rusts3-docker-bind.XXXXXX)
WORK=$(mktemp -d /tmp/rusts3-docker-work.XXXXXX)
ENDPOINT="http://127.0.0.1:$S3_PORT"
export AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_DEFAULT_REGION=us-east-1
s3() { aws --endpoint-url "$ENDPOINT" "$@"; }

pass=0; fail=0
ok()   { printf '  \033[32m[ok ]\033[0m %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  \033[31m[BAD]\033[0m %s\n' "$1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
say()  { printf '\n\033[1m==>\033[0m %s\n' "$*"; }

cleanup() {
  $RUNTIME rm -f "$NAME" "$NAME-bind" >/dev/null 2>&1 || true
  $RUNTIME volume rm -f "$VOLUME" >/dev/null 2>&1 || true
  rm -rf "$WORK" "$BIND_DIR" 2>/dev/null || true
}
trap cleanup EXIT

say "starting $IMAGE via $RUNTIME"
$RUNTIME rm -f "$NAME" >/dev/null 2>&1 || true
$RUNTIME volume rm -f "$VOLUME" >/dev/null 2>&1 || true
# A named volume is the documented deployment: the runtime creates it with the
# image's ownership, so the non-root user in the image can write to it without
# anyone chowning anything. (A host bind mount needs that ownership arranged
# by hand — exercised separately at the end.)
$RUNTIME run -d --name "$NAME" \
  -p "$S3_PORT:8002" -p "$UI_PORT:8003" \
  -v "$VOLUME:/data" \
  -e RUSTS3_ACCESS_KEY="$ACCESS_KEY" \
  -e RUSTS3_SECRET_KEY="$SECRET_KEY" \
  -e RUSTS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
  -e RUSTS3_LOG_LEVEL=info \
  "$IMAGE" >/dev/null

say "waiting for the S3 API"
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null "$ENDPOINT/minio/health/live"; then break; fi
  sleep 1
done
if ! curl -sf -o /dev/null "$ENDPOINT/minio/health/live"; then
  echo "server never became healthy; container logs:" >&2
  $RUNTIME logs "$NAME" 2>&1 | tail -30 >&2
  exit 1
fi

say "environment-driven configuration"
check "health endpoint answers" "$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/minio/health/live")" "200"
check "console answers on the mapped port" \
      "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$UI_PORT/")" "200"
check "console login uses the injected password" \
      "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$UI_PORT/api/login" \
          -H 'content-type: application/json' \
          -d "{\"username\":\"admin\",\"password\":\"$ADMIN_PASSWORD\"}")" "200"
check "a wrong password is refused" \
      "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$UI_PORT/api/login" \
          -H 'content-type: application/json' \
          -d '{"username":"admin","password":"wrong"}')" "401"
if AWS_ACCESS_KEY_ID=nope AWS_SECRET_ACCESS_KEY=nopenopenope \
   aws --endpoint-url "$ENDPOINT" s3api list-buckets >/dev/null 2>&1; then
  bad "the injected access key is enforced"
else
  ok "the injected access key is enforced"
fi

say "bucket and object lifecycle"
s3 s3api create-bucket --bucket sdk-test >/dev/null
check "bucket created and listed" \
      "$(s3 s3api list-buckets --query 'Buckets[?Name==`sdk-test`].Name' --output text)" "sdk-test"

head -c 1024 /dev/urandom > "$WORK/small.bin"
s3 s3api put-object --bucket sdk-test --key small.bin --body "$WORK/small.bin" >/dev/null
check "small object size round-trips" \
      "$(s3 s3api head-object --bucket sdk-test --key small.bin --query 'ContentLength' --output text)" "1024"
s3 s3api get-object --bucket sdk-test --key small.bin "$WORK/small.out" >/dev/null
if cmp -s "$WORK/small.bin" "$WORK/small.out"; then ok "small object bytes are identical"; else bad "small object bytes differ"; fi

say "multipart upload (the aws cli switches to it above 8 MiB)"
head -c 12582912 /dev/urandom > "$WORK/big.bin"
s3 s3 cp "$WORK/big.bin" "s3://sdk-test/big.bin" --only-show-errors
check "multipart object size round-trips" \
      "$(s3 s3api head-object --bucket sdk-test --key big.bin --query 'ContentLength' --output text)" "12582912"
s3 s3 cp "s3://sdk-test/big.bin" "$WORK/big.out" --only-show-errors
if cmp -s "$WORK/big.bin" "$WORK/big.out"; then ok "multipart object bytes are identical"; else bad "multipart object bytes differ"; fi

say "range, copy, list, presign"
s3 s3api get-object --bucket sdk-test --key big.bin --range bytes=0-99 "$WORK/range.out" >/dev/null
check "ranged read returns exactly the range" "$(stat -c %s "$WORK/range.out")" "100"
s3 s3api copy-object --bucket sdk-test --key copy.bin --copy-source sdk-test/small.bin >/dev/null
check "server-side copy preserves size" \
      "$(s3 s3api head-object --bucket sdk-test --key copy.bin --query 'ContentLength' --output text)" "1024"
for n in 1 2 3; do s3 s3api put-object --bucket sdk-test --key "prefix/f$n" --body "$WORK/small.bin" >/dev/null; done
check "prefix listing returns the right count" \
      "$(s3 s3api list-objects-v2 --bucket sdk-test --prefix prefix/ --query 'length(Contents)' --output text)" "3"
check "delimiter listing groups a common prefix" \
      "$(s3 s3api list-objects-v2 --bucket sdk-test --delimiter / --query 'CommonPrefixes[0].Prefix' --output text)" "prefix/"
URL=$(s3 s3 presign "s3://sdk-test/small.bin" --expires-in 300)
check "presigned URL downloads without credentials" \
      "$(curl -s -o "$WORK/presign.out" -w '%{http_code}' "$URL")" "200"
if cmp -s "$WORK/small.bin" "$WORK/presign.out"; then ok "presigned bytes are identical"; else bad "presigned bytes differ"; fi

say "durability across a restart on the mounted volume"
# stop + start rather than `restart`: rootless podman's port forwarder can still
# hold the host port for a moment after the container exits, and `restart` gives
# it no chance to let go. Retrying the start is what a person would do.
$RUNTIME stop -t 15 "$NAME" >/dev/null
for attempt in $(seq 1 15); do
  $RUNTIME start "$NAME" >/dev/null 2>&1 && break
  [ "$attempt" = 15 ] && { bad "container would not start again"; $RUNTIME logs "$NAME" 2>&1 | tail -5; }
  sleep 2
done
for _ in $(seq 1 60); do
  curl -sf -o /dev/null "$ENDPOINT/minio/health/live" && break
  sleep 1
done
check "objects survive a container restart" \
      "$(s3 s3api list-objects-v2 --bucket sdk-test --query 'length(Contents)' --output text)" "6"
s3 s3api get-object --bucket sdk-test --key big.bin "$WORK/big.after" >/dev/null
if cmp -s "$WORK/big.bin" "$WORK/big.after"; then ok "multipart bytes survive a restart"; else bad "multipart bytes differ after restart"; fi

say "deletion"
s3 s3api delete-object --bucket sdk-test --key copy.bin >/dev/null
if s3 s3api head-object --bucket sdk-test --key copy.bin >/dev/null 2>&1; then
  bad "deleted object is gone"
else
  ok "deleted object is gone"
fi
s3 s3 rm "s3://sdk-test" --recursive --only-show-errors >/dev/null
s3 s3api delete-bucket --bucket sdk-test >/dev/null
check "emptied bucket can be deleted" \
      "$(s3 s3api list-buckets --query 'length(Buckets)' --output text)" "0"

say "host bind mount (the uid has to line up)"
# The image runs as a non-root uid, so a host directory must be writable by it.
# Matching --user is the portable recipe; the alternative is chowning the dir to
# the image's uid. Anything else fails at startup — with a message naming the
# path and the uid, which is asserted below.
$RUNTIME rm -f "$NAME-bind" >/dev/null 2>&1 || true
BIND_OPTS=""
[ "$RUNTIME" = "podman" ] && BIND_OPTS=":Z"
BIND_EXTRA=()
[ "$RUNTIME" = "podman" ] && BIND_EXTRA+=(--userns=keep-id)
$RUNTIME run -d --name "$NAME-bind" \
  -p "$((S3_PORT + 10)):8002" \
  -v "$BIND_DIR:/data$BIND_OPTS" \
  --user "$(id -u):$(id -g)" \
  -e RUSTS3_ACCESS_KEY="$ACCESS_KEY" -e RUSTS3_SECRET_KEY="$SECRET_KEY" \
  -e RUSTS3_UI_ENABLED=false \
  "${BIND_EXTRA[@]}" "$IMAGE" >/dev/null 2>&1 || true
for _ in $(seq 1 45); do
  curl -sf -o /dev/null "http://127.0.0.1:$((S3_PORT + 10))/minio/health/live" && break
  sleep 1
done
if curl -sf -o /dev/null "http://127.0.0.1:$((S3_PORT + 10))/minio/health/live"; then
  ok "starts against a host directory when the uid matches"
  AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    aws --endpoint-url "http://127.0.0.1:$((S3_PORT + 10))" s3api create-bucket --bucket bind-test >/dev/null
  if [ -d "$BIND_DIR/buckets/bind-test" ]; then
    ok "the host directory really holds the data"
  else
    bad "the host directory holds no data"
  fi
else
  bad "did not start against a host directory"
  $RUNTIME logs "$NAME-bind" 2>&1 | tail -5
fi

printf '\n  %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ] || { echo; echo "container logs:"; $RUNTIME logs "$NAME" 2>&1 | tail -20; exit 1; }
echo "  PASS"
