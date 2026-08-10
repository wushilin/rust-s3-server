#!/usr/bin/env python3
"""Capture raw S3 wire responses from a target, so two targets can be diffed.

Why this exists
---------------
`rust-s3-server` is judged by one question: does a real S3 client get the same
answer it would get from Amazon? Unit tests cannot answer that -- they assert
what the author believed AWS does. This runs an identical scenario against
real S3 and against a local `rusts3`, records the *raw* status line, headers
and XML of every response, and diffs them. What survives normalisation is a
genuine behavioural difference.

Signing is implemented here rather than delegated to an SDK on purpose: an SDK
would helpfully paper over the very things being measured (it retries, it
normalises headers, it discards elements it does not model).

Usage
-----
    # Credentials come from the environment. They are never written to disk.
    export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...

    python3 s3probe.py capture --endpoint https://s3.ap-southeast-1.amazonaws.com \
        --region ap-southeast-1 --bucket my-bucket --prefix compat-probe \
        --out /tmp/aws.json

    python3 s3probe.py capture --endpoint http://127.0.0.1:9000 \
        --region us-east-1 --bucket my-bucket --prefix compat-probe \
        --path-style --out /tmp/local.json

    python3 s3probe.py diff /tmp/aws.json /tmp/local.json

The scenario creates and deletes only keys under `--prefix`.
"""

import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


# ── SigV4 ────────────────────────────────────────────────────────────────────

def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _signing_key(secret, date, region, service):
    k = _sign(f"AWS4{secret}".encode(), date)
    k = _sign(k, region)
    k = _sign(k, service)
    return _sign(k, "aws4_request")


def _uri_encode(value, encode_slash=True):
    out = []
    for ch in value.encode():
        c = chr(ch)
        if c.isalnum() or c in "-._~":
            out.append(c)
        elif c == "/" and not encode_slash:
            out.append(c)
        else:
            out.append(f"%{ch:02X}")
    return "".join(out)


def signed_request(cfg, method, path, query=None, headers=None, body=b""):
    """Issues one signed request and returns (status, headers, body_bytes)."""
    query = query or {}
    headers = dict(headers or {})
    body = body or b""

    # Always declare a content type when there is a body. `urllib` otherwise
    # defaults to `application/x-www-form-urlencoded`, and SigV4 says a POST
    # with that content type carries its parameters in the *body* -- so S3
    # parses the XML as form fields and folds them into the canonical query
    # string, producing a signature mismatch that looks like a credentials
    # problem. Costs one header; saves an afternoon.
    if body and not any(k.lower() == "content-type" for k in headers):
        headers["content-type"] = "application/octet-stream"

    host = cfg["host"]
    now = dt.datetime.now(dt.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()

    headers["host"] = host
    headers["x-amz-date"] = amz_date
    headers["x-amz-content-sha256"] = payload_hash

    canonical_uri = _uri_encode(path, encode_slash=False)
    # Canonical query: sorted by encoded key, then value. Repeated keys are not
    # used by this scenario, so a plain sort is sufficient.
    canonical_query = "&".join(
        f"{_uri_encode(k)}={_uri_encode(v)}"
        for k, v in sorted(query.items())
    )
    # Repeated headers are folded comma-joined in receipt order; this scenario
    # sends each header once, so a dict is sufficient.
    lowered = {k.lower(): " ".join(str(v).split()) for k, v in headers.items()}
    signed_names = ";".join(sorted(lowered))
    canonical_headers = "".join(f"{k}:{lowered[k]}\n" for k in sorted(lowered))
    canonical_request = "\n".join(
        [method, canonical_uri, canonical_query, canonical_headers,
         signed_names, payload_hash]
    )

    scope = f"{date_stamp}/{cfg['region']}/s3/aws4_request"
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope,
         hashlib.sha256(canonical_request.encode()).hexdigest()]
    )
    signature = hmac.new(
        _signing_key(cfg["secret"], date_stamp, cfg["region"], "s3"),
        string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={cfg['access']}/{scope}, "
        f"SignedHeaders={signed_names}, Signature={signature}"
    )

    url = f"{cfg['scheme']}://{host}{canonical_uri}"
    if canonical_query:
        url += "?" + canonical_query
    req = urllib.request.Request(url, data=body or None, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read()


# ── Normalisation ────────────────────────────────────────────────────────────

# Headers that differ per request or per vendor by design. Recorded as present
# or absent, never compared by value.
VOLATILE_HEADERS = {
    "date", "x-amz-request-id", "x-amz-id-2", "last-modified", "server",
    "connection", "x-amz-server-side-encryption", "transfer-encoding",
    "content-length", "x-amz-version-id", "etag", "x-amz-checksum-crc32",
    "x-amz-checksum-crc64nvme", "x-amz-checksum-type", "keep-alive",
}
# XML elements whose values are inherently per-request or per-account.
VOLATILE_ELEMENTS = {
    "RequestId", "HostId", "LastModified", "UploadId", "ID", "DisplayName",
    "Initiated", "CreationDate", "ETag", "Name", "Bucket", "Location",
    "ChecksumCRC32", "ChecksumCRC64NVME", "ChecksumType", "Checksum",
}


def flatten_xml(body):
    """Returns the document as an ordered list of "path=value" strings.

    Order is preserved because S3 element order is part of the contract for
    several shapes, and a set comparison would hide a reordering.
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    out = []

    def strip_ns(tag):
        return tag.split("}", 1)[-1]

    def walk(node, path):
        name = strip_ns(node.tag)
        here = f"{path}/{name}" if path else name
        text = (node.text or "").strip()
        children = list(node)
        if not children:
            value = "<VOLATILE>" if name in VOLATILE_ELEMENTS else text
            out.append(f"{here}={value}")
        else:
            out.append(f"{here}/")
            for child in children:
                walk(child, here)

    walk(root, "")
    return out


def normalize(status, headers, body, subs):
    lowered = {k.lower(): v for k, v in headers.items()}
    header_view = {}
    for name, value in sorted(lowered.items()):
        header_view[name] = "<PRESENT>" if name in VOLATILE_HEADERS else value
    text = body.decode("utf-8", "replace")
    for needle, token in subs:
        if needle:
            text = text.replace(needle, token)
    return {
        "status": status,
        "headers": header_view,
        "xml": flatten_xml(text.encode()),
        "body_len": len(body),
        "body": text if len(text) < 4000 and not text.startswith("<?xml") else None,
    }


# ── Scenario ─────────────────────────────────────────────────────────────────

def run_scenario(cfg, bucket, prefix):
    """Exercises every verb the server implements and records each response."""
    results = {}
    # Tokens must not contain angle brackets: they are substituted into the
    # raw body before it is parsed, and "<BUCKET>" would read as an unclosed
    # element and silently turn every XML comparison into "did not parse".
    subs = [(bucket, "~BUCKET~"), (prefix, "~PREFIX~")]

    def record(name, method, path, query=None, headers=None, body=b""):
        status, resp_headers, resp_body = signed_request(
            cfg, method, path, query, headers, body
        )
        results[name] = normalize(status, resp_headers, resp_body, subs)
        return status, resp_headers, resp_body

    small = b"hello compat probe\n"
    part1 = b"a" * (5 * 1024 * 1024)
    part2 = b"b" * (1 * 1024 * 1024)
    key = f"/{bucket}/{prefix}/small.txt"
    mpu_key = f"/{bucket}/{prefix}/mpu.bin"

    # ── bucket-level reads ──
    record("head_bucket", "HEAD", f"/{bucket}")
    record("get_bucket_location", "GET", f"/{bucket}", {"location": ""})

    # ── single-object lifecycle ──
    record("put_object", "PUT", key, headers={"content-type": "text/plain"}, body=small)
    record("head_object", "HEAD", key)
    record("get_object", "GET", key)
    record("get_object_range", "GET", key, headers={"range": "bytes=0-4"})
    record("get_object_range_unsatisfiable", "GET", key, headers={"range": "bytes=9999-"})
    record("get_object_part_number_1", "GET", key, {"partNumber": "1"})
    record("get_object_part_number_2", "GET", key, {"partNumber": "2"})
    record("get_object_part_number_zero", "GET", key, {"partNumber": "0"})
    record("get_object_part_number_bad", "GET", key, {"partNumber": "abc"})
    record("get_object_if_match_bad", "GET", key, headers={"if-match": '"0"'})
    record("get_object_attributes_single", "GET", key, {"attributes": ""},
           {"x-amz-object-attributes": "ETag,ObjectSize,StorageClass,ObjectParts"})

    # ── listings ──
    record("list_objects_v2", "GET", f"/{bucket}", {"list-type": "2", "prefix": f"{prefix}/"})
    record("list_objects_v1", "GET", f"/{bucket}", {"prefix": f"{prefix}/"})
    record("list_objects_v2_delimiter", "GET", f"/{bucket}",
           {"list-type": "2", "prefix": f"{prefix}/", "delimiter": "/", "max-keys": "1"})
    record("list_object_versions", "GET", f"/{bucket}", {"versions": "", "prefix": f"{prefix}/"})

    # ── copy ──
    record("copy_object", "PUT", f"/{bucket}/{prefix}/copy.txt",
           headers={"x-amz-copy-source": f"/{bucket}/{prefix}/small.txt"})

    # ── multipart lifecycle ──
    status, _, body = record("create_multipart", "POST", mpu_key, {"uploads": ""})
    upload_id = ""
    m = re.search(r"<UploadId>([^<]+)</UploadId>", body.decode("utf-8", "replace"))
    if m:
        upload_id = m.group(1)
        subs.append((upload_id, "~UPLOADID~"))

    etags = []
    if upload_id:
        for n, chunk in ((1, part1), (2, part2)):
            _, hdrs, _ = record(
                f"upload_part_{n}", "PUT", mpu_key,
                {"uploadId": upload_id, "partNumber": str(n)}, body=chunk
            )
            etags.append(hdrs.get("ETag") or hdrs.get("etag") or "")
        record("list_parts", "GET", mpu_key, {"uploadId": upload_id})
        record("list_multipart_uploads", "GET", f"/{bucket}", {"uploads": "", "prefix": f"{prefix}/"})
        parts_xml = "".join(
            f"<Part><PartNumber>{i + 1}</PartNumber><ETag>{e}</ETag></Part>"
            for i, e in enumerate(etags)
        )
        record("complete_multipart", "POST", mpu_key, {"uploadId": upload_id},
               {"content-type": "application/xml"},
               f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode())

    # ── multipart object reads ──
    record("mpu_head", "HEAD", mpu_key)
    record("mpu_head_part_1", "HEAD", mpu_key, {"partNumber": "1"})
    record("mpu_head_part_2", "HEAD", mpu_key, {"partNumber": "2"})
    record("mpu_head_part_9", "HEAD", mpu_key, {"partNumber": "9"})
    record("mpu_get_part_and_range", "GET", mpu_key, {"partNumber": "1"},
           {"range": "bytes=0-15"})
    record("mpu_attributes_all", "GET", mpu_key, {"attributes": ""},
           {"x-amz-object-attributes": "ETag,ObjectSize,StorageClass,ObjectParts"})
    record("mpu_attributes_parts_paged", "GET", mpu_key, {"attributes": ""},
           {"x-amz-object-attributes": "ObjectParts", "x-amz-max-parts": "1"})
    record("mpu_attributes_bad_name", "GET", mpu_key, {"attributes": ""},
           {"x-amz-object-attributes": "Bogus"})
    record("mpu_attributes_no_header", "GET", mpu_key, {"attributes": ""})

    # ── errors ──
    record("get_missing_key", "GET", f"/{bucket}/{prefix}/definitely-absent")
    record("head_missing_key", "HEAD", f"/{bucket}/{prefix}/definitely-absent")
    record("attributes_missing_key", "GET", f"/{bucket}/{prefix}/definitely-absent",
           {"attributes": ""}, {"x-amz-object-attributes": "ETag"})

    # ── deletes (also the cleanup path) ──
    delete_xml = (
        "<Delete>"
        f"<Object><Key>{prefix}/copy.txt</Key></Object>"
        f"<Object><Key>{prefix}/mpu.bin</Key></Object>"
        "</Delete>"
    ).encode()
    record("delete_objects", "POST", f"/{bucket}", {"delete": ""},
           {"content-md5": _content_md5(delete_xml), "content-type": "application/xml"},
           delete_xml)
    record("delete_object", "DELETE", key)
    record("delete_object_absent", "DELETE", f"/{bucket}/{prefix}/definitely-absent")
    return results


def _content_md5(body):
    import base64
    return base64.b64encode(hashlib.md5(body).digest()).decode()


# ── Diff ─────────────────────────────────────────────────────────────────────

def diff_captures(left, right, left_name, right_name):
    findings = []
    for op in left:
        if op not in right:
            findings.append((op, "missing", f"absent from {right_name}"))
            continue
        a, b = left[op], right[op]
        if a["status"] != b["status"]:
            findings.append(
                (op, "status", f"{left_name}={a['status']} {right_name}={b['status']}")
            )
        for name in sorted(set(a["headers"]) | set(b["headers"])):
            av, bv = a["headers"].get(name), b["headers"].get(name)
            if av == bv:
                continue
            if av is None:
                findings.append((op, "header", f"only {right_name} sends {name}: {bv!r}"))
            elif bv is None:
                findings.append((op, "header", f"only {left_name} sends {name}: {av!r}"))
            else:
                findings.append((op, "header", f"{name}: {left_name}={av!r} {right_name}={bv!r}"))
        ax, bx = a["xml"], b["xml"]
        if ax != bx:
            if ax is None or bx is None:
                findings.append((op, "xml", f"{left_name} parsed={ax is not None} {right_name} parsed={bx is not None}"))
            else:
                only_a = [x for x in ax if x not in bx]
                only_b = [x for x in bx if x not in ax]
                if only_a:
                    findings.append((op, "xml", f"only {left_name}: {only_a}"))
                if only_b:
                    findings.append((op, "xml", f"only {right_name}: {only_b}"))
                if not only_a and not only_b:
                    findings.append((op, "xml", f"element order differs: {left_name}={ax} {right_name}={bx}"))
    for op in right:
        if op not in left:
            findings.append((op, "missing", f"absent from {left_name}"))
    return findings


def is_accepted(finding, rules):
    """Is this finding covered by a rule in the accepted-divergences file?

    Accepting a divergence is deliberate: every rule carries the reason it is
    acceptable, so the file doubles as the list of known differences from real
    S3. Anything not covered fails a strict run.
    """
    op, kind, detail = finding
    for rule in rules:
        if rule.get("op") not in ("*", op):
            continue
        if rule.get("kind") != kind:
            continue
        if rule.get("contains", "") in detail:
            return True
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    cap = sub.add_parser("capture")
    cap.add_argument("--endpoint", required=True)
    cap.add_argument("--region", default="us-east-1")
    cap.add_argument("--bucket", required=True)
    cap.add_argument("--prefix", default="compat-probe")
    cap.add_argument("--out", required=True)

    mk = sub.add_parser(
        "create-bucket",
        help="create the scenario bucket (AWS already has one; a fresh rusts3 does not)",
    )
    mk.add_argument("--endpoint", required=True)
    mk.add_argument("--region", default="us-east-1")
    mk.add_argument("--bucket", required=True)

    dif = sub.add_parser("diff")
    dif.add_argument("left")
    dif.add_argument("right")
    dif.add_argument("--left-name", default="aws")
    dif.add_argument("--right-name", default="rusts3")
    dif.add_argument("--accepted", help="JSON file of accepted divergences")
    dif.add_argument("--strict", action="store_true",
                     help="exit non-zero if any finding is not accepted")

    args = ap.parse_args()
    if args.cmd == "create-bucket":
        scheme, _, host = args.endpoint.partition("://")
        access = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not access or not secret:
            sys.exit("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
        cfg = {"scheme": scheme, "host": host, "region": args.region,
               "access": access, "secret": secret}
        status, _, body = signed_request(cfg, "PUT", f"/{args.bucket}")
        # 409 means it already exists, which is exactly the state we wanted.
        if status not in (200, 409):
            sys.exit(f"create bucket failed: {status} {body[:300]!r}")
        print(f"bucket {args.bucket} ready ({status})")
    elif args.cmd == "capture":
        scheme, _, host = args.endpoint.partition("://")
        access = os.environ.get("AWS_ACCESS_KEY_ID")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not access or not secret:
            sys.exit("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
        cfg = {"scheme": scheme, "host": host, "region": args.region,
               "access": access, "secret": secret}
        results = run_scenario(cfg, args.bucket, args.prefix)
        with open(args.out, "w") as fh:
            json.dump(results, fh, indent=2, sort_keys=True)
        print(f"captured {len(results)} operations -> {args.out}")
    else:
        with open(args.left) as fh:
            left = json.load(fh)
        with open(args.right) as fh:
            right = json.load(fh)
        findings = diff_captures(left, right, args.left_name, args.right_name)
        rules = []
        if args.accepted:
            with open(args.accepted) as fh:
                rules = json.load(fh).get("rules", [])
        unaccepted = [f for f in findings if not is_accepted(f, rules)]
        shown = unaccepted if args.strict else findings
        current = None
        for op, kind, detail in shown:
            if op != current:
                print(f"\n### {op}")
                current = op
            print(f"  [{kind}] {detail}")
        accepted_count = len(findings) - len(unaccepted)
        print(
            f"\n{len(findings)} difference(s) across {len(left)} operations: "
            f"{accepted_count} accepted, {len(unaccepted)} unaccounted for"
        )
        if args.strict and unaccepted:
            print(
                "\nFAIL: the differences above are not in the accepted list.\n"
                "Either fix the server, or -- if the divergence is deliberate --\n"
                "add a rule with its reason to the accepted-divergences file."
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
