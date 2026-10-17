# rusts3 webhook delivery — specification v1

A webhook is a changelog subscription the server drives: it POSTs
batches of events to a URL, in order, at least once. The event format is
§3 of [spec-replication.md](spec-replication.md); this document specifies
the request, the batch, the signature and the delivery rules. Covered by
the conformance tests in `src/server/spec_tests.rs`.

## 1. The request

```
POST <webhook url>
Content-Type: application/json
X-RS3-Subscription: <subscription id>
X-RS3-Delivery: <32 hex characters, unique per attempt>
X-RS3-Signature: t=<unix seconds>,v1=<64 hex>
<custom headers configured on the webhook>
```

- Exactly one batch per request.
- The URL must be `https://` and must not resolve to an internal address,
  unless the server's config relaxes that. Redirects are not followed.
- Custom headers cannot set `Host`, `Content-Type`, `Content-Length`,
  framing headers, `Proxy-*` or any `X-RS3-*` header.
- The request times out after `webhooks.request_timeout_secs` (10 s).

## 2. The signature

`v1` is `hex(HMAC-SHA256(secret, "<t>.<body>"))` where `<t>` is the
decimal unix time in the header and `<body>` the exact request body bytes.
The secret is shown once, at creation and at rotation. A receiver SHOULD
verify the signature and SHOULD reject a `t` far from its own clock.

## 3. The batch

```json
{"subscription": "wh_3f2a…", "bucket": "photos",
 "phase": "live", "from_seq": 101, "to_seq": 250,
 "snapshot_begin": false, "snapshot_end": false,
 "events": [ … ]}
```

| Field | Meaning |
|---|---|
| `subscription` | The webhook's id (same as `X-RS3-Subscription`). |
| `bucket` | The bucket. |
| `phase` | `live` or `snapshot`. |
| `from_seq`, `to_seq` | Live: the inclusive seq range this batch covers. Events inside it may be sparse: filtered out, or never committed. Consecutive live batches tile the log without overlap. Snapshot: both equal `snapshot_through`. |
| `snapshot_begin`, `snapshot_end` | Snapshot: this is the first / last batch of the walk. Both may be true on one batch (an empty bucket). |
| `snapshot_through` | Snapshot batches only: the changelog position the snapshot was taken at; live delivery continues at `snapshot_through + 1`. |
| `events` | Events (§3 of the replication spec). Live: `put`/`delete` with `seq`, `origin`, `ts_ms`. Snapshot: `exists`, no `seq`. |

Rules:

- Events are delivered in `seq` order; a receiver MUST skip any event
  whose `seq` is at or below the last one it applied (redelivery happens
  after a timeout or an error).
- A batch holds at most `batch_max_events` (500) events or
  `batch_max_bytes` (1 MiB) of body, per webhook or server default.
- A receiver that mirrors the bucket SHOULD delete every key it did not
  see between `snapshot_begin` and `snapshot_end`.
- The live `from_seq` of the first batch after a snapshot is
  `snapshot_through + 1`.

## 4. Responses and retries

- Any `2xx` acknowledges the batch; the subscription's position moves to
  `to_seq` (or the snapshot cursor).
- Anything else — a non-2xx status, a timeout, a connection error — mutes
  the webhook for `mute_secs` (30 s), or longer when the response carries a
  larger `Retry-After` (seconds or HTTP date; capped by
  `max_mute_secs`, 900 s). The same batch is sent again afterwards,
  followed by the backlog.
- Filtered-out changes still advance the position, so a batch may be
  empty-looking from the receiver's point of view only in that gaps
  appear between batches; the server never sends an empty live batch.
- If the webhook's position falls behind retention, it pauses (events
  were lost) and an admin decides how to resume.

## 5. Versioning

Fields MAY be added to the batch and to events in later versions; a
receiver MUST ignore unknown fields and MUST NOT depend on field order. A
receiver MUST NOT guess at an `op` or `origin` value it does not know.
