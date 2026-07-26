# object_store compatibility harness

Proves that rusts3 behaves the way the [`object_store`](https://crates.io/crates/object_store)
crate expects — the S3 client used by Delta Lake, DataFusion, Lance, Iceberg,
and many other Rust data apps. If these pass, those apps' storage layer works
against rusts3.

## Running

```sh
./run.sh
```

The launcher is self-contained: it builds rusts3, starts a throwaway instance on
a temp data dir and free ports, creates the test bucket through the console API,
runs the tests against it, and tears everything down on exit. Extra arguments are
forwarded to `cargo test`:

```sh
./run.sh conditional            # filter by test name
./run.sh -- --nocapture         # see client/server output
```

Ports default to S3 `18502` / UI `18503`; override with `S3_PORT` / `UI_PORT`.

To point the tests at an already-running server instead of the launcher, set
`RUSTS3_COMPAT_ENDPOINT`, `RUSTS3_COMPAT_BUCKET`, `RUSTS3_COMPAT_ACCESS_KEY`,
`RUSTS3_COMPAT_SECRET_KEY` and run `cargo test` directly.

## What it covers

put / get roundtrip, overwrite, empty objects, `head` (size + ETag), `get_range`
and `get_ranges`, `NotFound` mapping, recursive `list`, `list_with_delimiter`
(common prefixes), `list_with_offset`, `copy`, `rename` (copy + delete),
multipart upload of a large object, multipart abort, and **conditional PUT**
(`PutMode::Create` via `If-None-Match: *` — the atomic-commit primitive Delta
Lake and Iceberg rely on).

## Known gaps

None currently. The related `copy_if_not_exists` (conditional *COPY*) is not yet
exercised here; it shares the server's precondition mechanism and is a candidate
for a future addition.
