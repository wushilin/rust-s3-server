# rs3

`rs3` is a Rust S3-compatible client.

The MinIO `mc` source is cloned into `mc-reference/` for behavior reference, but
`rs3` intentionally focuses on portable S3 bucket/object operations. MinIO-only
admin and management command families are not exposed because rusts3 is not
expected to implement those APIs.

## Implemented

- `alias set|list|remove|export`
- `ls`
- `mb`
- `rb`, including `--force` (aborts incomplete multipart uploads, empties the
  bucket, then deletes it) and `--dangerous` (required to remove every bucket
  on an alias when no bucket is named)
- `put`
- `cp` for local-to-S3 and S3-to-local
- `get`
- `cat`
- `rm`, including recursive removal with batched `DeleteObjects` calls
  (1000 keys per batch); `--recursive` requires `--force` (or `--dry-run`)
- `stat`
- `mirror` for local directory -> S3 prefix, S3 prefix -> local directory,
  and S3-compatible prefix -> S3-compatible prefix, including
  S3-compatible-to-S3-compatible across different aliases; local-to-local
  mirroring is rejected. `mirror` is incremental (size/mtime diff) and
  supports `--remove` (delete destination-only entries), `--overwrite`
  (replace differing destination entries), and `--dry-run` (print the plan
  without transferring)

## Copy And Mirror Behavior

`cp` supports:

- local file -> S3 object
- local directory -> S3 prefix with `--recursive`
- S3 object -> local file/directory
- S3 prefix -> local directory with `--recursive`
- S3 object -> S3 object/prefix
- S3 prefix -> S3 prefix with `--recursive`

`mirror` supports:

- local directory -> S3 prefix
- S3 prefix -> local directory
- S3 prefix -> S3 prefix, including across different S3-compatible aliases

Large uploads and S3-to-S3 copies use multipart when object size is above the
configured part size. The default is `256MiB`; use `--part-size` to override it.
Multipart part uploads are parallelized with `--parallel` workers.

S3-to-S3 copies stream small objects directly from `GetObject` into `PutObject`.
Large S3-to-S3 copies use parallel ranged `GetObject` requests feeding multipart
`UploadPart` requests, avoiding whole-object buffering.

When source and target resolve to the same alias/endpoint, `cp`/`mirror` use
server-side copy instead (`CopyObject` for whole-object copies, or parallel
`UploadPartCopy` requests for objects above the multipart threshold), so data
never round-trips through the client.

Downloads (`get`, `cp`, `mirror` to a local path) above the multipart
threshold use parallel ranged `GetObject` requests writing into a `.rs3.part`
sibling file, which is atomically renamed into place once every range has
completed successfully.

## Intentionally Dropped

MinIO-specific or non-portable `mc` command families are not part of the `rs3`
compatibility target:

- `admin`
- `anonymous` / `policy`
- `batch`
- `cors`
- `diff`
- `du`
- `encrypt`
- `event`
- `find`
- `head`
- `ilm`
- `idp`
- `license`
- `legalhold`
- `mv`
- `od`
- `ping`
- `pipe`
- `quota`
- `ready`
- `replicate`
- `retention`
- `share`
- `sql`
- `support`
- `tag`
- `tree`
- `undo`
- `update`
- `version`
- `watch`

Some of these could be added later if rusts3 implements the corresponding S3 or
MinIO-compatible APIs, but they are not advertised as supported now.

## Not Yet Implemented

The following flags and features are recognized by the CLI (so scripts using
them get a clear error, not a silent no-op) but are not implemented yet.
Passing them fails with a `<command> --<flag> is not implemented yet` error
and a non-zero exit code, rather than being silently ignored:

- `ls --rewind`, `ls --versions`, `ls --incomplete`, `ls --summarize`,
  `ls --zip`, `ls --storage-class`
- `cat --offset`, `cat --tail`
- `cp --older-than`, `cp --newer-than`
- `stat --recursive`
- `mb --with-lock`, `mb --with-versioning`
- `rm --versions`, `rm --version-id`
- `get --version-id`
- `mirror --watch`
- `alias import`

More broadly, object versioning is not supported end-to-end (rusts3's
version-related flags above are refused rather than honored), and
`--json`/`--quiet`/`--no-color` are accepted as global flags but currently
have no effect on output; `rs3` always prints its normal human-readable
output pending future output-format compatibility work.

## Multipart Uploads

MinIO `mc put` defaults `--part-size` to `16MiB`. `rs3` defaults it to `256MiB`.

`--part-size` sets both the size of each multipart part and the automatic
multipart threshold:

- files/objects at or below `--part-size` use a single `PutObject` /
  `CopyObject` (or a single-range `GetObject` for downloads)
- files/objects above `--part-size` use multipart upload (or parallel
  ranged `GetObject`/`UploadPartCopy` requests, as appropriate)
- `--disable-multipart` forces single-request transfer regardless of size
- `--part-size 0` is rejected as invalid

## Config

`rs3` reads and writes an `mc`-style config file:

- `$MC_CONFIG_FILE`, when set
- `$MC_CONFIG_DIR/config.json`, when set
- `~/.mc/config.json`, by default

Example:

```sh
rs3 alias set local http://127.0.0.1:9000 minioadmin minioadmin
rs3 mb local/test
rs3 put ./file.bin local/test/file.bin
rs3 ls local/test
```
