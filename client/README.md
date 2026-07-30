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
- `rb`
- `put`
- `cp` for local-to-S3 and S3-to-local
- `get`
- `cat`
- `rm`
- `stat`
- `mirror` for local directory -> S3 prefix, S3 prefix -> local directory,
  and S3-compatible prefix -> S3-compatible prefix

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

## Multipart Uploads

MinIO `mc put` defaults `--part-size` to `16MiB`. `rs3` defaults it to `256MiB`.

`rs3` also uses `256MiB` as the automatic multipart threshold:

- files at or below `256MiB` use `PutObject`
- files above `256MiB` use multipart upload
- `--disable-multipart` forces single-object upload

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
