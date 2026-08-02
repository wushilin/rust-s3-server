# rs3

`rs3` is a high-performance Rust CLI for the **rusts3** server. Throughput and
concurrency control are the point: parallel multipart uploads, parallel ranged
downloads with atomic commit, server-side copies that never round-trip through
the client, and a single global concurrency budget you can actually reason
about.

For the operations it supports, `rs3` deliberately stays as close to MinIO's
`mc` as it reasonably can -- same command names, same flag grammar, same
message and JSON shapes -- so muscle memory and existing scripts mostly carry
over. That is a convenience goal, not a compatibility contract: `rs3` is **not
a drop-in `mc` replacement**, it implements only what works against rusts3, and
in several places it deliberately behaves *better* than `mc` (see
[Where rs3 diverges deliberately](#where-rs3-diverges-deliberately)).

Anything rusts3 doesn't implement -- object versioning, object lock, `--zip`
archive listing, `--rewind` point-in-time views, continuous `--watch` mirroring
-- is simply **absent from the CLI**. There are no flags that parse and then
fail; `--help` shows only what actually works.

## Commands

19 top-level commands, all `mc`-shaped:

| Command | Notes |
|---|---|
| `alias set\|list\|remove\|export` | `mc`-style config file, shared or separate (see [Config](#config)) |
| `ls` | `--recursive`, `--incomplete`, `--summarize`, `--storage-class`/`--sc` |
| `mb` | `--region`, `--ignore-existing` |
| `rb` | `--force` aborts incomplete multipart uploads, empties, then deletes; `--dangerous` required to remove every bucket on an alias when none is named |
| `put` | multipart, `--attr`, `--if-not-exists`, `--preserve` (Unix only) |
| `cp` | local<->S3 and S3<->S3 (incl. cross-alias), `--recursive`, `--older-than`/`--newer-than`, `--attr`, `--preserve` |
| `mv` | same machinery as `cp` plus a post-copy source delete; subdirectory-collision guard |
| `get` | single-object download; parallel ranged GETs above the multipart threshold |
| `cat` | `--offset`, `--tail`, `--part-number` |
| `head` | `-n`/`--lines`; gzip decoded transparently (bzip2 is a hard error) |
| `rm` | `--recursive` (requires `--force` or `--dry-run`), `--older-than`/`--newer-than`; batched `DeleteObjects` |
| `stat` | `--recursive`, `--verbose` |
| `mirror` | local<->S3 and S3<->S3 (incl. cross-alias); incremental (size/mtime diff), `--remove`, `--overwrite`, `--dry-run`/`--fake`, time filters, `--attr`, `--preserve` |
| `du` | `-r`/`--recursive`, `-d`/`--depth`; S3 aliases only |
| `tree` | `-f`/`--files`, `-d`/`--depth`; `--json` aliases to `ls --recursive --json`; S3 aliases only |
| `pipe` | stdin -> object; `--storage-class`, `--attr`, `--part-size` (528MiB default), `--concurrent` |
| `diff` | two-URL positional-only comparison; local and S3 sides |
| `find` | `--name`, `--path`, `--regex`, `--older-than`/`--newer-than`, `--larger`/`--smaller`, `--maxdepth`, `--ignore`, `--exec`, `--print`; S3 aliases only |
| `share download\|upload\|list` | presigned `GetObject` URLs, browser-POST upload commands (`--content-type`/`-T`), `--expire`/`-E` (Go duration grammar, clamped to `[1s, 7d]`) |

A few flag combinations are legitimate on their own but unsupported together,
and hard-error at runtime rather than silently no-opping: `--attr` on an
S3-to-S3 copy, `cp --preserve` for local-to-local copies, `--preserve` on
non-Unix platforms, and `head` on a bzip2-compressed object.

## Output shapes

`rs3` follows mc's `printMsg`/message-struct output contract rather than
inventing its own shape:

- **`--json`**: every command prints one JSON object per `printMsg` call
  (a message per object/bucket/event, plus a final summary where mc has
  one). When stdout is **not a TTY** (the common case: pipes, redirects,
  scripts, CI), each object is compacted to a single line -- true
  JSON-lines. When stdout **is a TTY**, the same JSON is pretty-printed
  (one-space indent) instead, matching mc's `globalJSONLine = !isTerminal()
  && json` rule exactly. Most message types use that one-space indent; a
  few mirror mc's per-type quirks (`ls --summarize`'s summary line uses no
  indent at all; `rb`'s success message is always compact, even on a TTY).
  Success messages are the domain object itself (with an embedded
  `"status":"success"`); errors always use the separate
  `{"status":"error","error":{...}}` envelope -- there is no single shared
  envelope shape between the two.
- **`--quiet`**: not a "suppress everything" flag. It turns off the
  animated transfer progress bar for `cp`/`mv`/`put`/`get`/`mirror`, and as
  a direct consequence of that, the per-object lines that would otherwise
  be consumed by the bar's caption are printed instead -- one message per
  object, plus a final `AccountStat` summary line, exactly as if `--json`
  had been passed (mc's own "`--quiet` is actually more verbose than
  default" behavior, `mc-research-output.md` §4). The bar itself is shown
  only when stdout is a TTY and neither `--quiet` nor `--json` is set.
- **Errors**: human-mode errors print `<prog>: <ERROR> <message>` to
  stderr, where `<prog>` is the invoking binary's own `argv[0]` basename
  (typically `rs3`, matching mc's own `ProgramName()`-from-`argv[0]`
  derivation rather than a hardcoded string, so scripts grepping for a
  literal `mc:` prefix will not match). `--json` mode instead writes the
  `{"status":"error","error":{"message":...,"cause":{...},"type":"fatal"|"error"}}`
  envelope to stdout.
- **Exit codes**: `0` on full success, `1` if any error occurred anywhere
  in the invocation -- including multi-target/recursive operations where
  some objects succeeded and others failed (mc's exit code is boolean per
  invocation, not a count or severity; rs3 matches that).

## Where rs3 diverges deliberately

These are not compatibility gaps -- they are places where the `mc` behavior
was examined and a different choice was made on purpose.

- **`-P` is a single global stream budget, not a per-layer multiplier.**
  One `-P` value caps *all* concurrent streams, cooperatively shared between
  whole objects and multipart segments: `-P 5` means 5 small files in flight,
  or 2 large files whose segments draw from the same 5 tokens. Every S3
  operation -- including byte-less control-plane calls -- consumes a token.
  The alternative (and rs3's own earlier design) lets `P` objects each run `P`
  segments, so the real ceiling is `P`\*`P` streams and the number you typed
  bounds nothing you care about.
- **A live worker-lane grid instead of one aggregate bar.** `mc` shows a
  single progress bar. rs3 shows a fixed grid of `-P` worker lanes above an
  overall `TOTAL x/y objects` bar, so you can see *what* each stream is doing,
  not just that bytes are moving. Details below.
- **Large downloads are parallel and atomically committed.** Transfers above
  the multipart threshold use parallel ranged `GetObject` requests writing into
  a `.rs3.part` sibling file, renamed into place only after every range
  succeeds -- so an interrupted download never leaves a truncated file at the
  destination path.
- **Part size defaults to 256MiB, not mc's 16MiB.** Fewer, larger parts on a
  fast link; `--part-size` overrides it. (`pipe`, whose total size is unknown
  up front, defaults to 528MiB, matching mc.)
- **`du -r -d 0` does something useful.** `mc` can't distinguish `-d 0` from
  "not set" (an `IsSet`-based special case) and prints nothing at all; rs3
  treats `0` as unset and falls through to normal default-depth behavior.
- **An empty source is not an error in `diff`.** An empty-but-existing S3
  prefix or local directory is fatal in real `mc` ("Object does not exist");
  rs3 treats it as an empty listing and succeeds, which is what a diff against
  a freshly created prefix should do.
- **`cp`/`mv` `--json` reports a real `totalSize`.** For a single
  non-recursive object, `CopyMessage.totalSize` carries the object's actual
  byte count; real `mc` always emits `0` there because it only tracks running
  totals for multi-object operations.
- **`head` gzip detection is case-insensitive**, where mc's equivalent check
  is case-sensitive and misses `Content-Type: GZIP`.
- **`mirror --dry-run --json` prints nothing rather than inventing a shape.**
  `mc` has no JSON contract for a dry-run plan, so rather than make one up,
  `--json --dry-run` emits zero stdout lines (still exits `0`) and the
  plain-text `PUT`/`DEL`/`Planned N put(s), M delete(s).` prose is gated on
  `!--json` so it can never corrupt a JSON-lines stream.

## Incidental differences from mc

Smaller places where output or edge-case handling doesn't match `mc`
byte-for-byte. Relevant only if you're diffing the two tools' output.

- **`AccountStat` summary is one line**, not mc's bordered 4-column table:
  `Total: X | Transferred: Y | Duration: Zs | Speed: W MB/s`.
- **`cp -r` `--json` lines are `MirrorMessage`-shaped**, not
  `CopyMessage`-shaped, because recursive `cp` runs on the `mirror` machinery.
  Relatedly, `cp` with mixed recursive and non-recursive sources in one
  invocation prints two `AccountStat` summaries instead of one.
- **`find`/`tree`/`du` are S3-alias-only** -- none has a local-filesystem code
  path.
- **`find --exec` omits mc's trailing `exit status N` line** when a child
  exits non-zero, and refuses the `{url}`/`{version}` template tokens outright
  (they'd require generating presigned URLs) rather than substituting them.
- **`stat`'s metadata is sparser**: only user metadata and `Content-Type`, not
  mc's full header set (expiration, replication status, checksum map, restore
  info, ...).
- **`ls`/`find` `--json` omit mc's `url`/`versionOrdinal` fields** --
  `ContentMessage` carries `status`/`type`/`lastModified`/`size`/`key`/`etag`/
  `storageClass` only.
- **`alias`/`config` don't speak the JSON message contract**: `--json` on the
  `alias` subcommands still prints prose/pretty-JSON rather than the
  message-per-line envelope the object commands use.
- **`ls --incomplete` boundary heuristic**: exact-object-vs-prefix resolution
  for in-progress multipart uploads reuses plain `ls`'s single-probe
  heuristic, so nested partial-key matches can diverge in edge cases.
- **`mirror --preserve` S3-to-local can re-download unchanged objects**: in
  some configurations a preserved mtime compares as older than the object's
  `LastModified`, so a subsequent run isn't the no-op it should be.
- **`share`'s history DB is rs3-specific** and `share list` hard-fails on a
  corrupt `{config_dir}/share/{uploads,downloads}.json`.
- **Cosmetic formatting nits**: local-time zone abbreviations come from
  chrono's `%Z` and won't always match Go's `time`; the byte humanizer rounds
  differently at unit boundaries (`10239B` -> `10.0KiB` here vs mc's `10KiB`);
  `stat` prints a bare `12B` without mc's space and column padding; `share`'s
  `--expire` clamp messages lack mc's trailing space; and `du`'s `Prefix` is
  rendered in aliased rather than bucket-relative form.
- **clap handles usage errors**: a missing required argument or unknown flag
  is caught by clap, which prints its own usage message and exits `2` --
  bypassing rs3's `<prog>: <ERROR>` machinery entirely.

## TTY progress display

Everything in this section is a TTY-only, stderr-only display detail.
`--json`, `--quiet`, and non-TTY output are completely unaffected, and
`--no-color` disables the grid.

**The worker-lane grid.** During `cp`/`mv`/`put`/`get`/`mirror` on a terminal,
rs3 renders a fixed gradle-style grid of worker lanes above an overall
`TOTAL x/y objects` bar. The grid always has `-P` lanes (**default 5**), capped
only by what the console can render (`min(-P, usable_rows)`, derived from the
detected terminal height; an undetectable height falls back to a 22-lane
classic-terminal cap). The lane count is fixed for the whole run, so the grid
never grows or shrinks mid-transfer.

A lane claims a bar the moment its stream holds a budget token and reverts to a
dim `> IDLE` row the moment it releases one -- so a lane visibly cycles
bar → `> IDLE` → bar as work recycles through it, and the number of *non-idle*
lanes at any moment is the live parallelism, never more than `-P`. When the
console is too short to show all `-P` lanes, the extra streams still run and
count toward the TOTAL bar, just without a lane of their own.

Bar labels are a verb plus a condensing path, e.g.
`Uploading asdf/a.img part 4/24` (long paths lose middle components, then
collapse to `…/name`, then trim from the left, to fit a fixed label column).
The byte column shares one unit across the pair, e.g. `123.3/256MiB`. Bytes
tick live as they cross the wire; server-side S3→S3 copy parts tick on
completion, since no bytes cross the client.

**Every S3 call is a worker task.** Control-plane calls with no bytes of their
own -- `CreateMultipartUpload`, `CompleteMultipartUpload`,
`AbortMultipartUpload`, `HeadObject`, `ListObjectsV2` (and its
`ListBuckets`/`ListMultipartUploads`/`ListParts` siblings),
`DeleteObject`/`DeleteObjects` -- are dispatched as worker tasks that each
consume one `-P` token for their duration, from the same budget the transfer
segments draw from. They render as a transient spinner line,
`<verb + path> <spinner> <ApiName>` (e.g. `Listing bucket1 ⣹ ListObjectsV2`),
that appears while the call is in flight and disappears the instant it
completes. So a multipart `cp` shows a `Creating ... ⣹ CreateMultipartUpload`
line before its part bars and a `Completing ... ⣹ CompleteMultipartUpload` line
after them, and a paginated `ls`/`rm -r`/`du`/`find` shows one such line per
`ListObjectsV2` page. These lines are painted on the next draw tick and erased
as soon as the call returns, so against a fast (e.g. loopback) server an op
that completes in under a tick may never be visible at all -- most often the
closing `CompleteMultipartUpload`, which also races the UI's own shutdown.

Standalone commands with no byte transfer of their own (`ls`, `rm`, `stat`,
`du`, `tree`, `find`, `diff`, `cat`, `head`, `mb`, `rb`) use a tasks-only UI:
transient spinner lines with **no** overall `TOTAL` bar, so their normal stdout
output (object listings, `stat` fields, file contents, ...) is never glued to a
persistent bar line.

## What's not supported

`mc` flags that rs3 does **not** declare at all. They're rejected by clap's own
usage-error parser (non-zero exit, no app-level "not implemented" text),
because no rs3 handler code exists to run:

- **Object versioning**: `ls --versions`/`--rewind`, `rm --versions`/
  `--version-id`, `get --version-id`, `head --rewind`, `tree --rewind`
- **Object lock**: `mb --with-lock`, `mb --with-versioning`
- **Zip archive listing**: `ls --zip`
- **Continuous watch modes**: `mirror --watch`, `find --watch` -- rs3 is a
  one-shot transfer tool
- **Client-side checksums**: `cp`/`put --md5`/`--checksum`
- **Other**: `find --metadata`/`--tags`, `alias import` (reads a config blob
  from stdin)

`tests/e2e_refuse.rs` exercises these end to end, alongside the handful of
flag *combinations* that hard-error at runtime.

MinIO-specific management command families are out of scope entirely, since
rusts3 doesn't implement the corresponding APIs: `admin`, `anonymous`/`policy`,
`batch`, `cors`, `encrypt`, `event`, `idp`, `ilm`, `legalhold`, `license`,
`od`, `ping`, `quota`, `ready`, `replicate`, `retention`, `sql`, `support`,
`tag`, `undo`, `update`, `version`, `watch`.

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

`--attr` (put/cp/mirror) attaches custom user metadata to uploaded objects;
`--if-not-exists` (put) uploads only if the target key doesn't already exist;
`--preserve`/`-a` (put/cp/mirror, Unix only) restores filesystem mode,
ownership, and timestamps on downloads. `cp --preserve` for local-to-local
copies and `--attr` on an S3-to-S3 copy are unsupported combinations and
hard-error.

## Multipart Uploads

MinIO `mc put` defaults `--part-size` to `16MiB`. `rs3` defaults it to `256MiB`
(`pipe`, whose total size is unknown up front, defaults to `528MiB`, matching
mc's own `pipe` default exactly).

`--part-size` sets both the size of each multipart part and the automatic
multipart threshold:

- files/objects at or below `--part-size` use a single `PutObject` /
  `CopyObject` (or a single-range `GetObject` for downloads)
- files/objects above `--part-size` use multipart upload (or parallel
  ranged `GetObject`/`UploadPartCopy` requests, as appropriate)
- `--disable-multipart` forces single-request transfer regardless of size
- `--part-size 0` is rejected as invalid

## Config

`rs3` reads and writes an `mc`-style config file. rs3-specific variables win
over their `mc`-compatible equivalents:

- `$RS3_CONFIG_FILE`, when set
- `$RS3_CONFIG_DIR/config.json`, when set
- `$MC_CONFIG_FILE`, when set
- `$MC_CONFIG_DIR/config.json`, when set
- `~/.mc/config.json`, by default

Aliases can also come from the environment without a config file:
`RS3_HOST_<ALIAS>` (preferred) or `MC_HOST_<ALIAS>`, in the form
`https://ACCESS_KEY:SECRET_KEY@host:port`. `AWS_S3_REGION` or `AWS_REGION`
sets the region for environment aliases.

Example:

```sh
rs3 alias set local http://127.0.0.1:9000 minioadmin minioadmin
rs3 mb local/test
rs3 put ./file.bin local/test/file.bin
rs3 ls local/test
```

## Credits

Where `rs3` follows `mc`, the behavior was ground-truthed against the MinIO
`mc` source and binary via two research documents produced for this effort:

- `../docs/superpowers/research/mc-research-output.md` -- the `printMsg`/
  message-struct output contract, JSON envelope shapes, exit codes,
  `--quiet`/`--no-color` semantics, and the progress bar.
- `../docs/superpowers/research/mc-research-semantics.md` -- per-command flag
  grammar and behavior (time filters, attr/preserve, versioning, `pipe`'s
  part-size default, `share`'s duration grammar and POST-policy format,
  `find`'s token substitution, and more).
