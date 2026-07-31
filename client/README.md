# rs3

`rs3` is a Rust CLI client targeting compatibility with MinIO's `mc` for the
subset of `mc`'s command surface that maps onto portable S3
bucket/object operations. The MinIO `mc` source is cloned into
`mc-reference/` for behavior reference, and two research documents
(`../docs/superpowers/research/mc-research-output.md`,
`../docs/superpowers/research/mc-research-semantics.md`) ground-truth every
message shape, exit code, and edge case cited below against the real `mc`
binary and source. MinIO-only admin/management command families are not
exposed, because rusts3 is not expected to implement those APIs.

## Commands

All 19 top-level `mc`-shaped commands are implemented. "Refused" flags parse
but fail fast with a clear error or a clap usage error rather than silently
no-opping -- see [Refused and unsupported flags](#refused-and-unsupported-flags).

| Command | Status | Notes |
|---|---|---|
| `alias set\|list\|remove\|export` | Implemented | `alias import` is refused (reads from stdin in mc; out of scope here) |
| `ls` | Implemented | `--recursive`, `--incomplete`, `--summarize`, `--storage-class`/`--sc`; `--rewind`/`--versions`/`--zip` refused |
| `mb` | Implemented | `--region`, `--ignore-existing`; `--with-lock`/`--with-versioning` refused |
| `rb` | Implemented | `--force` aborts incomplete multipart uploads, empties, then deletes; `--dangerous` required to remove every bucket on an alias when none is named |
| `put` | Implemented | multipart, `--attr`, `--if-not-exists`, `--preserve` (Unix only) |
| `cp` | Implemented | local<->S3 and S3<->S3 (incl. cross-alias), `--recursive`, `--older-than`/`--newer-than`, `--attr` (S3-to-S3 refused), `--preserve` (local-to-local refused) |
| `mv` | Implemented | same machinery as `cp` plus a post-copy source delete; subdirectory-collision guard |
| `get` | Implemented | `--version-id` refused |
| `cat` | Implemented | `--offset`, `--tail`, `--part-number` |
| `head` | Implemented | `-n`/`--lines`; gzip decoded transparently, bzip2 refused |
| `rm` | Implemented | `--recursive` (requires `--force` or `--dry-run`), `--older-than`/`--newer-than`; `--versions`/`--version-id` refused |
| `stat` | Implemented | `--recursive`, `--verbose` |
| `mirror` | Implemented | local<->S3 and S3<->S3 (incl. cross-alias); incremental (size/mtime diff), `--remove`, `--overwrite`, `--dry-run`/`--fake`, time filters, `--attr` (S3-to-S3 refused), `--preserve`; local-to-local refused; `--watch` refused |
| `du` | Implemented | `-r`/`--recursive`, `-d`/`--depth`; S3 aliases only, no local-filesystem path |
| `tree` | Implemented | `-f`/`--files`, `-d`/`--depth`; `--json` aliases to `ls --recursive --json` (matches mc); S3 aliases only |
| `pipe` | Implemented | stdin -> object; `--storage-class`, `--attr`, `--part-size` (528MiB default, matching mc), `--concurrent` |
| `diff` | Implemented | two-URL positional-only comparison; local and S3 sides |
| `find` | Implemented | mc's matchers (`--name`, `--path`, `--regex`, `--older-than`/`--newer-than`, `--larger`/`--smaller`, `--maxdepth`, `--ignore`), `--exec`, `--print`; S3 aliases only; `--watch`/`--metadata`/`--tags` never declared (clap-rejected) |
| `share download\|upload\|list` | Implemented | presigned `GetObject` URLs, browser-POST upload commands (`--content-type`/`-T`), `--expire`/`-E` (Go duration grammar, clamped to `[1s, 7d]`) |

## Output compatibility with mc

rs3 replicates mc's `printMsg`/message-struct output contract
(`mc-research-output.md` §1-§5) rather than inventing its own shape:

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
  derivation rather than a hardcoded string -- see
  [Known divergences](#known-divergences-from-mc) for what that means for
  scripts expecting literal `mc:` text). `--json` mode instead writes the
  `{"status":"error","error":{"message":...,"cause":{...},"type":"fatal"|"error"}}`
  envelope to stdout.
- **Exit codes**: `0` on full success, `1` if any error occurred anywhere
  in the invocation -- including multi-target/recursive operations where
  some objects succeeded and others failed (mc's exit code is boolean per
  invocation, not a count or severity; rs3 matches that).

## Known divergences from mc

Deliberate or accepted-as-minor differences from real `mc`, found and
disclosed during ground-truth verification against `mc-research-output.md`
and `mc-research-semantics.md` (and, for several, against the real `mc`
binary itself):

- **`AccountStat` human line vs mc's table**: rs3 renders the final
  transfer summary as one line (`Total: X | Transferred: Y | Duration: Zs
  | Speed: W MB/s`); mc renders a bordered 4-column table.
- **Date zone rendering**: rs3's local-time formatting (`stat`, `ls`, ...)
  uses chrono's `%Z`, which doesn't always resolve to the same zone
  abbreviation Go's `time` package would; expect an offset or a different
  abbreviation token in some zones/platforms.
- **`head` gzip detection is case-insensitive**: rs3 matches
  `Content-Type` containing `gzip` case-insensitively before decompressing
  a `head` stream; mc's equivalent check is case-sensitive.
- **`cp -r`/`cp --recursive` JSON shape**: delegates internally to the same
  machinery as `mirror`, so its `--json` per-object lines are
  `MirrorMessage`-shaped, not `CopyMessage`-shaped (mc emits `copyMessage`
  for `cp -r` and reserves `mirrorMessage` for `mirror`).
- **`cp` with mixed recursive and non-recursive sources** in one invocation
  prints two `AccountStat` summaries instead of mc's one.
- **`mirror --dry-run --json` prints nothing**: mc has no documented JSON
  contract for a dry-run plan. Rather than invent one, rs3's `--json
  --dry-run` emits zero stdout lines (still exits `0`); the plain-text
  `PUT`/`DEL`/`Planned N put(s), M delete(s).` prose is gated on `!--json`
  so it never corrupts a JSON-lines stream.
- **`du -r -d 0`**: mc can't distinguish `-d 0` from "not set" (an
  `IsSet`-based special case) and prints nothing for `-r -d 0`; rs3 treats
  `0` as unset and falls through to its normal default-depth behavior
  instead. `du`'s `Prefix` field is also rendered in aliased form, not
  mc's bucket-relative form.
- **Empty-source `diff` tolerance**: an empty-but-existing S3 prefix or
  local directory used as a `diff` source is fatal in real mc ("Object
  does not exist"); rs3 treats it as an empty listing and succeeds.
- **`find --exec` omits mc's `exit status N` line**: when an exec child
  exits non-zero, mc prints an extra status line to stdout; rs3 doesn't.
- **`find`'s `{url}`/`{"url"}`/`{version}`/`{"version"}` tokens are
  refused outright** in `--exec`/`--print` templates (they'd require
  generating presigned URLs, out of scope here) rather than substituted;
  mc's `--exec` supports them.
- **`share`'s local history DB hard-fails on corrupt JSON**: rs3's
  `{config_dir}/share/{uploads,downloads}.json` file is an rs3-specific
  format ([SEM] §10 explicitly declines to specify mc's own on-disk
  format), and `share list` errors out if it's unparseable; mc's own
  behavior here is unresearched.
- **`share`'s `--expire` clamp error text lacks mc's trailing space**: rs3
  prints `Expiry cannot be lesser than 1 second.` / `Expiry cannot be
  larger than 7 days.`; ground-truth mc carries a trailing space on these
  -- a byte-exact-diff nit only, not a behavior difference.
- **`ls --incomplete` boundary heuristic**: exact-object-vs-prefix
  resolution for in-progress multipart uploads reuses the same
  single-probe heuristic as plain `ls` rather than a dedicated
  incomplete-upload probe; nested partial-key matches can diverge from mc
  in edge cases.
- **`mirror --preserve` S3-to-local re-download quirk**: in some
  configurations a preserved mtime compares as older than the object's
  `LastModified`, so a subsequent `mirror --preserve` run re-downloads
  objects that haven't actually changed, instead of being a no-op.
- **`find`/`tree`/`du` are S3-alias-only**: unlike mc, none of the three
  have a local-filesystem code path in rs3.
- **Object versioning, checksums, and zipped downloads are absent**:
  `ls --versions`/`--rewind`/`--zip`, `rm --versions`/`--version-id`, `get
  --version-id`, `mb --with-lock`/`--with-versioning`, and
  `cp`/`put --md5`/`--checksum` are either hard-refused at runtime or
  never declared on the relevant flag struct at all -- see the next
  section for exactly which.
- **`stat`'s metadata is sparser than mc's**: only user metadata and
  `Content-Type` are surfaced, not mc's full header set (expiration,
  replication status, checksum map, restore info, etc.).
- **`humanize_ibytes` rounding boundary**: rs3's byte-humanizer
  round-then-compares slightly differently from Go's `go-humanize` at unit
  boundaries (e.g. `10239B` renders `"10.0KiB"` here vs mc's `"10KiB"`).
- **clap usage errors bypass the `<prog>: <ERROR>` prefix and exit `1`**:
  a missing required argument or an unknown flag is caught by clap itself,
  which prints its own usage message and exits with status `2` -- not
  rs3's own error machinery. This is clap's default behavior, left as-is.
- **`alias`/`config` subcommands don't speak the mc JSON contract yet**:
  `--json` on `alias list`/`alias set`/`alias remove`/`alias export` (and
  `config`) still prints the same prose/pretty-JSON it always has, not
  mc's message-per-line JSON envelope -- `alias` was never in tier-2
  scope. Tier-3 item.
- **`stat`'s human size omits mc's space and column padding**: for `stat`
  specifically (not `ls`, which strips the space itself via
  `strings.Join(strings.Fields(...), "")`), mc's raw `humanize.IBytes`
  renders sub-1024 sizes as `12 B` (digits-space-unit) and then
  left-justifies/pads that whole string to a fixed column width with a
  trailing space (`%-6s `); rs3's `stat` prints the bare `12B`, no space,
  no column padding. Byte-exact-diff nit only.
- **`cp`/`mv` `--json` `totalSize` is real, not mc's `0`**: for a single
  (non-recursive) object, rs3's `CopyMessage.totalSize` carries the
  object's actual byte count; real mc always emits `0` there since it
  only tracks running totals for multi-object operations.
- **`ls`/`find` `--json` lack mc's `url`/`versionOrdinal` fields**:
  `ContentMessage`'s JSON has `status`/`type`/`lastModified`/`size`/`key`/
  `etag`/`storageClass` but no presigned `url` or version-ordinal field --
  out of scope alongside the versioning/zip absence noted above.
- **TTY progress display (deliberate):** during `cp`/`mv`/`put`/`get`/`mirror`
  on a terminal, rs3 shows up to 10 per-unit progress bars (one per in-flight
  multipart segment or small file) above an overall `TOTAL x/y objects` bar,
  where mc shows a single aggregate bar. `-P` is a *global concurrent-stream
  budget*, cooperatively shared between whole objects and multipart segments
  (e.g. `-P 5` covers 5 small files in flight, or 2 large files whose
  segments draw from the same 5 tokens) -- **default 5**. A per-unit bar is
  visible exactly while its stream holds a budget token, so the number of
  bars on screen at any moment *is* the live parallelism, never more than
  `-P`; this is a deliberate reduction from the old per-layer worst case of
  up to `P` objects each running `P` segments (`P`\*`P` concurrent streams).
  Bar labels are a verb plus the condensing path, e.g.
  `Uploading asdf/a.img part 4/24` (long paths lose middle components, then
  collapse to `…/name`, then trim from the left, to fit a fixed label
  column), and the byte column shares one unit across the pair, e.g.
  `123.3/256MiB`. Bytes tick live as they cross the wire; server-side S3→S3
  copy parts tick on completion since no bytes cross the client. Non-TTY,
  `--json`, and `--quiet` output is unaffected. `--no-color` disables the
  bars.

## Refused and unsupported flags

rs3 has two distinct refusal styles for `mc` flags it doesn't support,
covering the versioning/checksum/zip surface named above plus a few
command-specific ones:

1. **Runtime hard-errors**: flags rs3 *does* declare on the relevant
   `*Args` struct (so they show up in `--help` and parse normally) but
   whose handler immediately fails with `"<flag> is not implemented yet"`
   (a non-zero exit and that exact substring on stderr) before touching
   the network:
   - `ls --rewind`, `ls --versions`, `ls --zip`
   - `mb --with-lock`, `mb --with-versioning`
   - `rm --versions`, `rm --version-id`
   - `get --version-id`
   - `mirror --watch`
   - `alias import`
2. **Clap-level rejections**: flags rs3 never declared on the struct at
   all, because they're either MinIO-specific or out of scope for this
   tier. clap's own usage-error parser refuses them -- exit non-zero, no
   app-level "not implemented" text, because no rs3 handler code ever
   runs:
   - `head --rewind`/`--version-id`/`--zip`
   - `find --watch`/`--metadata`/`--tags`
   - `tree --rewind`
   - `cp`/`put --md5`/`--checksum`

`tests/e2e_refuse.rs` exercises both styles end to end.

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
ownership, and timestamps on downloads (S3-to-local-to-local `cp --preserve`
and S3-to-S3 `--attr` are refused -- see Known divergences).

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

## Intentionally out of scope

MinIO-specific or non-portable `mc` command families are not part of the
`rs3` compatibility target, since rusts3 is not expected to implement the
corresponding MinIO-only management APIs:

- `admin`
- `anonymous` / `policy`
- `batch`
- `cors`
- `encrypt`
- `event`
- `idp`
- `ilm`
- `legalhold`
- `license`
- `od`
- `ping`
- `quota`
- `ready`
- `replicate`
- `retention`
- `sql`
- `support`
- `tag`
- `undo`
- `update`
- `version`
- `watch`

## Credits

`rs3`'s `mc`-compatibility behavior is ground-truthed against the vendored
MinIO `mc` source (`mc-reference/`) and two research documents produced for
this effort:

- `../docs/superpowers/research/mc-research-output.md` -- the `printMsg`/
  message-struct output contract, JSON envelope shapes, exit codes,
  `--quiet`/`--no-color` semantics, and the progress bar.
- `../docs/superpowers/research/mc-research-semantics.md` -- per-command flag
  grammar and behavior (time filters, attr/preserve, versioning, `pipe`'s
  part-size default, `share`'s duration grammar and POST-policy format,
  `find`'s token substitution, and more).
