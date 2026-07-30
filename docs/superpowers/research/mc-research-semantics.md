# MinIO `mc` semantics reference (for rs3 tier-2 compatibility work)

Source tree: `/home/code/workspace/mc` (read-only reference). All paths below are
relative to that root unless stated otherwise. Line numbers reflect the checked-out
revision at research time and may drift slightly with upstream changes.

---

## 1. Time filters: `--older-than` / `--newer-than`

Grammar is a **custom extension of Go's `time.ParseDuration`**, implemented from
scratch in `cmd/duration.go` (`ParseDuration`, not `time.ParseDuration`). It supports
the same `[-+]?([0-9]*(\.[0-9]*)?[a-z]+)+` grammar as Go duration strings, plus extra
units:

```go
// cmd/duration.go
var unitMap = map[string]int64{
	"ns": int64(Nanosecond),
	"us": int64(Microsecond),
	"µs": int64(Microsecond), // U+00B5 = micro symbol
	"μs": int64(Microsecond), // U+03BC = Greek letter mu
	"ms": int64(Millisecond),
	"s":  int64(Second),
	"m":  int64(Minute),
	"h":  int64(Hour),
	"d":  int64(Day),
	"w":  int64(Week),
	"y":  int64(Year), // Approximation
}
```
`Day = Hour*24`, `Week = Day*7`, `Year = Day*365` (approximation; no leap handling).
No `month` unit exists despite `Month` being defined as a constant in the file — it's
unused/unexposed in `unitMap`. Fractional values are allowed (`1.5d`). A bare `"0"`
means zero duration. Empty string is an error. Unknown unit or malformed number is an
error with message `"unknown unit %s in duration %s"` / `"invalid duration %s"`.

Example from `find`'s help text: `--older-than, --newer-than flags accept the string
for days, hours and minutes i.e. 1d2h30m states 1 day, 2 hours and 30 minutes.`
(`cmd/find-main.go:122`)

**Fallback to absolute dates**: if `ParseDuration` fails, `isOlder`/`isNewer` retry
parsing the same string against `rewindSupportedFormat` (defined in `cmd/ls-main.go:118`):
```go
var rewindSupportedFormat = []string{
	"2006.01.02",
	"2006.01.02T15:04",
	"2006.01.02T15:04:05",
	time.RFC3339,
	printDate, // "2006-01-02 15:04:05 MST"
}
```
So `--older-than 2020.01.01` is valid too (absolute wall-clock date, parsed in the
process's local timezone via `time.Parse`, NOT `time.ParseInLocation` — unlike
`--rewind`, see `isOlder`/`isNewer` in `cmd/utils.go:172-210`).

**Comparison semantics** (`cmd/utils.go:171-210`), compared against `content.Time`
(object's `LastModified`, or for `cp`/`mv`/`rm`/`find` the listed object's mtime):

```go
// isOlder returns true if the passed object is older than olderRef
func isOlder(ti time.Time, olderRef string) bool {
	...
	objectAge := time.Since(ti)
	olderThan, e := ParseDuration(olderRef)   // or fallback to absolute-date parse
	...
	return objectAge < time.Duration(olderThan)
}

// isNewer returns true if the passed object is newer than newerRef
func isNewer(ti time.Time, newerRef string) bool {
	...
	objectAge := time.Since(ti)
	newerThan, e := ParseDuration(newerRef)
	...
	return objectAge >= time.Duration(newerThan)
}
```
Note the naming is inverted from what you'd expect: `isOlder(ti, "7d")` returns true
if `ti`'s age is **less than** 7 days (i.e., "is this object [younger than the
'older-than' bound], meaning does NOT qualify as old"). Callers use it as a *skip*
predicate: e.g. in `cp-url.go:370` — `if o.olderThan != "" &&
isOlder(cpURLs.SourceContent.Time, o.olderThan) { skip }` — meaning objects are
**included** only when `objectAge >= olderThan` (i.e. object's mtime really is older
than the cutoff). Symmetric for newer: object is **included** only when
`objectAge < newerThan` (object really is newer than cutoff), because `isNewer`
returns true (→ skip) when `objectAge >= newerThan`.

For `find`'s `matchFind` (`cmd/find.go:418-423`) the polarity is inverted again into
positive-match form:
```go
if match && ctx.olderThan != "" {
	match = !isOlder(fileContent.Time, ctx.olderThan)
}
if match && ctx.newerThan != "" {
	match = !isNewer(fileContent.Time, ctx.newerThan)
}
```
Net effect for `find --older-than 7d`: object matches iff `objectAge >= 7d` (its
LastModified truly is 7+ days old). `find --newer-than 7d`: object matches iff
`objectAge < 7d`.

Consumers: `cp` (`cmd/cp-main.go:333-334`), `mv` (same, via `mv-main.go` reusing
`doCopySession`), `mirror` (`cmd/mirror-main.go:832-838`, applied to
`SourceContent.Time`), `rm` (`cmd/rm-main.go`, applied to `content.Time`/`modTime`),
`find` (`cmd/find.go:418-423`).

`replicate-reset-start.go` uses a *different*, MinIO-admin-specific parsing (requires
unit to contain `d`, `w`, or `y`; converts to Go `time.Duration` days count) — this is
MinIO-admin-only and out of scope for rs3 (skip family).

---

## 2. `cp` / `mirror` metadata flags

### `--attr` (custom metadata)

Flag: `cli.StringFlag{Name: "attr", Usage: "add custom metadata for the object"}`
(`cmd/cp-main.go:64-67`, also on `mv`, `pipe`).

Parser: `getMetaDataEntry` in `cmd/cp-main_contrib.go:31-138`. It is a **hand-rolled
character-by-character parser**, not a naive `;`/`=` split — it supports single- and
double-quoted values (quotes are stripped, not preserved) so values can contain `;`
or `=` when quoted:

```go
// validate the passed metadataString and populate the map
func getMetaDataEntry(metadataString string) (map[string]string, *probe.Error) {
	metaDataMap := make(map[string]string)
	...
	// State machine: KEY/VALUE token, NORMAL/QSTRING('...')/DQSTRING("...") parser state.
	// '=' switches from KEY to VALUE token (only the first '=' per segment; extra
	//     '=' inside VALUE state are written literally).
	// ';' (outside quotes) commits current key=value pair and resets to KEY state;
	//     ';' with empty/absent KEY is an error.
	// EOF while still in KEY state, or while inside an open quote, is an error
	//     (ErrInvalidMetadata: "specified metadata should be of form
	//      key1=value1;key2=value2;... and so on").
	// Keys are canonicalized via http.CanonicalHeaderKey(key) when stored.
}
```
So format is `key1=value1;key2=value2;...`, keys become canonical HTTP header form
(`Cache-Control`, `X-Amz-Meta-Foo`, etc. — note: user must pass full header names,
`--attr` does NOT auto-prefix with `X-Amz-Meta-`). Example from help text:
`--attr "Cache-Control=max-age=90000,min-fresh=9000;key1=value1;key2=value2"`.

`ErrInvalidMetadata` message: `"specified metadata should be of form
key1=value1;key2=value2;... and so on"` (`cmd/cp-main.go:116`).

### `--preserve` / `-a` (filesystem attribute preservation)

Flag: `cli.BoolFlag{Name: "preserve, a", Usage: "preserve filesystem attributes
(mode, ownership, timestamps)"}`.

On the **local→remote GET side** (`client-fs.go:598-643`, `Get()`), when
`opts.Preserve` is true, mc calls `disk.GetFileSystemAttrs(path)` (OS-specific,
`pkg/disk/stat_linux.go:33-66` for Linux) and stores the encoded string under the
metadata key:

```go
const metadataKey      = "X-Amz-Meta-Mc-Attrs"      // client-fs.go:62
const metadataKeyS3Cmd = "X-Amz-Meta-S3cmd-Attrs"    // client-fs.go:63 (read-compat only)
```

Encoded value format (Linux, `pkg/disk/stat_linux.go`), a `/`-separated list of
`key:value` pairs, sorted alphabetically by field name as written:
```go
// atime:<sec>#<nsec>/gid:<gid>/gname:<name>/mode:<st_mode int>/mtime:<sec>#<nsec>/uid:<uid>/uname:<name>
fileAttr.WriteString("atime:")
fileAttr.WriteString(strconv.FormatInt(int64(st.Atim.Sec), 10) + "#" + strconv.FormatInt(int64(st.Atim.Nsec), 10))
fileAttr.WriteString("/gid:")  ... "/gname:" ... "/mode:" ... "/mtime:" ... "/uid:" ... "/uname:"
```
(`gname`/`uname` are omitted if `user.LookupGroupId`/`LookupId` fail, e.g. no
`/etc/passwd` entry.) `mode` is the raw `st.Mode` (includes file-type bits, not just
permission bits).

On the **remote→local PUT side** (`client-fs.go:preserveAttributes`, called from
`put()` around lines 325/486 when `opts.metadata[metadataKey]` exists AND
`opts.isPreserve`):
```go
func preserveAttributes(fd *os.File, attr map[string]string) *probe.Error {
	if val, ok := attr["mode"]; ok {
		mode, e := strconv.ParseUint(val, 0, 32)
		if e == nil { fd.Chmod(os.FileMode(mode)) }
	}
	// uid/gid parsed (defaulting to -1 = "don't change" on parse failure)
	fd.Chown(uid, gid)
	return nil
}
```
`atime`/`mtime` are parsed separately by `parseAtimeMtime` (`cmd/utils.go:245-278`,
format `"<unix-sec>#<unix-nsec>"`, `#nsec` part optional) and applied via
`os.Chtimes(objectPath, atime, mtime)`.

Attribute-string parser: `parseAttribute` (`cmd/utils.go:281+`) splits on `/` then on
first `:` to build the map; a value-less segment (`len(attrVal)==1`) is stored with
empty string.

**On S3-to-S3 copy with `--preserve`**, mc additionally fetches "all metadata"
(`getAllMetadata`) from the source object and merges it into the target's metadata
map before issuing the copy (`common-methods.go:356-364` and `393-403`) — this
preserves user metadata/tags across a same-account server-side copy, in addition to
(or instead of) the local-FS xattr behavior above.

**Windows**: `--preserve` is hard-rejected at syntax-check time:
```go
// cp-url-syntax.go:68-71
if cliCtx.Bool("preserve") && runtime.GOOS == "windows" {
	fatalIf(errInvalidArgument().Trace(), "Permissions are not preserved on windows platform.")
}
```

### `--md5` (hidden flag)

`cli.BoolFlag{Name: "md5", Hidden: true, Usage: "force all upload(s) to calculate
md5sum checksum"}` (`cmd/cp-main.go:76-80`). Parsed via `parseChecksum` (`flags.go:135`):
`useMD5 = ctx.Bool("md5")`; forcing `checksum=="MD5"` also sets `useMD5=true`.
Combining `--md5` with `--checksum <algo>` (any non-MD5 algo) is a fatal error
(`"cannot combine MD5 with checksum"`). When `useMD5` is set, `PutOptions.SendContentMd5`
is effectively forced true downstream, causing the S3 PUT to compute/send a
Content-MD5 header rather than relying on trailing checksum headers. Object-locked
targets force `md5=true, checksum=ChecksumNone` automatically regardless of the flag
(`cp-main.go:338-341`, "Content-MD5 header is required for any request to upload an
object with a retention period configured using Amazon S3 Object Lock").

### `put --if-not-exists` (hidden)

`cli.BoolFlag{Name: "if-not-exists", Hidden: true, Usage: "upload only if object does
not exist"}` (`cmd/put-main.go:44-48`). Implementation (`cmd/client-s3.go:1123-1126`):
```go
if putOpts.ifNotExists {
	// Only supported in newer MinIO releases.
	opts.SetMatchETagExcept("*")
}
```
i.e. sends conditional header **`If-None-Match: *`** on the PutObject call (via
minio-go's `PutObjectOptions.SetMatchETagExcept`). Comment explicitly notes this is a
MinIO-server extension ("only supported in newer MinIO releases") — AWS S3 also now
supports `If-None-Match: *` for conditional writes, but rs3 should treat this as
best-effort/optional depending on target compatibility. Only exposed on `put`, not on
`cp`/`mv` at the CLI flag level even though `doCopyOpts.ifNotExists` field exists (cp
never sets it from a flag — dead/internal-only wiring for `cp`).

---

## 3. `mv` semantics

`cmd/mv-main.go` — `mv` is **not a separate code path**; `mainMove` calls the exact
same `doCopySession(ctx, cancelMove, cliCtx, encKeyDB, /*isMvCmd=*/true)` used by `cp`
(`cmd/cp-main.go`). All `cp` flags apply except `--attr`'s validation, retention flags
(`rmFlag`, `rdFlag`, `lhFlag`), `--zip`, `--max-workers`, `--rewind`, `--version-id`
are **not** present on `mv` (`mvFlags` in `cmd/mv-main.go:32-68` is a strict subset of
`cpFlags`).

**Copy-then-remove, per object, decoupled from the copy result stream**:
In `doCopy` (`cmd/cp-main.go:239-282`):
```go
urls := uploadSourceToTargetURL(ctx, uploadSourceToTargetURLOpts{...})
if copyOpts.isMvCmd && urls.Error == nil {
	rmManager.add(ctx, sourceAlias, sourceURL.String())
}
return urls
```
So the delete is only enqueued if the copy of *that specific object* succeeded
(`urls.Error == nil`). Objects whose copy failed are simply left in place at the
source — **no rollback of the copy, no retry of the delete**; the failure is reported
normally through the same error-handling path as `cp` (printed via `errorIf`, sets
`globalErrorExitStatus`, and if not `isErrIgnored`, marks `errSeen`).

**Deletes are batched per-source-alias and asynchronous** via a `removeManager`
(`cmd/mv-main.go:146-210`): the first object for a given alias lazily creates one
`client.Remove()` streaming call (recursive=false, force=false, etc. — `false,false,
false,false` positional args) and a background goroutine drains
`resultCh` logging any removal errors via `errorIf` (**removal errors do not fail the
overall `mv` exit status** beyond the log message — they're fire-and-forget). All
per-object deletes for one alias funnel through one shared content channel. After the
whole copy session completes, `mainMove` prints "Waiting for move operations to
complete" and calls `rmManager.close()`, which closes all content channels and
`Wait()`s the internal `WaitGroup` for the drain goroutines — this is effectively
mv's synchronization point, ensuring the process doesn't exit before pending deletes
finish.

**Refuses moving a directory without `-r`/`--recursive`**: enforced in the shared
`prepareCopyURLs` (`cmd/cp-url.go:216-220`):
```go
if cc.sourceContent.Type.IsDir() {
	if !o.isRecursive {
		return returnErrorAndCloseChannel(errRequiresRecursive(cc.sourceURL).Trace(cc.sourceURL))
	}
	if isURLContains(cc.sourceURL, cc.targetURL, string(c.GetURL().Separator)) {
		return returnErrorAndCloseChannel(errCopyIntoSelf(cc.sourceURL).Trace(cc.targetURL))
	}
}
```
(same guard used by `cp`; `printCopyURLsError` special-cases the "is a folder."
substring to print "Folder cannot be copied. Please use `...` suffix." — the actual
mc idiom for recursive globbing is a `...` suffix, not `--recursive`, on some
call sites, but the flag also works.)

**Same-source/target-prefix guard**: `mainMove` checks (only when exactly 2 args)
`isURLPrefix(srcURL, dstURL)` and fatals with "The source %v and destination %v
cannot be subdirectories of each other" (`cmd/mv-main.go:220-228`) — this is a
`mv`-only up-front check not present in `cp` (cp only guards
copy-into-self at the per-object listing stage above).

---

## 4. `head`

Flags (`cmd/head-main.go:36-54`): `-n, --lines` (`Int64Flag`, **default `10`**),
`--rewind`, `--version-id/--vid`, `--zip`.

`-n`/`--lines` default is baked into the flag (`Value: 10`) — but note `headOut`
*also* has its own defensive default: `if nlines < 0 { nlines = 10 }` (only triggers
for an explicit negative value, since the cli flag default already supplies 10 when
unset).

**Read strategy — full GET, not a ranged request**: `headURL` calls
`getSourceStreamMetadataFromURL` (no offset/range options passed), i.e. it opens a
**full, un-ranged GET stream** and then reads only the first N lines off that stream
client-side via a `bufio.Reader.ReadLine()` loop, stopping early once `nlines`
lines are consumed (the underlying HTTP body is left un-drained/closed early — no
special early-stop optimization on the wire). Compression is auto-detected and
decoded client-side from `Content-Type` header substring match: `strings.Contains(ctype,
"gzip")` → `gzip.NewReader`, `strings.Contains(ctype, "bzip")` → `bzip2.NewReader`
(one-way; bzip2 reader in Go stdlib has no `Close`, hence `io.NopCloser` wrap).

Output framing: each line from `br.ReadLine()` (strips the line terminator) is
written followed by an explicit `"\n"` — so line-ending normalization always emits
Unix `\n` regardless of source CRLF. If stdout is a terminal, output is filtered
through `prettyStdout` which replaces any non-printable/non-space rune with the
literal 2-char sequence `^?` to avoid terminal corruption (shared with `cat`).
`head` on stdin (`-`, or no positional args at all) reads directly with no
version/zip/rewind support and always uses `ctx.Int64("lines")`.

---

## 5. `du`

Flags (`cmd/du-main.go:37-56`): `--depth, -d` (`IntFlag`, **no explicit default →
zero value 0**), `--recursive, -r` (`BoolFlag`), `--rewind`, `--versions`.

**Default-depth resolution logic** (`mainDu`, `cmd/du-main.go:224-234`):
```go
depth := cliCtx.Int("depth")
if depth == 0 {
	if cliCtx.Bool("recursive") {
		if !cliCtx.IsSet("depth") {
			depth = -1        // recursive with no explicit depth => unlimited
		}
	} else {
		depth = 1              // plain `du` (no -r, no -d) => depth 1 (immediate children only)
	}
}
```
So: `mc du` alone ⇒ depth 1 (summarize only the first level, one aggregated line per
immediate subfolder... actually see below, it's recursive-into-subfolder w/ separate
print per subfolder at level 1); `mc du -r` ⇒ depth -1 (fully unlimited, single
recursive listing, single total line for the whole tree); `mc du -d N` ⇒ that depth
regardless of `-r`; `mc du -d 0` is rejected implicitly (0 is indistinguishable from
"unset" due to the check above — effectively `-d 0` behaves like default).

**Recursion strategy** (`du()`, `cmd/du-main.go:122-207`): `recursive :=
depth == 1`. When `depth==1`, does one flat recursive listing
(`ListOptions{Recursive: true, ...}`) and sums everything into one number (this is
the terminal/leaf case — "no disk usage details below this level, just do a recursive
listing"). When `depth != 1` (i.e. `-1` unlimited, or `>1`), it does a **non-recursive**
listing (`Recursive: recursive` = false) and for every directory entry it recurses
into `du()` again with `depth-1` (clamped to not go below 0 once already 0... actually
depth only decrements while `>0`; `-1` stays `-1` forever ⇒ true unlimited descent),
**printing one `duMessage` line per prefix visited** (except depth==0, which never
prints — see below). Each subfolder therefore gets its own aggregated summary line —
`du` prints one row *per directory level traversed*, not just a single grand total,
except when depth resolves to exactly 1 (single flat total for the whole target).

Print condition: `if depth != 0 { printMsg(duMessage{...}) }` — i.e. once depth
counts down to exactly `0` (only reachable when caller passed a positive `--depth N`
and recursion bottoms out), that final level is aggregated into the parent's sum
silently (size/objects returned up via the `size, objects` return values) but **not
printed as its own line** — it still contributes to the parent's total.

`duMessage` fields (`Prefix`, `Size`, `Objects`, `Status`, `IsVersions`); `String()`
prints `%s\t%s\t%s` = colorized human size (via `humanize.IBytes`, whitespace
stripped so no thousands-separator spaces), colorized `"%d object[s]"` /
`"%d version[s]"` (pluralized unless exactly 1; label switches to "version(s)" when
`--versions` is set), colorized prefix path. Skips delete markers and directories
themselves in the byte/object count (`if !content.IsDeleteMarker &&
!content.Type.IsDir()`), and glacier/insufficient-permission/not-found/broken-symlink
errors are silently skipped (except `PathInsufficientPermission`, which is logged via
`errorIf` but doesn't abort).

`--rewind` on `du` accepts the same grammar as `ls`'s `--rewind` (`parseRewindFlag`,
absolute date formats or relative duration, see §1 fallback list).

---

## 6. `tree`

Flags (`cmd/tree-main.go:64-78`): `--files, -f` (bool, off by default — directories
only unless set), `--depth, -d` (`IntFlag`, **default `-1`** = unlimited), `--rewind`.

**Depth validation** (`parseTreeSyntax`, lines 116-138):
```go
if depth < -1 || cliCtx.Int("depth") == 0 {
	fatalIf(..., "please set a proper depth, for example: '--depth 1' to limit the
	  tree output, default (-1) output displays everything")
}
```
So `--depth 0` and any negative value other than exactly `-1` are hard errors (`-1` is
the sentinel for "unlimited", not a usable numeric depth value otherwise).

**Drawing rules** (`doTree`, lines 141-265): constants
```go
treeEntry     = "├─ "
treeLastEntry = "└─ "
treeNext      = "│"
treeLevel     = "  "
```
Recursion is depth-first per directory: it lists the current prefix
non-recursively (`ListOptions{Recursive:false, ...}`), buffers a `prev` pointer, and
for every entry after the first it calls `show(false)` (retroactively renders
`prev` as a non-last sibling) before advancing; the true final entry of each
directory listing is rendered via `show(true)` (last-sibling glyph `└─`) after the
loop exits. Branch-string bookkeeping incrementally rewrites the previous level's
connector: closes a level with `treeLastEntry`/`treeNext` replaced by two spaces or
`│ ` depending on whether that level was itself the last child, so continuation
columns for still-open ancestor branches show `│  ` while closed ones show `   `.

At `level == 1` the very first call also prints one line for the root target itself
(bucket/alias root), with `IsDir: true` unconditionally, before printing any children.
`--files` gates plain-file entries: `if !includeFiles && !content.Type.IsDir() {
continue }` — directories are always shown regardless of `--files`.

`tree` recurses only `if depth == -1 || level <= depth` — so with `--depth 2`, levels
1 and 2 are expanded but level-3 directories are listed (shown) yet not descended
into further.

Under `--json`, tree explicitly redirects to `ls -r` output instead of a JSON tree
(`treeMessage.JSON()` always `fatalIf`s — "JSON() should never be called here");
`mainTree` special-cases `globalJSON` to call `doList` directly with
`isRecursive:true, filter:"*"` instead of `doTree` (`cmd/tree-main.go:284-308`).

---

## 7. `find`

Flags (`cmd/find-main.go:34-97`): `--exec`, `--ignore`, `--versions`, `--name`,
`--newer-than`, `--older-than`, `--path`, `--print`, `--regex`, `--larger`,
`--smaller`, `--maxdepth` (`UintFlag`), `--watch`, `--metadata` (`StringSliceFlag`,
"MinIO server only"), `--tags` (`StringSliceFlag`, "MinIO server only").

All matching in `matchFind` (`cmd/find.go:396-437`) operates on a `path` value that
has the *target URL prefix already trimmed off* (`path := strings.TrimPrefix(fileContent.Key,
prefixPath)`), i.e. filters see paths relative to the find target, not absolute keys.
All conditions are AND'ed (`match = match && ...`), short-circuiting on first failure.

- **`--name`**: `nameMatch` (`cmd/find.go:87-99`) — first tries
  `filepath.Match(pattern, filepath.Base(path))` (glob against basename only,
  standard `filepath.Match` syntax: `*`, `?`, `[...]` char classes). **If that fails
  to match**, it falls back to an exact-equality scan over every path component
  (`strings.Split(path, "/")`) — i.e. `--name foo` also matches if any *directory
  component* along the path is literally `foo`, not just the basename. This fallback
  is a plain string `==`, not a glob.
- **`--path`**: `pathMatch` → `wildcard.Match(pattern, path)` (from
  `github.com/minio/pkg/v3/wildcard`) against the **full relative path** (not just
  basename), flat-namespace wildcard semantics (unlike `filepath.Match`, `*` in
  `wildcard.Match` crosses `/` boundaries — it treats the path as a flat string, see
  doc comment on `pathMatch`: "unlike path.Match(), considers a path as a flat name
  space").
- **`--regex`**: `regexp.MustCompile(cliCtx.String("regex"))` — **Go's RE2 syntax**
  (`regexp` stdlib), matched via `regexPattern.MatchString(path)` against the same
  relative path used by `--path` (full path, not basename). Help text: "match
  directory and object name with RE2 regex pattern."
- **`--ignore`**: same `pathMatch`/wildcard mechanism as `--path`, but **inverted**
  and evaluated first: `match = !pathMatch(ctx.ignorePattern, path)` — if it matches,
  the object is excluded regardless of every other filter.
- **`--larger` / `--smaller`**: parsed via `humanize.ParseBytes(...)`
  (`cmd/find-main.go:272-280`) — accepts the same human-size grammar documented in
  `find`'s own help UNITS section: `k, m, g, t` = metric KB/MB/GB/TB; `ki, mi, gi, ti`
  (case-insensitive) = IEC KiB/MiB/GiB/TiB; trailing `b` accepted; no suffix = bytes.
  Comparison is **strict**: `match = int64(ctx.largerSize) < fileContent.Size` (strictly
  greater than threshold) and `match = int64(ctx.smallerSize) > fileContent.Size`
  (strictly less than threshold) — `0` (flag unset) disables the filter entirely
  since the code guards on `ctx.largerSize > 0` / `ctx.smallerSize > 0` first.
- **`--maxdepth`**: NOT applied as a filter/predicate in `matchFind` at all — instead
  it *truncates the printed path* via `trimSuffixAtMaxDepth` (`cmd/find.go:212-225`),
  called from `getAliasedPath`. It does **not** stop descent or filter out
  deeper objects; deeper objects are still matched/printed but their displayed key is
  truncated to `maxDepth` path components below the start prefix. (Contrast with
  `tree --depth`, which genuinely stops descent.)
- **`--older-than` / `--newer-than`**: see §1 — matched against `fileContent.Time`
  (object `LastModified`, localized).
- **`--metadata KEY=REGEX`** / **`--tags KEY=REGEX`**: parsed by `getRegexMap`
  (`cmd/find.go:473-499`), split on first `=` (`SplitN(v, "=", 2)`); an entry with no
  `=` is a fatal parse error ("want one = separator, got none"); an entry with empty
  RHS (`key=`) means "key must not exist or be empty" (stored as `nil` regex, matched
  by `matchMetadataRegexMaps`/`matchRegexMaps` requiring `v[k] == ""`). Metadata
  lookups additionally fall back to the canonicalized `X-Amz-Meta-<Key>` header form
  if the bare key isn't found (`cmd/find.go:532-534`). Regex values are RE2 syntax,
  normalized to NFC unicode form before matching. Using either flag forces
  `WithMetadata: true` on the underlying `List()` call (extra cost).
- Objects with `StorageClass == "GLACIER"` are unconditionally skipped before any
  filter is applied (`cmd/find.go:304-306`).

**Action flags & default action** (`find()`, `cmd/find.go:250-265` and `324-332`):
precedence is `--exec` first, then `--print`, then **default = print full aliased
key** (`printMsg(findMessage{fileContent})`, i.e. same rendering as bare `mc find` —
prints the resolved path, optionally with ` (versionID)` suffix when `--versions`
is set, see `findMessage.String()` lines 51-57). So when neither `--exec` nor
`--print` is given, the default action is simply printing the match's path (like
plain `find`, not `find -print0`-style raw path but with `mc`'s colorized console
output).

**Substitution tokens** (`stringsReplace`, `cmd/find.go:342-392`), applied in this
exact order and available both bare and JSON-string-quoted (`{"tok"}` variants wrap
the substituted value with `strconv.Quote`):

```
{}        -> fileContent.Key (full path)
{""}      -> strconv.Quote(fileContent.Key)
{base}    -> filepath.Base(fileContent.Key)
{"base"}  -> strconv.Quote(filepath.Base(fileContent.Key))
{dir}     -> filepath.Dir(fileContent.Key)
{"dir"}   -> strconv.Quote(filepath.Dir(fileContent.Key))
{size}    -> humanize.IBytes(uint64(fileContent.Size))   (human string, e.g. "1.2MiB")
{"size"}  -> strconv.Quote(humanize.IBytes(...))
{time}    -> fileContent.Time.Format(printDate)            ("2006-01-02 15:04:05 MST")
{"time"}  -> strconv.Quote(...)
{url}     -> getShareURL(ctx, fileContent.Key)              (presigned GET URL, 7-day expiry, S3 targets only)
{"url"}   -> strconv.Quote(getShareURL(...))
{version} -> fileContent.VersionID
{"version"} -> strconv.Quote(fileContent.VersionID)
```
All are plain `strings.ReplaceAll` passes (not templating — no escaping of literal
`{}` sequences that appear in a filename itself; and `{url}`/`{"url"}` are only
computed if the substring is actually present in the string, to avoid an unnecessary
network round-trip per match). `{url}` internally does its own `expandAlias` +
`Stat` + `ShareDownload` with a hardcoded 7-day (`defaultSevenDays`) expiry — it does
NOT reuse whatever `--expire` a user might have configured for `share` (there's no
such flag on `find`).

`--exec` (`execFind`, `cmd/find.go:130-156`) tokenizes the **raw, un-substituted**
command line via `github.com/google/shlex` (POSIX shell-word splitting) *first*, then
substitutes `stringsReplace`'s tokens into each already-isolated word, then runs via
`exec.Command` directly (no shell interposed — no `sh -c`). **[Corrected against real
mc RELEASE.2025-08-13]**: an earlier draft of this doc claimed the command line was
tokenized *after* substitution; live-verified against the real binary that the opposite
is true — split-then-substitute, not substitute-then-split. Two probes only disagree
between the two orderings and both settle it: a matched key containing a space (e.g.
`sp file.txt`) reaches the spawned child as a single argv word (splitting the template
`"script {}"` first gives `["script", "{}"]`, and only then does `{}` get replaced with
the literal, unsplit key — substitute-then-split would instead hand the child two argv
words); and a matched key containing an unbalanced double quote (e.g. `unbal"file.txt`)
runs *without error* (substitute-then-split would feed `shlex` a string with a stray
quote from the substituted key and abort the whole find run on a parse error, which is
not what happens). On any exec failure it prints stderr (if any) + the Go error to
console and calls `os.Exit(getExitStatus(err))`, propagating the child's real exit code
via `syscall.WaitStatus` when available — i.e. `find --exec` **aborts the entire find
loop** on the first failing exec, exiting the whole `mc` process with the child's exit
status. `--print`'s output, by contrast, is never tokenized at all — it renders the raw
substituted string byte-for-byte (spaces/quotes from the key included).

`--watch` (bool) switches `find` into a perpetual mode: after the initial listing
completes, `watchFind` blocks on `clnt.Watch()` with `Recursive:true,
Events:["put"]` and re-runs the same match+action pipeline for every live PUT event
until the context is canceled (Ctrl-C). Note: only `"put"` events are watched — no
delete/remove notifications trigger `find --watch` actions.

Default target is `./` if no positional arg given (or `.` normalized to `./`).

---

## 8. `pipe`

Flags (`cmd/pipe-main.go:40-69`): `--storage-class/-sc`, `--attr`, `--tags`,
`--concurrent` (`IntFlag`, default `1`), `--part-size` (`StringFlag`, default =
`defaultPartSize()`), `--pipe-max-size` (hidden `IntFlag`), `checksumFlag`.

**Default part size**: `defaultPartSize()` calls minio-go's
`minio.OptimalPartInfo(-1, 0)` (objectSize=-1 meaning "unknown", configuredPartSize=0
meaning "use library default") and formats the resulting `partSize` via
`humanize.IBytes`. In `minio-go/v7` (`api-put-object-common.go`,
`OptimalPartInfo`), with `configuredPartSize==0` the library falls back to
`minPartSize = 16 MiB` (constant in `minio-go/v7/constants.go`:
`absMinPartSize = 5 MiB`, `minPartSize = 16 MiB`, `maxPartSize = 5 GiB`,
`maxPartsCount = 10000`, `maxMultipartPutObjectSize = 5 TiB`). **However, `minPartSize`
is only an *input* to the unknown-size branch's formula, not the final default itself**:
with `objectSize == -1`, `OptimalPartInfo` first substitutes `objectSize =
maxMultipartPutObjectSize` (5 TiB), then computes `partSizeFlt =
ceil((objectSize / maxPartsCount) / minPartSize) * minPartSize` = `ceil((5 TiB / 10000)
/ 16 MiB) * 16 MiB` = `ceil(32.768) * 16 MiB` = `33 * 16 MiB` = **528 MiB**. So **pipe's
actual default chunk size is 528 MiB**, matching the flag's displayed default
`--part-size` value ([Corrected against real mc RELEASE.2025-08-13]: the real binary's
own `mc pipe --help` shows `--part-size value ... (default: "528 MiB")`, confirming this
value directly — the original "16 MiB" conclusion above stopped short of finishing the
`OptimalPartInfo` arithmetic for the unknown-size case).

**Streaming/unknown-size handling**: `pipe()` (lines 141-208) always calls
`putTargetStreamWithURL(targetURL, reader, /*size=*/-1, opts)` — size is hardcoded to
`-1` ("Ignore size, since os.Stat() would not return proper size all the time for
local filesystem for example /proc files" — comment at line 172-173). This flows into
minio-go's streaming multipart PUT path which buffers/splits at `multipartSize`
boundaries (from `--part-size`, parsed via `humanize.ParseBytes`) as data arrives from
stdin, since total size is unknown up front.

**`--concurrent N`** sets `PutOptions.multipartThreads = N` and
`concurrentStream = ctx.IsSet("concurrent")` (i.e. concurrent part upload is only
enabled if the flag was *explicitly* passed, not just because it defaults to 1);
when `N>1`, mc proactively lowers `debug.SetGCPercent(20)` to bound memory growth
from buffering multiple large parts concurrently (comment: "we will be allocating
large buffers, reduce default GC overhead"). The flag help itself warns: "allow N
concurrent uploads [WARNING: will use more memory use it with caution]".

**`--pipe-max-size`** (hidden): calls a platform-specific `increasePipeBufferSize(os.Stdin,
N)` (e.g. `F_SETPIPE_SZ` on Linux, see `cmd/pipe_supported.go` / `pipe_unsupported.go`)
before streaming — an OS pipe-buffer tuning knob, unrelated to S3 multipart sizing;
not portable across OSes (no-op on unsupported platforms).

**Target semantics**: `pipe [TARGET]` — the `pipe()` helper does treat an empty
`targetURL` as "degenerate to `cat`" (`if targetURL == "" { return catOut(os.Stdin,
-1).Trace() }`), but **that branch is unreachable via a truly omitted argument**:
`mainPipe`'s `checkPipeSyntax(ctx)` runs first and unconditionally requires `len(ctx.Args())
== 1` (`if len(ctx.Args()) != 1 { showCommandHelpAndExit(ctx, 1) }`), so `mc pipe` with
**zero** args (or 2+) shows the full command help on stdout and exits `1` — it is *not* a
no-op passthrough, contrary to what the surface reading of `pipe()` alone suggests. The
passthrough is only reachable by supplying **exactly one, empty-string** argument (`mc
pipe ""`): `mainPipe` then calls `pipe(ctx, "", ...)` with that empty `targetURL`, and
*that* triggers the `catOut` branch. `--attr` and `--tags` build a metadata map exactly
like `cp`'s `getMetaDataEntry` (tags become `X-Amz-Tagging` header value verbatim, not
URL-encoded by mc itself). [Corrected against real mc RELEASE.2025-08-13]: confirmed on
the real binary — `echo -n x | mc pipe` (zero args) prints the full `pipe --help` text
and exits 1; `echo -n x | mc pipe ""` (one empty-string arg) streams `x` through to
stdout and exits 0; `echo -n x | mc pipe a b` (two args) also prints help and exits 1,
same as zero args.

---

## 9. `diff`

No flags of its own (`diffFlags = []cli.Flag{}` beyond globals). Args: exactly two
directory-like URLs (`SOURCE TARGET`); both are forced to have a trailing separator
before comparison; `checkDiffSyntax` requires both to `Stat` as directories (target
missing entirely is tolerated — `ObjectMissing` is swallowed — but if it exists it
must be a directory too).

**Comparison is name + size + type + (optionally metadata) — never content/hash.**
Both sides are listed recursively (`ListOptions{Recursive:true, ShowDir:DirNone}`,
default `opts.isMetadata=false` for the plain `diff` command via
`bucketObjectDifference`) and merge-compared in sorted order by the core
`differenceInternal` routine (`cmd/difference.go:227-391`), which is shared with
`mirror`'s incremental planner. Per-pair logic once keys match exactly (after NFC
unicode normalization of both suffixes):
1. Type mismatch (regular file vs non-regular, e.g. directory) → `differInType`.
2. `srcSize != tgtSize` → `differInSize`.
3. Else, "active-active" mtime heuristic (`activeActiveModTimeUpdated`, compares
   `X-Amz-Meta-Mm-Source-Mtime` if present, else falls back to raw `.Time.After()`) →
   `differInAASourceMTime` if source is judged newer than target by that heuristic.
4. Else, if `opts.isMetadata` is set (only true for `mirror --a`/metadata-diff mode,
   NOT plain `diff`) and `!metadataEqual(...)` on both `UserMetadata` and `Metadata`
   maps → `differInMetadata`.
5. Otherwise no output for that pair (objects considered equal) — `diff` never emits
   an explicit "same" (`differInNone`) line by default (`returnSimilar=false` for
   both `diff` and plain `mirror`; only some internal callers set it true).

Keys present only in source vs. only in target are found by walking both sorted
streams like a merge-join (string comparison of NFC-normalized suffixes) — no
extra key-sort flag; the underlying `List()` already returns lexicographically sorted
S3 keys, and local filesystem walking is expected sorted too.

**Legend** (quoted verbatim from `diffCmd.CustomHelpTemplate`, `cmd/diff-main.go:58-61`):
```
LEGEND:
  < - object is only in source.
  > - object is only in destination.
  ! - newer object is in source.
```
Actual `String()` output markers (`cmd/diff-main.go:83-106`) — note the legend text is
slightly imprecise vs. the code, which uses `!` for **all** of type/size/metadata/
mtime differences, not just "newer object in source":
```go
case differInFirst:  "< " + FirstURL   (colorized "DiffOnlyInFirst")
case differInSecond: "> " + SecondURL  (colorized "DiffOnlyInSecond")
case differInType:   "! " + SecondURL  (colorized "DiffType")
case differInSize:   "! " + SecondURL  (colorized "DiffSize")
case differInMetadata: "! " + SecondURL (colorized "DiffMetadata")
case differInAASourceMTime: "! " + SecondURL (colorized "DiffMMSourceMTime")
case differInNone:   "= " + FirstURL   (colorized "DiffInNone")  // not emitted by plain diff
```
Help text explicitly disclaims content comparison: *"Diff only calculates differences
in object name, size and time. It DOES NOT compare objects' contents."*
No `--exclude` or ignore-pattern flag exists for `diff` at all.

---

## 10. `share download` / `share upload` / `share list`

### Common
`shareDefaultExpiry = 604800 * time.Second` = **7 days**, defined once
(`cmd/ls-main.go:35-38`, oddly located in that file). Both `download` and `upload`
subcommands hard-clamp expiry: `< 1s` → fatal "Expiry cannot be lesser than 1
second."; `> 604800s` (7 days) → fatal "Expiry cannot be larger than 7 days." — so
**7 days is both the default AND the hard maximum**; there is no way to request a
longer-lived share link. `--expire, -E` flag default value string is `"168h"`
(`shareFlagExpire`, `cmd/share.go:46-50`), parsed with plain Go `time.ParseDuration`
(NOT the custom `d`/`w`/`y`-aware `ParseDuration` from §1) — so `--expire 7d` is
actually **invalid** for `share` (Go's stdlib `ParseDuration` has no `d` unit); must
use `168h` or `10080m` etc. Help text says `"set expiry in NN[h|m|s]"`.

Share metadata (URL, target, expiry, content-type, creation date) persists to
`~/.mc/share/uploads.json` / `downloads.json` via a tiny local JSON DB
(`cmd/share-db-v1.go`, not detailed further here — implementation detail of local
state, not S3-facing).

### `share download`
Args: `TARGET [TARGET...]`. Flags: `--recursive/-r`, `--version-id/--vid`,
`--expire/-E`. `--version-id` + `--recursive` together is a fatal error. Without
`--recursive`, every target is `Stat`'d up front to confirm existence
(`url2Stat`); with `--recursive`, existence isn't pre-checked (folder semantics take
over). For each resolved object, generates a presigned-GET share URL via
`clnt.ShareDownload(ctx, versionID, expiry)` and prints one `shareMessage` per object
(directories among a recursive listing are skipped, not descended-error'd).

### `share upload`
Args: `TARGET [TARGET...]`. Flags: `--recursive/-r`, `--expire/-E`,
`--content-type/-T`. A non-recursive target ending in the path separator (i.e. looks
like a prefix/folder) is rejected: *"Use --recursive flag to generate curl command for
prefixes."* Produces a **presigned POST policy** (`clnt.ShareUpload(ctx, isRecursive,
expiry, contentType)`) and renders a literal shell `curl` command a human can run to
upload without needing S3 credentials. Curl-command template (`makeCurlCmd`,
`cmd/share-upload-main.go:117-135`):
```go
curlCommand := "curl " + postURL + " "
for k, v := range uploadInfo {         // POST-policy form fields returned by the SDK
	if k == "key" { key = v; continue }  // "key" field overrides the object key var
	curlCommand += fmt.Sprintf("-F %s=%s ", k, v)
}
if isRecursive {
	curlCommand += fmt.Sprintf("-F key=%s<NAME> ", shellQuote(key)) // literal "<NAME>" placeholder
} else {
	curlCommand += fmt.Sprintf("-F key=%s ", shellQuote(key))
}
curlCommand += "-F file=@<FILE>" // literal "<FILE>" placeholder the user must substitute
```
So the printed command contains the literal tokens `<NAME>` (only for `--recursive`,
meaning "append the desired filename here") and `<FILE>` (always, meaning "path to
the local file to upload") for the user to fill in — these aren't real shell
variables, they're placeholders `shareMessage.String()` re-highlights specially
(`strings.Replace(s.ShareURL, "<FILE>", ..., 1)` etc., see `cmd/share.go:70-73`).
Field values are shell-escaped via `shellQuote` (`regexp` escaping of
`` ([&;#$` \t\n<>()|'"]) `` with a backslash) — but note `postURL` itself and the
non-`key` `-F` field values are **not** shell-quoted, only the `key` value is.

### `share list [upload|download]`
No flags. Requires exactly one positional arg, either literal `upload` or
`download` (anything else → usage/help + exit 1). Loads the corresponding local JSON
DB and prints one `shareMessage` per saved entry with `TimeLeft: share.Expiry -
time.Since(share.Date)` — i.e. it recomputes remaining TTL live at list time rather
than storing a fixed absolute expiry; entries whose computed `TimeLeft` has gone
negative are still listed (no pruning/filtering of expired entries here).

---

## 11. `ls`

Flags (`cmd/ls-main.go:33-64`): `--rewind`, `--versions`, `--recursive/-r`,
`--incomplete/-I`, `--summarize`, `--storage-class/-sc`, `--zip`.

**`--summarize`**: appends one extra block at the very end of output
(`cmd/ls.go:262-267`, `summaryMessage`), string form (`cmd/ls.go:186-191`):
```
\nTotal Size: <humanize.IBytes(totalSize)>
Total Objects: <totalObjects>
```
(two lines, colorized "Summarize", leading blank line before "Total Size"). JSON form
emits `{"totalObjects":N,"totalSize":N}` as its own separate JSON object appended
after the per-object JSON lines (not merged into a wrapping array — `mc`'s JSON output
is always newline-delimited JSON objects, never a JSON array). `totalSize`/
`totalObjects` accumulate ALL listed content (subject to `--storage-class` filtering)
including every version's size if `--versions` is set — no separate "objects vs
versions" distinction in the summary count.

**`--incomplete/-I`**: passed through as `ListOptions{Incomplete: true}` to the
client (`cmd/ls.go:229-237`) — for S3 backends this maps to listing **in-progress
multipart uploads** (`ListMultipartUploads`) rather than committed objects; the
underlying API is MinIO/AWS S3 `ListMultipartUploads`, not a MinIO-only extension, so
it's implementable against any real S3-compatible endpoint. Output uses the same
`contentMessage` shape (size reported is bytes uploaded so far for the incomplete
upload, no special "incomplete" marker field is added to the JSON — check
`client-s3.go`'s incomplete-listing code if exact fields matter for parity; not
further expanded in this pass).

**`--storage-class`**: pure client-side post-filter (`cmd/ls.go:244-246`):
```go
if content.StorageClass != "" && o.filter != "" && o.filter != "*" && content.StorageClass != o.filter {
	continue
}
```
i.e. objects with an *empty* `StorageClass` (e.g. STANDARD on some backends omits the
field) are **never filtered out** even if a specific class was requested — only
objects that explicitly report a non-matching storage class are excluded. `"*"` (or
empty string) disables the filter. This is exact-match string comparison, no
wildcarding beyond the `"*"` sentinel meaning "no filter".

Per-object listing groups by path (`lastPath != content.URL.Path` triggers a flush of
`perObjectVersions` via `printObjectVersions`, which is where `--versions` sorting
happens — `sortObjectVersions` puts `IsLatest` first, then by `Time` descending) —
this is how multiple versions of the same key get grouped and ordered together in
output even though the underlying List stream interleaves nothing (S3 ListObjectVersions
already returns versions grouped/ordered per key, but mc re-sorts defensively).

Auto-detects a bare object URL that's actually a directory and re-lists with a
trailing separator appended (`mainList`, lines 220-228) — convenience so `mc ls
s3/bucket/prefix` (no trailing slash) still lists the prefix's contents rather than
erroring as "not found".

---

## 12. `cat`

Flags (`cmd/cat-main.go:37-62`): `--rewind`, `--version-id/--vid`, `--zip`,
`--offset` (`Int64Flag`), `--tail` (`Int64Flag`), `--part-number` (`IntFlag`).

Mutual-exclusion / validation rules (`parseCatSyntax`, lines 174-223):
- `--tail` and `--offset` together → fatal ("You cannot specify both --tail and
  --offset").
- Either negative → fatal ("You cannot specify negative --tail or --offset").
- `--zip` combined with either `--tail` or `--offset` → fatal.
- stdin mode (`-` or no args) combined with `--zip`/`--tail`/`--offset` → fatal.
- `(--tail != 0 || --offset != 0)` combined with `--part-number > 0` → fatal ("You
  cannot use --part-number with --tail or --offset").
- `--version-id` + `--rewind` together → fatal; `--version-id` with more than one
  positional TARGET → fatal.

**`--offset`**: becomes `GetOptions.RangeStart` — implemented as an actual HTTP Range
GET on S3 targets (`gopts := GetOptions{..., RangeStart: o.startO, PartNumber:
o.partN}`, `catURL` lines 253-271); for local filesystem it's an `os.Seek`
(`client-fs.go` `Get()`, `if opts.RangeStart != 0 { fileData.Seek(opts.RangeStart,
io.SeekStart) }`).

**`--tail N`**: computed client-side *before* the GET, by first `Stat`-ing the object
to learn its size, then converting to an equivalent `RangeStart`:
```go
if o.tailO > 0 && content.Size > 0 {
	o.startO = max(content.Size-o.tailO, 0)
}
```
i.e. `--tail` is just sugar for `--offset (size - tail)`, clamped to 0 (never
negative) — so `--tail` bigger than the object just returns the whole object, matches
POSIX `tail -c`. Because it depends on an up-front `Stat`, `--tail` cannot be combined
with stdin (`-`) input, which has no knowable size.

**`--part-number N`**: passed straight through as `GetOptions.PartNumber` to the S3
GetObject call (`x-amz-part-number` selects a specific part of a multipart-uploaded
object, S3-native semantic — not mc-invented). When `--part-number` is set, the
size-validation logic is explicitly disabled (`if o.partN != 0 { size = int64(-1) }`)
since the part's size can't be predicted from the full object's `Content-Length`.

**Size verification / output framing** (`catOut`, lines 285-326): after
`io.Copy(stdout, r)`, if a known `size != -1` was expected (from the pre-GET Stat,
adjusted for offset) and the actual bytes copied (`n`) don't exactly match, returns
`UnexpectedEOF{TotalSize: size, TotalWritten: n}` as an error — this check applies
symmetrically for both truncated (`n < size`) and over-long (`n > size`) transfers.
Terminal output goes through the same `prettyStdout` non-printable-character filter
as `head` (`^?` substitution). EPIPE on stdout (reader side closed, e.g. piping into
`head`) is treated as a clean/silent success, not an error. Concatenating multiple
positional args streams each one in sequence to stdout (classic `cat file1 file2`
semantics); a literal `-` argument reads stdin at that position, with special
argument-order-preserving logic (`cmd/cat-main.go:345-354`) to keep `-`'s position
correct relative to other args, working around `cli`'s own arg reordering.

---

## 13. `stat`

Flags (`cmd/stat-main.go:32-58`): `--rewind`, `--versions`, `--version-id/--vid`,
`--recursive/-r`, `--verbose/-v`, `--no-list`.

Validation (`parseAndCheckStatSyntax`, lines 104-157):
- `--version-id` + more than one positional arg → fatal.
- `--version-id` combined with any of `--rewind`/`--versions`/`--recursive` → fatal.
- `(--recursive || --versions)` combined with `--no-list` → fatal ("You cannot
  specify --no-list with either --versions or --recursive").

**`--recursive` behavior**: passed down into `statURL` (defined in `cmd/stat.go`,
not fully expanded in this pass, but the entry point signature is `statURL(ctx,
targetURL, versionID, rewind, withVersions, /*isIncomplete*/false, isRecursive,
headOnly, encKeyDB)`) — recursion causes `stat` to walk every object under the
target prefix (like `ls -r`) and print a full per-object metadata block for each,
rather than a single directory-level summary. It is orthogonal to `--verbose`.

**`--verbose` on bucket/alias-root targets**: handled specially, *before* calling
`statURL` at all (lines 138-156) — if the target URL has no path component (i.e. it's
a bare alias like `myminio`) and `--verbose` is set, `stat` lists all buckets on that
alias (`clnt.ListBuckets`) and expands the single argument into one target URL per
bucket (`filepath.Join(url, bucket.BucketName)`), each subsequently `stat`'d
individually — so `mc stat myminio --verbose` effectively becomes "stat every bucket
individually", not a single combined alias-level stat call. Without `--verbose`, a
bare alias target is stat'd as-is (whatever a zero-path Stat call returns, e.g.
generic "alias reachable" info, not expanded per-bucket).

Human-output layout for the per-object/bucket blocks is defined in `cmd/stat.go`
(`statMessage`/`statBucketMessage` types, colors set up as `Name`, `Date`, `Size`,
`ETag`, `Metadata`, plus a distinct `Key`/`Value`/`Unset`/`Set`/`Title`/`Count`
palette used specifically for the bucket-summary variant of `stat`) — exact field
ordering/table layout wasn't traced line-by-line in this pass; flag it for a
follow-up read of `cmd/stat.go` if byte-for-byte output parity (not just semantic
parity) with `mc stat`'s human table is required.

`--no-list` disables any LIST call entirely (single HeadObject/Stat-equivalent per
target, no enumeration) — useful against buckets where the caller lacks
`s3:ListBucket` permission but has `s3:GetObject`.

---

## 14. `ping` / `ready` — MinIO-only, not portable

**Both commands are MinIO-server-specific and should be dropped/stubbed for rs3's
generic-S3 goal** — neither uses the S3 REST API at all; both go through
`github.com/minio/madmin-go/v3`'s **admin API client**, which targets MinIO's
non-S3, server-management HTTP surface (paths under `/minio/...` and
`/minio/health/...`, requiring either anonymous access to MinIO's health endpoints or
full admin credentials — none of this exists on AWS S3, Ceph RGW's S3-only mode,
Backblaze B2's S3 gateway, etc.).

- **`ping`** (`cmd/ping.go`): builds both a `madmin.AdminClient`
  (`newAdminClient`, used only for `--distributed`/`--node` modes to call
  `admClnt.ServerInfo(ctx)` — cluster topology info, a MinIO admin-only concept) and a
  `madmin.AnonymousClient` (`newAnonymousClient`) whose `Alive(ctx,
  madmin.AliveOpts{}, servers...)` method is the actual per-request liveness probe
  used every iteration (`cmd/ping.go:283-320`). This maps to MinIO's `/minio/health/live`
  (or similar internal anonymous health endpoint) under the hood — not part of the S3
  API. `--distributed/-a` and `--node` flags require MinIO's clustered-server info
  format (`madmin.ServerProperties`), meaningless for a single-endpoint generic S3
  target.
- **`ready`** (`cmd/ready-main.go`): builds only a `madmin.AnonymousClient` and polls
  `anonClient.Healthy(ctx, madmin.HealthOpts{ClusterRead, Maintenance})` every 5s
  (`healthCheckInterval`) until healthy or canceled. `--cluster-read` and
  `--maintenance` map directly to MinIO's cluster-quorum/maintenance-mode concepts
  (`WriteQuorum`, `HealingDrives` in the response) — again MinIO-server internals with
  no generic-S3 equivalent.

**Recommendation for rs3**: drop both commands (or implement a best-effort shim that
just does a lightweight `HeadBucket`/`ListBuckets` call and reports reachability +
latency, explicitly documenting that it is NOT equivalent to `mc ping`/`mc ready`
against a real MinIO cluster). Do not attempt byte-for-byte compatibility here.

---

## 15. Alias resolution order: env vs config file

Definitive precedence, from `expandAlias` (`cmd/config.go:322-345`), checked in this
exact order and **short-circuiting at the first match** (no merging):

```go
func expandAlias(aliasedURL string) (alias, urlStr string, aliasCfg *aliasConfigV10, err *probe.Error) {
	alias, path := url2Alias(aliasedURL)

	// 1. MC_HOST_<alias> environment variable — highest priority.
	if env.IsSet(mcEnvHostPrefix + alias) {
		aliasCfg, err = expandAliasFromEnv(env.Get(mcEnvHostPrefix+alias, ""))
		...
		return alias, urlJoinPath(aliasCfg.URL, path), aliasCfg, nil
	}

	// 2. Aliases pre-loaded from MC_CONFIG_ENV_FILE (a file of MC_HOST_x=... lines).
	aliasCfg = aliasToConfigMap[alias]
	if aliasCfg != nil {
		return alias, urlJoinPath(aliasCfg.URL, path), aliasCfg, nil
	}

	// 3. On-disk config file (~/.mc/config.json), via mustGetHostConfig.
	if aliasCfg = mustGetHostConfig(alias); aliasCfg != nil {
		return alias, urlJoinPath(aliasCfg.URL, path), aliasCfg, nil
	}

	// 4. No match anywhere: treat aliasedURL as a literal (non-aliased) URL/path.
	return "", aliasedURL, nil, nil
}
```
So the order is: **`MC_HOST_<alias>` env var > `MC_CONFIG_ENV_FILE`-sourced entries >
`~/.mc/config.json` file entries > literal passthrough.** Note `mustGetHostConfig`
(`cmd/config.go:196-211`), used as a fallback helper elsewhere too, actually
re-checks env first *inside itself* as well (`env.Get(mcEnvHostPrefix+alias, "")`)
before falling back to `aliasToConfigMap` and then `getAliasConfig` (the real
config-file loader) — redundant with step 1/2 above but consistent, not
contradictory.

`MC_CONFIG_ENV_FILE` itself (env var naming the file path) is read once via
`readAliasesFromFile` (`cmd/config.go:275-303`) into the global
`aliasToConfigMap`; each line must be `MC_HOST_<alias>=<url>` form (same value
grammar as the direct env var, parsed by the same `expandAliasFromEnv`/
`parseEnvURLStr`).

**`MC_HOST_<alias>` / config-file URL grammar** (`parseEnvURLStr`,
`cmd/config.go:219-266`): `https?://[accessKey[:secretKey[:sessionToken]]@]host[/]`
— two regexes tried in order: `hostKeyTokens` (5-part: scheme, key, secret, token,
host — for STS/temporary credentials with a session token) then `hostKeys` (4-part:
scheme, key, secret, host — the common case). If neither regex matches, the raw
string is parsed as a normal URL and credentials pulled from `url.User` instead (i.e.
`https://user:pass@host` also works via stdlib URL user-info parsing as a fallback
path). Validation rejects any URL with a non-http(s) scheme, non-root/non-empty path,
opaque component, force-query, raw query, or fragment — env-var/config aliases must
be bare scheme+host, no path or query string allowed.

Everything not in RS3's current mc-env-fallback work
(`RS3_HOST_*`/`RS3_CONFIG_*` per recent commit `62a3f7d`) needs to preserve this exact
3-tier precedence (env var > env-config-file > config-file) to match `mc` behavior,
not just support the two source *types*.

---

## Surprising behaviors (gotchas for rs3 parity)

1. **`isOlder`/`isNewer` are named for the opposite of what their return value gates.**
   `isOlder(t, "7d")` returns **true when the object is NOT actually 7+ days old**
   (age < threshold); every caller negates or otherwise re-interprets the boolean.
   Reimplementing straightforwardly from the function names will silently invert
   `--older-than`/`--newer-than` filtering.

2. **`--older-than`/`--newer-than` silently accept absolute dates too**, via a
   fallback chain through `rewindSupportedFormat` when the day/hour/minute duration
   grammar fails to parse — so `mc find s3 --older-than 2020.01.01` is valid input,
   not an error, even though the flag help only documents the duration form.

3. **`mv` doesn't roll back or retry on partial failure.** A batch `mv` over many
   objects can end with some objects copied+deleted, some copied-but-not-yet-deleted
   (queued in the async `removeManager`, drained only errors-logged not
   failure-halted), and some left entirely untouched at the source (copy failed) —
   there's no transactional guarantee and no cleanup pass; the exit code is
   nonzero if *any* copy failed, but doesn't distinguish "some objects moved, some
   didn't" from a clean failure.

4. **`put --if-not-exists` is a hidden flag with a self-admitted server-support
   caveat** ("Only supported in newer MinIO releases") — it sends `If-None-Match: *`
   unconditionally; against an S3 backend that doesn't honor conditional writes on
   PUT (or older MinIO), the request likely just succeeds and silently overwrites,
   defeating the flag's purpose. rs3 should document this rather than assume the
   header is universally enforced.

5. **`du`'s default `--depth` resolution is a 3-way branch that's easy to get wrong**:
   plain `mc du` = depth 1 (NOT depth -1/unlimited, despite `du` conceptually being a
   recursive-summary tool); `-r` alone = depth -1 (unlimited); `-d N` always wins
   outright regardless of `-r`. And `du` prints **one line per directory level
   visited**, not a single grand total, whenever depth is anything other than
   exactly 1.

6. **`find --maxdepth` doesn't limit what's matched — only how the printed path is
   truncated.** Deeper objects are still found/acted-on (executed against, counted,
   etc.); only their displayed `{}`/key text gets shortened. This is easy to
   misimplement as an actual descent-limiting depth filter (which is what `tree
   --depth` actually does, creating an inconsistency between the two commands that
   share the word "depth").

7. **`--name` on `find` has a hidden fallback path**: if the basename glob
   (`filepath.Match`) doesn't match, mc additionally checks every *path component*
   for an *exact string* match against the pattern — so `--name foo` can match a key
   like `a/foo/bar.txt` even though `foo` isn't the basename and isn't a glob pattern
   with wildcards.

8. **`ls --storage-class` never filters out objects with an empty storage class
   field**, even when a specific class is requested — only objects that positively
   report a *different* non-empty class get excluded. A naive `storage_class ==
   filter` implementation without the "empty string is exempt" carve-out will produce
   a stricter (wrong) filter than real `mc`.

9. **`share upload`/`share download`'s `--expire` flag uses plain Go
   `time.ParseDuration`**, not the custom `d`/`w`/`y`-aware `ParseDuration` used by
   `cp`/`mv`/`mirror`/`rm`/`find`'s `--older-than`/`--newer-than`. `--expire 7d` is
   invalid for `share`; only `h`/`m`/`s`/etc. Go-native units work, capped hard at 7
   days (604800s) both as ceiling and as the default.

10. **`ping`/`ready` are pure MinIO-admin-API commands with zero S3 REST API
    involvement** — they cannot be implemented against generic S3-compatible storage
    at all without redefining what they mean; recommend dropping them from rs3's
    scope rather than attempting a compatibility shim that would necessarily diverge
    in behavior.
