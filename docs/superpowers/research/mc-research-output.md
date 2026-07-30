# MinIO `mc` output contract — reference for `rs3`

Source: `/home/code/workspace/mc` (read-only reference). All paths below are relative to that
repo root unless stated otherwise. Go module versions actually in use (per `go.mod`):
`github.com/minio/pkg/v3 v3.1.0` (console), `github.com/minio/colorjson v1.0.8` (JSON encode),
`github.com/minio/cli v1.24.2` (urfave/cli v1 fork), `github.com/cheggaaa/pb` (progress bar v1).

---

## 1. The printMsg/message machinery

### 1.1 The `message` interface and `printMsg`

`cmd/print.go`:

```go
// message interface for all structured messages implementing JSON(), String() methods.
type message interface {
	JSON() string
	String() string
}

// printMsg prints message string or JSON structure depending on the type of output console.
func printMsg(msg message) {
	var msgStr string
	if !globalJSON {
		msgStr = msg.String()
	} else {
		msgStr = msg.JSON()
		if globalJSONLine && strings.ContainsRune(msgStr, '\n') {
			// Reformat.
			var dst bytes.Buffer
			if err := json.Compact(&dst, []byte(msgStr)); err == nil {
				msgStr = dst.String()
			}
		}
	}
	msgStr = strings.TrimSuffix(msgStr, "\n")
	console.Println(msgStr)
}
```

Every command's output message type implements `String()` (human) and `JSON()` (machine).
`printMsg` is the single choke point: `--json` picks `.JSON()`, otherwise `.String()`. If
`globalJSONLine` is set, the (indented, multi-line) JSON that `.JSON()` produced gets
re-compacted to a single line before being written — this is the "JSON lines" behavior.

### 1.2 Global flag state (`cmd/globals.go`)

```go
globalErrorExitStatus  = 1
globalCancelExitStatus = 130
globalKillExitStatus   = 137
globalTerminatExitStatus = 143
```
```go
var (
	globalQuiet    = false
	globalJSON     = false
	globalJSONLine = false // Print json as single line.
	globalDebug    = false
	globalNoColor  = false
	...
)
```

`setGlobalsFromContext` (`cmd/globals.go:126`) computes these from the parsed CLI flags:

```go
globalQuiet    = globalQuiet || quiet
globalDebug    = globalDebug || debug
globalJSONLine = !isTerminal() && json
globalJSON     = globalJSON || json
globalNoColor  = globalNoColor || noColor || globalJSONLine

if globalNoColor || globalQuiet {
	console.SetColorOff()
	lipgloss.SetColorProfile(termenv.Ascii)
}
```

**Key gotcha**: `--json` alone gives pretty-printed (indented) JSON *when stdout is a TTY*;
it only collapses to single-line "JSON lines" output when stdout is **not** a TTY (piped /
redirected). `isTerminal()` (`cmd/update-main.go:359`) = `isatty.IsTerminal(stdout) &&
isatty.IsTerminal(stderr)` (both must be TTYs).

Also, in `cmd/main.go` `Main()`:
```go
if w, h, e := term.GetSize(int(os.Stdout.Fd())); e != nil {
	globalQuiet = runtime.GOOS != "windows"
} else {
	globalTermWidth, globalTermHeight = w, h
}
```
i.e. **if stdout size can't be determined (non-TTY) and the OS isn't Windows, `mc`
auto-enables `--quiet`**, even without the flag being passed.

### 1.3 The JSON error envelope, `fatalIf`/`errorIf` (`cmd/error.go`)

```go
type causeMessage struct {
	Message string `json:"message"`
	Error   error  `json:"error"`
}

type errorMessage struct {
	Message   string             `json:"message"`
	Cause     causeMessage       `json:"cause"`
	Type      string             `json:"type"`
	CallTrace []probe.TracePoint `json:"trace,omitempty"`
	SysInfo   map[string]string  `json:"sysinfo,omitempty"`
}
```

`fatalIf`/`fatal` (exits process) and `errorIf` (does not exit) both build the **same**
top-level envelope in JSON mode:

```go
json, e := json.MarshalIndent(struct {
	Status string       `json:"status"`
	Error  errorMessage `json:"error"`
}{
	Status: "error",
	Error:  errorMsg,
}, "", " ")
console.Println(string(json))
```

`errorMsg.Type` is `"fatal"` for `fatal()` and `"error"` for `errorIf()`. `CallTrace`/`SysInfo`
are only populated `if globalDebug`. So the JSON envelope on error is:

```json
{
 "status": "error",
 "error": {
  "message": "<the msg passed to fatalIf/errorIf, printf-formatted>",
  "cause": {
   "message": "<err.Error()>",
   "error": <the raw error's default JSON marshaling, usually {} or a string>
  },
  "type": "fatal" | "error"
 }
}
```
(`trace`/`sysinfo` only appear with `--debug`, `omitempty`.)

`fatal()` additionally calls `console.Fatalln()` at the very end (after printing the JSON, in
JSON mode) which does `os.Exit(1)`; in non-JSON mode it never prints the JSON, only the
human line, then `console.Fatalln(...)`.

### 1.4 Human-mode error text and the `mc: <ERROR>`/`mc: <FATAL>`/`mc: <DEBUG>` prefixes

The prefix logic is **not** in mc itself — it's in the vendored console package,
`github.com/minio/pkg/v3/console` (`console.go`), used via `console.Errorln`/`console.Fatalln`.
`consolePrintln` (identical logic exists in `consolePrint`/`consolePrintf`):

```go
func consolePrintln(tag string, c *color.Color, a ...interface{}) {
	...
	switch tag {
	case "Debug":
		...
		c.Print(ProgramName() + ": <DEBUG> ")   // if stderr is a TTY
		// else: fmt.Fprint(color.Output, ProgramName()+": <DEBUG> ")
	case "Fatal":
		fallthrough
	case "Error":
		...
		c.Print(ProgramName() + ": <ERROR> ")   // Fatal and Error share the SAME "<ERROR>" tag
	case "Info":
		...
		c.Print(ProgramName() + ": ")           // no bracketed tag
	default:
		// Print/Println: no prefix at all
	}
}

func ProgramName() string {
	_, progName := filepath.Split(os.Args[0])
	return progName
}
```

So:
- `console.Error*`/`console.Fatal*` (i.e. mc's `errorIf`/`fatal` in human mode) print
  **`<argv[0] basename>: <ERROR> <message>`** to **stderr**. Note it is literally `<ERROR>`
  for *both* `Error` and `Fatal` calls — there is no separate `<FATAL>` tag in this version.
- `console.Debug*` prints **`<prog>: <DEBUG> <message>`** to stderr, only if `globalDebug`
  (`errorIf`/`fatal` build the message differently when `!globalDebug` — see below).
- `console.Info*`/`console.Println` prints **`<prog>: <message>`** / plain message to stdout
  with no bracket tag.
- The program name is derived from `os.Args[0]`'s basename at print time (typically `mc`, or
  `mc.exe` trimmed on Windows only inside mc's own `appName` var — the console package itself
  does not trim `.exe`).
- All of Debug/Fatal/Error tags write to **stderr** (`color.Output` swapped to
  `stderrColoredOutput`); Print/Info write to stdout via `color.Output` (default stdout).

`fatal()`/`errorIf()` (`cmd/error.go`) construct the human message before handing it to
`console.Fatalln`/`console.Errorln`:

```go
msg = fmt.Sprintf(msg, data...)
errmsg := err.String()
if !globalDebug {
	// use err.ToGoError().Error() instead of the probe.Error's full trace/String()
	errmsg = e.Error()
}
msg = strings.TrimSpace(msg)
errmsg = strings.TrimSpace(errmsg)
// Add punctuation
if len(errmsg) > 0 && len(msg) > 0 {
	if msg[len(msg)-1] != ':' && msg[len(msg)-1] != '.' {
		if unicode.IsUpper(rune(errmsg[0])) {
			msg += "."
		} else {
			msg += ":"
		}
	}
	if errmsg[len(errmsg)-1] != '.' {
		errmsg += "."
	}
}
console.Fatalln(fmt.Sprintf("%s %s", msg, errmsg))
```

So a typical fatal human-mode line looks like:
`mc: <ERROR> Unable to make bucket 's3/foo'.: Access Denied.`
(exact punctuation depends on whether the caller's `msg` already ends in `:`/`.` and whether
the underlying Go error string starts uppercase.)

If the context was canceled (Ctrl-C), the detail error text becomes literally
`"Canceling upon user request"` instead of the real underlying error, **unless** `--debug`
is set (then the full probe trace is used, via `err.String()`, ignoring the cancellation
substitution).

### 1.5 Usage errors (`onUsageError`, `cmd/main.go:166`)

Invalid flags produce (always, regardless of `--json`):
```go
fmt.Fprintln(&errMsg, "Invalid command usage,", err.Error())
fmt.Fprintln(&errMsg, "\nSUPPORTED FLAGS:")
for _, h := range help {
	fmt.Fprintf(&errMsg, "   %s%s%s\n", h.flagName, spaces, h.usage)
}
console.Fatal(errMsg.String())
```
i.e. `mc: <ERROR> Invalid command usage, ...` followed by a flag usage dump, **not** wrapped
in the JSON error envelope even under `--json` (this path bypasses `fatalIf`/`errorIf`
entirely and calls `console.Fatal` directly, which unconditionally calls `os.Exit(1)`).

---

## 2. Per-command message structs

Every struct below lives in `cmd/<file>.go`. Boilerplate pattern: `String()` produces the
colorized human line, `JSON()` sets `Status = "success"` (or similar) then calls
`json.MarshalIndent(x, "", " ")` (single-space indent) using `github.com/minio/colorjson`
(API-compatible with stdlib `encoding/json`, adds ANSI colorization when stdout is a
color-capable TTY, and no colorization otherwise/under `--no-color`/`--quiet`).

### ls — `contentMessage` (`cmd/ls.go`)

```go
type contentMessage struct {
	Status   string    `json:"status"`
	Filetype string    `json:"type"`
	Time     time.Time `json:"lastModified"`
	Size     int64     `json:"size"`
	Key      string    `json:"key"`
	ETag     string    `json:"etag"`
	URL      string    `json:"url,omitempty"`

	VersionID      string `json:"versionId,omitempty"`
	VersionOrd     int    `json:"versionOrdinal,omitempty"`
	VersionIndex   int    `json:"versionIndex,omitempty"`
	IsDeleteMarker bool   `json:"isDeleteMarker,omitempty"`
	StorageClass   string `json:"storageClass,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}
```

`String()`:
```go
message := console.Colorize("Time", fmt.Sprintf("[%s]", c.Time.Format(printDate)))
message += console.Colorize("Size", fmt.Sprintf("%7s", strings.Join(strings.Fields(humanize.IBytes(uint64(c.Size))), "")))
// optional: " " + StorageClass
// optional (versioned): " "+VersionID + " v"+VersionOrd + " DEL"/" PUT"
fileDesc += " " + c.Key
// colorized "Dir" if folder else "File"
```
`printDate = "2006-01-02 15:04:05 MST"` (Go reference layout), `c.Time` is `.Local()`.
Size is `humanize.IBytes` (binary/IEC units, e.g. `1.0KiB`) with internal spaces stripped,
right-justified to width 7.

Directories get `Key` suffixed with `/` (`getOSDependantKey`). `VersionOrd` is
`(number of versions for this key) - index`, i.e. 1-based ordinal counting from the oldest
shown = highest number is the newest (only emitted when iterating `--versions`).

Companion **`summaryMessage`** (for `ls --summarize`):
```go
type summaryMessage struct {
	TotalObjects int64 `json:"totalObjects"`
	TotalSize    int64 `json:"totalSize"`
}
```
String(): `"\nTotal Size: <humanize.IBytes>"` + `"\nTotal Objects: <n>"`, both colorized tag
`"Summarize"`. Note: **no `status` field on summaryMessage**, and its `JSON()` uses
`json.MarshalIndent(s, "", "")` — empty indent string, still multi-line but with no leading
spaces per level (unlike every other message type which uses `" "`).

### stat — `statMessage` (`cmd/stat.go`)

```go
type statMessage struct {
	Status            string             `json:"status"`
	Key               string             `json:"name"`
	Date              time.Time          `json:"lastModified"`
	Size              int64              `json:"size"`
	ETag              string             `json:"etag"`
	Type              string             `json:"type,omitempty"`
	Expires           *time.Time         `json:"expires,omitempty"`
	Expiration        *time.Time         `json:"expiration,omitempty"`
	ExpirationRuleID  string             `json:"expirationRuleID,omitempty"`
	ReplicationStatus string             `json:"replicationStatus,omitempty"`
	Metadata          map[string]string  `json:"metadata,omitempty"`
	VersionID         string             `json:"versionID,omitempty"`
	DeleteMarker      bool               `json:"deleteMarker,omitempty"`
	Restore           *minio.RestoreInfo `json:"restore,omitempty"`
	Checksum          map[string]string  `json:"checksum,omitempty"`
}
```

Human layout (`String()`), each field on its own line as `%-10s: value`, in this order,
each conditional on non-zero/non-empty:
`Name` (always) → `Date` → `Size` (skipped when `Type == "folder"`) → `ETag` → `VersionID`
(appends ` (delete-marker)` if `DeleteMarker`) → `Type` (always) → `Expires` → `Expiration`
(appends ` (lifecycle-rule-id: <id>)`) → `Checksum` (map rendered via `fmt.Sprintf("%v", ...)`
with `map[`/`]` stripped) → `Restore` block (`ExpiryTime`, `Ongoing`) → `Encryption` (derived
from metadata headers, values `SSE-KMS`/`SSE-C`/`SSE-S3`/`SSE-Unknown`) → `Metadata` block
(each non-encryption key, aligned to the longest key) → `Replication Status`. Trailing blank
line always appended.

`getKey`/`Name` value comes from `getOSDependantKey`, same helper as `ls`.

For bucket-level `stat` (no object, non-recursive): a different struct, `bucketInfoMessage`
(`cmd/stat.go:428`):
```go
type bucketInfoMessage struct {
	Status string `json:"status"`
	BucketInfo         // embedded, see below
	Usage  madmin.BucketUsageInfo
}
```
`BucketInfo` carries `Key`("name")/`Date`("lastModified")/`Size`("size", but always `-`
"N/A" in human mode)/`Versioning`/`Encryption`/`Locking`(ObjectLock)/`Replication`/`Policy`/
`Location`/`Tagging`/`ILM`/`Notification` — see full struct in `cmd/stat.go:371-407` if the
bucket-info path needs replicating; JSON uses `enc.SetEscapeHTML(false)` so `<`/`>`/`&` are
**not** escaped (differs from the default colorjson/encoding-json HTML-escaping behavior used
elsewhere, e.g. compare with `shareMessage.JSON()` below which explicitly *un*-escapes after
the fact instead).

### mb — `makeBucketMessage` (`cmd/mb-main.go:95`)

```go
type makeBucketMessage struct {
	Status string `json:"status"`
	Bucket string `json:"bucket"`
	Region string `json:"region"`
}
```
`String()`: `console.Colorize("MakeBucket", "Bucket created successfully `"+s.Bucket+"`.")`
Note: `Region` field exists in the struct but is **never actually set** by the caller
(`printMsg(makeBucketMessage{Status: "success", Bucket: targetURL})` — no `Region:` set), so
it always serializes as `"region": ""`.

### rb — `removeBucketMessage` (`cmd/rb-main.go:79`)

```go
type removeBucketMessage struct {
	Status string `json:"status"`
	Bucket string `json:"bucket"`
}
```
`String()`: `` "Removed `%s` successfully." ``, colorized tag `"RemoveBucket"`.
`JSON()` uses **`json.Marshal`** (not `MarshalIndent`) — i.e. `rb --json` output is
**compact single-line JSON even when stdout is a TTY** (unlike almost every other command).

### rm — `rmMessage` (`cmd/rm-main.go:169`)

```go
type rmMessage struct {
	Status       string     `json:"status"`
	Key          string     `json:"key"`
	DeleteMarker bool       `json:"deleteMarker"`
	VersionID    string     `json:"versionID"`
	ModTime      *time.Time `json:"modTime"`
	DryRun       bool       `json:"dryRun"`
}
```
`String()`:
```go
msg := "Removed "
if r.DryRun { msg = "DRYRUN: Removing " }
if r.DeleteMarker { msg = "Created delete marker " }
msg += console.Colorize("Removed", fmt.Sprintf("`%s`", r.Key))
if r.VersionID != "" {
	msg += fmt.Sprintf(" (versionId=%s)", r.VersionID)
	if r.ModTime != nil {
		msg += fmt.Sprintf(" (modTime=%s)", r.ModTime.Format(printDate))
	}
}
msg += "."
```
Note `ModTime *time.Time` with no `omitempty` — when nil it serializes as JSON `null`.

### cp/mv/put/get — `copyMessage` (`cmd/cp-main.go:202`)

```go
type copyMessage struct {
	Status     string `json:"status"`
	Source     string `json:"source"`
	Target     string `json:"target"`
	Size       int64  `json:"size"`
	TotalCount int64  `json:"totalCount"`
	TotalSize  int64  `json:"totalSize"`
}
```
`String()`: `` console.Colorize("Copy", fmt.Sprintf("`%s` -> `%s`", c.Source, c.Target)) ``.
**`mv` reuses this exact struct and color tag** (`console.SetColor("Copy", ...)` in
`cmd/mv-main.go:218`) — there is no separate "moved" message/verb; JSON output for `mv` is
indistinguishable in shape from `cp`'s (`"status":"success","source":...,"target":...`).
**`put`/`get` also funnel through `doCopy`** (`cmd/cp-main.go`), so they emit the same
`copyMessage` per object, plus the `accountStat` summary (§5) at the end of the session.

Per-object `copyMessage` is only printed **when a progress bar is NOT active**:
```go
if progressReader, ok := copyOpts.pg.(*progressBar); ok {
	progressReader.SetCaption(...)   // interactive: caption updates bar, no printMsg
} else {
	printMsg(copyMessage{Source: sourcePath, Target: targetPath, Size: length})
}
```
See §5 for exactly when that's true.

### mirror — `mirrorMessage` (`cmd/mirror-main.go:288`)

```go
type mirrorMessage struct {
	Status     string                 `json:"status"`
	Source     string                 `json:"source"`
	Target     string                 `json:"target"`
	Size       int64                  `json:"size"`
	TotalCount int64                  `json:"totalCount"`
	TotalSize  int64                  `json:"totalSize"`
	EventTime  string                 `json:"eventTime"`
	EventType  notification.EventType `json:"eventType"` // string-typed enum, e.g. "s3:ObjectRemoved:Delete"
}
```
`String()`:
```go
if m.EventTime != "" { msg = "[<EventTime>] " (colorized "Time") }
switch m.EventType {
case notification.ObjectRemovedDelete:
	return msg + "Removed " + "`"+Target+"`" (colorized "Removed")
case notification.ObjectRemovedDeleteMarkerCreated:
	return msg + "Removed (Delete Marker)" + "`"+Target+"`"
case notification.ILMDelMarkerExpirationDelete:
	return msg + "Removed (ILM)" + "`"+Target+"`"
}
if m.EventTime == "" {
	return "`"+Source+"` -> `"+Target+"`" (colorized "Mirror")   // used for --watch bootstrap/plain copy line
}
msg += "<6-wide humanize.IBytes(Size)> " (colorized "Size")
msg += "`"+Source+"` -> `"+Target+"`" (colorized "Mirror")
```

### du — `duMessage` (`cmd/du-main.go:92`)

```go
type duMessage struct {
	Prefix     string `json:"prefix"`
	Size       int64  `json:"size"`
	Objects    int64  `json:"objects"`
	Status     string `json:"status"`
	IsVersions bool   `json:"isVersions"`
}
```
`String()`: `"<humanSize>\t<N object(s)|version(s)>\t<Prefix>"` (tab-separated, three
`console.Colorize` tags: `Size`, `Objects`, `Prefix`). Pluralization: `"object"`/`"version"`
+ `"s"` unless `Objects == 1`. One `duMessage` is printed **per directory level actually
recursed into** (recursive calls print at every depth boundary reached), not just once at the
end — `du` without `--recursive` therefore emits one line per immediate subdirectory plus the
top-level total.

### tree — `treeMessage` (`cmd/tree-main.go:42`)

```go
type treeMessage struct {
	Entry        string
	IsDir        bool
	BranchString string
}
```
No JSON tags at all (unexported-JSON design — `JSON()` is a hard-fail no-op):
```go
func (t treeMessage) JSON() string {
	fatalIf(probe.NewError(errors.New("JSON() should never be called here")), ...)
	return ""
}
```
**`mc tree --json` never calls this.** `mainTree` special-cases `globalJSON`: when JSON mode
is requested it does **not** call `doTree` at all — it silently redirects to the exact same
code path as `mc ls --recursive --json` (`doList` with `isRecursive: true`), so tree's JSON
output is really `ls`'s `contentMessage`/`summaryMessage` JSON, not a tree-shaped payload.

`String()`: `` fmt.Sprintf("%s%s", t.BranchString, console.Colorize(entryType, t.Entry)) ``
where `entryType` is `"Dir"` or `"File"` (colorize tag name, not printed literally).
Branch-drawing constants:
```go
treeEntry     = "├─ "
treeLastEntry = "└─ "
treeNext      = "│"
treeLevel     = "  "
```
The branch string is built incrementally per recursion level in `doTree`
(`cmd/tree-main.go:141`): closing a level trims the previous entry glyph, appends
`" "+treeLevel` if the parent branch was already closed (`treeLastEntry`) else
`treeNext+treeLevel`, then appends `treeEntry` or `treeLastEntry` depending on whether this
is the last child.

### head / cat / pipe

**head** (`cmd/head-main.go`) and **cat** (`cmd/cat-main.go`) have **no message struct at
all** — they stream raw object bytes straight to `os.Stdout` (wrapped in
`newPrettyStdout(os.Stdout)` when `isTerminal()`, which presumably filters control chars —
otherwise raw `os.Stdout`). `--json`/`--quiet` have **zero effect** on `cat`/`head` output;
there is no success/error JSON envelope for the data itself (errors from the surrounding
command still go through the normal `fatalIf`/`errorIf` machinery on stderr).

**pipe** — `pipeMessage` (`cmd/pipe-main.go:121`):
```go
type pipeMessage struct {
	Status string `json:"status"`
	Target string `json:"target"`
	Size   int64  `json:"size"`
}
```
`String()`: `` console.Colorize("Pipe", fmt.Sprintf("%d bytes -> `%s`", p.Size, p.Target)) ``

### diff — `diffMessage` (`cmd/diff-main.go:73`)

```go
type diffMessage struct {
	Status        string       `json:"status"`
	FirstURL      string       `json:"first"`
	SecondURL     string       `json:"second"`
	Diff          differType   `json:"diff"`
	Error         *probe.Error `json:"error,omitempty"`
	firstContent  *ClientContent  // unexported, not serialized
	secondContent *ClientContent  // unexported, not serialized
}
```
`differType` (`cmd/difference.go:34`) is a plain `int` (`iota`-based enum) with a `String()`
method **but no `MarshalJSON`**:
```go
differInUnknown       differType = iota // 0
differInNone                            // 1  ""
differInSize                            // 2  "size"
differInMetadata                        // 3  "metadata"
differInType                            // 4  "type"
differInFirst                           // 5  "only-in-first"
differInSecond                          // 6  "only-in-second"
differInAASourceMTime                   // 7  "mm-source-mtime"
```
**Gotcha**: because there is no `MarshalJSON`, `"diff"` serializes as a **raw integer**
(0-7) in `diff --json` output, not the human string from `String()`.

`String()` (human mode) prints one of:
```
< <FirstURL>     // differInFirst  — colorize "DiffOnlyInFirst"
> <SecondURL>    // differInSecond — colorize "DiffOnlyInSecond"
! <SecondURL>    // differInType/differInSize/differInMetadata/differInAASourceMTime — colorize "DiffType"/"DiffSize"/"DiffMetadata"/"DiffMMSourceMTime" respectively
= <FirstURL>     // differInNone — colorize "DiffInNone"
```
The command's own doc-comment LEGEND (`cmd/diff-main.go:58`) is slightly imprecise/outdated
about the `!` cases (only documents "newer object is in source" for `!`, but `!` actually
covers type/size/metadata/mm-source-mtime differences too).

### find — `findMessage` (`cmd/find.go:46`)

```go
type findMessage struct {
	contentMessage   // embedded — inherits ALL of contentMessage's fields/json tags verbatim
}
```
```go
func (f findMessage) String() string {
	msg := f.Key
	if f.VersionID != "" { msg += " (" + f.VersionID + ")" }
	return console.Colorize("Find", msg)
}
func (f findMessage) JSON() string { return f.contentMessage.JSON() }
```
So `find`'s JSON output is **byte-for-byte `contentMessage`'s JSON** (same `"type"`,
`"lastModified"`, `"size"`, `"key"`, `"etag"`, `"url"`, version fields, etc.) — there is no
find-specific JSON shape, only the string rendering differs (bare key, optionally
` (<versionID>)`, no size/time/colorized dir/file distinction).

### share — `shareMessage` (`cmd/share.go:54`) — shared by download/upload/list subcommands

```go
type shareMessage struct {
	Status      string        `json:"status"`
	ObjectURL   string        `json:"url"`
	ShareURL    string        `json:"share"`
	TimeLeft    time.Duration `json:"timeLeft"`
	ContentType string        `json:"contentType,omitempty"` // upload only
}
```
`String()`:
```
URL: <ObjectURL>\n            (colorize "URL")
Expire: <humanized TimeLeft>\n  (colorize "Expire")
Content-Type: <ContentType>\n   (colorize "Content-type", only if non-empty; upload only)
Share: <ShareURL with <FILE>/<NAME> placeholders highlighted>\n  (colorize "Share")
```
`TimeLeft` is a `time.Duration` — JSON-marshals as a **raw int64 nanoseconds number**, not a
duration string (same class of gotcha as `accountStat.Duration` below).
`JSON()` additionally un-escapes `&`→`&`, `<`→`<`, `>`→`>` post-marshal
(colorjson/encoding-json HTML-escapes by default; share manually reverses it because the URL
needs literal `&`).

### ping — `PingResult` / `PingSummary` (`cmd/ping.go`)

```go
type EndPointStats struct {
	Endpoint *url.URL `json:"endpoint"`
	DNS      string   `json:"dns"`
	Status   string   `json:"status,omitempty"`
	Error    string   `json:"error,omitempty"`
	Time     string   `json:"time"`
}
type PingResult struct {
	Status         string          `json:"status"`
	Counter        string          `json:"counter"`
	EndPointsStats []EndPointStats `json:"servers"`
}
type PingSummary struct {
	Status    string                  `json:"status"`
	ServerMap map[string]ServerStats  `json:"serverMap"`
}
```
`PingResult.String()` renders via a `text/template` (`Ping`/`PingDist` templates,
`cmd/ping.go:127-131`) piped through `text/tabwriter`: one line per endpoint,
`N: scheme://host\tstatus=<ok |...>\ttime=<dur>`, colored white if `Status == "ok "` (note:
literal `"ok "` with a trailing space is the sentinel) else red. `PingDist` is used instead of
`Ping` whenever `len(EndPointsStats) > 1` (distributed ping) — same template body, just a
different Go `template.Template` instance, output is identical since the two constant
strings (`Ping`, `PingDist`) are byte-identical in this version.
`PingSummary.String()` renders a bordered table (`console.NewTable`) with columns
`Endpoint | Min | Avg | Max | Error | Count`.

### ready — `readyMessage` (`cmd/ready-main.go:76`)

```go
type readyMessage struct {
	Status          string `json:"status"`
	Alias           string `json:"alias"`
	Healthy         bool   `json:"healthy"`
	MaintenanceMode bool   `json:"maintenanceMode"`
	WriteQuorum     int    `json:"writeQuorum"`
	HealingDrives   int    `json:"healingDrives"`
	Err             error  `json:"error"`
}
```
`String()`:
```go
switch {
case r.Healthy:  return color.GreenString("The cluster '%s' is ready", r.Alias)
case r.Err != nil: return color.RedString("The cluster '%s' is unreachable: %s", r.Alias, r.Err.Error())
default:         return color.RedString("The cluster '%s' is not ready", r.Alias)
}
```
Note this uses raw `github.com/fatih/color.GreenString`/`RedString` directly, **not**
`console.Colorize` — so `ready`'s coloring is not gated by `console.SetColorOff()` the same
way (it still respects the global `color.NoColor` switch that `fatih/color` maintains
internally, which IS toggled elsewhere, but it's a different code path worth flagging).
`ready` polls in a loop and calls `printMsg` **every interval**, even when nothing changed,
until healthy or ctx canceled — so plain (non-JSON) `mc ready` streams repeated lines, not
just a final verdict.

---

## 3. Exit codes

Constants (`cmd/globals.go:58-68`):
```go
globalErrorExitStatus   = 1     // generic/any error
globalCancelExitStatus  = 130   // SIGINT (Ctrl-C)   = 128+2
globalKillExitStatus    = 137   // SIGKILL           = 128+9
globalTerminatExitStatus = 143  // SIGTERM           = 128+15
```

Mechanism: command `Action` funcs return `exitStatus(N)` which is
`cli.NewExitError("", N)` (`cmd/error.go:126`) — an `ExitCoder`. The vendored `minio/cli`
library's `App.Run()` intercepts any returned error implementing `ExitCoder` via
`HandleExitCoder` (`github.com/minio/cli` `errors.go`):
```go
func HandleExitCoder(err error) {
	if exitErr, ok := err.(ExitCoder); ok {
		if err.Error() != "" { fmt.Fprintln(ErrWriter, err) }
		OsExiter(exitErr.ExitCode())   // os.Exit(N) — called from INSIDE Run(), never returns
		return
	}
	...
}
```
Since `exitStatus(N)` always uses an **empty message** (`cli.NewExitError("", status)`), this
path prints nothing extra — all the actual error text was already printed via `errorIf`
earlier; `exitStatus()` purely carries the numeric code.

Consequences:
- **Batch/partial failure** (e.g. `mc cp a b c s3/bucket/` where one of several files fails,
  or `mc mirror` hitting per-object errors): the loop accumulates
  `cErr = exitStatus(globalErrorExitStatus)` on each failure but **keeps processing the rest**
  (`continue`), and returns that single `exitStatus(1)` at the end. There is **no way to
  distinguish "some objects failed" from "one object failed" from "total failure"** via exit
  code — it's always **1** if *any* object failed, `0` otherwise. rs3 should replicate this:
  exit code is boolean-ish (0 = fully clean, 1 = at least one error), not a count or severity.
- **Ctrl-C / SIGTERM / SIGKILL**: handled by `trapSignals` (`cmd/signals.go`), which cancels
  the global context and then itself calls `os.Exit(130|137|143|1)` based on the signal
  string (`"interrupt"`→130, `"killed"`→137, `"terminated"`→143, default→1). This exit path is
  independent of any in-flight command's own return value.
- **Usage errors** (`onUsageError`) and **help-with-bad-args**
  (`showCommandHelpAndExit(ctx, 1)`, used pervasively, e.g. `rm`/`mb`/`rb`/`diff`/`ready`/`get`
  when required args are missing) exit with `os.Exit(1)` directly (bypassing the `ExitCoder`
  return-value path — they call `os.Exit` themselves after the pager exits).
- **Unrecognized top-level command** (`commandNotFound`) also ends in `exitStatus(1)` via the
  app's default `Action`.
- If `mc.Main()` returns a *non*-`ExitCoder`, *non*-nil error some other way (rare — cli's
  `Run()` mostly already turned things into `ExitCoder`s or already called `os.Exit`), `main.go`
  does `console.Fatalln(e)` which is *also* a hard `os.Exit(1)`.
- **Successful run**: process falls off the end of `Run()`/`Main()` with `nil` error → exit 0
  (implicit, standard Go `main()` return).

---

## 4. `--quiet` and `--no-color` semantics

### `--quiet` / `-q` (usage string: *"disable progress bar display"*, env `MC_QUIET`)

`--quiet` is emphatically **not** a "suppress all output" flag. Its actual, narrow effect:

1. Disables the `cheggaaa/pb` progress bar for `cp`/`mv`/`put`/`get`/`mirror` — they fall back
   to an `*accounter` (byte counter with no visual rendering) instead of a `*progressBar`:
   ```go
   if !globalQuiet && !globalJSON {
       pg = newProgressBar(totalBytes)
   } else {
       pg = newAccounter(totalBytes)
   }
   ```
2. **As a side effect of (1), per-object messages that were suppressed while the progress bar
   owned the line now get printed instead.** For `cp`/`mv`/`put`/`get`:
   ```go
   if progressReader, ok := copyOpts.pg.(*progressBar); ok {
       progressReader.SetCaption(...)          // interactive: no printMsg per object
   } else {
       printMsg(copyMessage{...})               // quiet or json: ONE LINE PER OBJECT
   }
   ```
   For `mirror`, same idea via the `Status` interface: `ProgressStatus.PrintMsg` is a no-op,
   `QuietStatus.PrintMsg` calls `printMsg(msg)` — and `NewQuietStatus` is selected whenever
   `globalQuiet || opts.isSummary || globalJSON`.
3. At the end of the session, if nothing errored, the `*accounter`'s `Stat()` summary
   (`accountStat` — Total/Transferred/Duration/Speed) is printed via `printMsg` — **this
   happens in quiet mode too**, both for `cp`/`put`/`get` (`showLastProgressBar`) and for
   `mirror`/anything using `Status.Finish()` (`QuietStatus.Finish()` calls
   `printMsg(qs.Stat())`; `ProgressStatus.Finish()` just finishes the bar, prints nothing).

   **Net effect: `--quiet` on `cp`/`mirror`/etc. is actually *more verbose* line-count-wise
   than the default (interactive) mode** — you get one `copyMessage`/`mirrorMessage` line per
   object PLUS a final stat-summary table, instead of just an animated progress bar with no
   persisted per-object output.
4. `console.SetColorOff()` is also triggered by `--quiet` (see globals.go: `if globalNoColor
   || globalQuiet { console.SetColorOff(); ... }`) — so `--quiet` implicitly disables color
   too, same as `--no-color`.
5. `du`'s `-q`/`ls`'s output etc. that don't use a progress bar are unaffected by `--quiet` in
   any special way beyond the ambient color-off.
6. mc's own auto-update check (`mainStart`/`app.Action`, `cmd/main.go:401`) is skipped when
   quiet: `if !ctx.Bool("quiet") ...`.

### `--no-color` (usage: *"disable color theme"*, env `MC_NO_COLOR`)

Purely cosmetic: calls `console.SetColorOff()` and `lipgloss.SetColorProfile(termenv.Ascii)`.
All `console.Colorize(tag, data)` calls become plain `fmt.Sprint(data)` (see `console.go`:
`Colorize` checks `isatty.IsTerminal(os.Stdout.Fd())` **and** internally consults the color
package's global "off" switch set by `SetColorOff`). Structural output (field order, JSON
shape, line content) is unchanged — only ANSI escape sequences are stripped. Also implicitly
triggered by `--json` when stdout is not a TTY (`globalJSONLine` implies `globalNoColor`), and
by `--quiet` (see above).

---

## 5. Progress bar

- Library: `github.com/cheggaaa/pb` (v1, not v3) — wrapped by `cmd/progress-bar.go`'s
  `progressBar` type (`*pb.ProgressBar` embedded).
- Construction (`newPB`, `cmd/progress-bar.go:36`): `pb.New64(total)`, units
  `pb.U_BYTES`, refresh rate `125ms`, `bar.NotPrint = true` (manual printing via
  `bar.Callback`), `bar.ShowSpeed = true`. Custom render glyphs per OS:
  - linux: `┃▓█░┃`
  - darwin: ` ▓ ░ ` (with leading/trailing spaces, i.e. no border char)
  - default (incl. Windows): ASCII `[=> ]`
  Callback prints `console.Print(console.Colorize("Bar", "\r"+s))`.
- **When shown**: exactly when `!globalQuiet && !globalJSON` for the transfer commands
  (`cp`, `mv`, `put`, `get`, `mirror`). This is a static per-invocation decision based on the
  two flags — **not an isatty check on the progress-bar code path itself** (though recall
  `globalQuiet` gets auto-forced-true on non-Windows when stdout size can't be read, which
  indirectly makes it TTY-aware — see §1.2). So piping mc's stdout on Linux/macOS effectively
  disables the bar even without `-q`, but on Windows it would still try to render it unless
  `-q`/`--json` is explicit.
- `--json` disables the bar exactly like `--quiet` (both fall into the `*accounter` branch);
  it does **not** get any special animated/streaming JSON progress — only the discrete
  `copyMessage`/`mirrorMessage`-per-object plus final `accountStat` JSON, same as quiet mode
  minus colorization/table formatting differences (JSON always machine-shaped regardless of
  quiet vs json).
- `console.Eraseline()` (`cmd/print.go`... actually `console` package, used pervasively in
  `cp-main.go`/`get-main.go`/`put-main.go`/`status.go`) clears the current line and repositions
  the cursor before printing an error/log line over an active bar — but only called guarded by
  `if !globalQuiet && !globalJSON`, i.e. purely a bar-cleanup affordance, irrelevant when the
  bar isn't showing.
- Final summary struct, `accountStat` (`cmd/accounting-reader.go:86`):
  ```go
  type accountStat struct {
	  Status      string        `json:"status"`
	  Total       int64         `json:"total"`
	  Transferred int64         `json:"transferred"`
	  Duration    time.Duration `json:"duration"`
	  Speed       float64       `json:"speed"`
  }
  ```
  `Duration` is a `time.Duration` with **no custom marshaling** → serializes as a **raw int64
  nanosecond count** in JSON (same footgun class as `shareMessage.TimeLeft`). Human `String()`
  renders a 4-column bordered table (`Total | Transferred | Duration | Speed`) using
  `pb.Format(...).To(pb.U_BYTES)`/`pb.U_DURATION` for human-friendly units and `<speed> MB/s`
  suffix (falls back to literal `"0 MB"` if speed formats to empty string).

---

## Gotchas for a reimplementer

1. **JSON envelope asymmetry**: success messages are the message struct itself (top-level,
   `"status":"success"` mixed in among the domain fields); errors are always wrapped in
   `{"status":"error","error":{...}}` — there is no single shared envelope shape between
   success and failure. Consumers parsing rs3's `--json` output need `status` to disambiguate
   which shape to expect next.
2. **Indent inconsistency**: nearly everything uses `json.MarshalIndent(x, "", " ")` (one
   space), but `summaryMessage.JSON()` (ls --summarize) uses `""` indent, and
   `removeBucketMessage.JSON()` (rb) uses plain `json.Marshal` (fully compact, single line,
   even in interactive/TTY mode). If exact byte-for-byte diffing against real `mc` output is a
   goal, don't assume a single global "pretty vs line" JSON policy — it's per-message-type.
3. **`--quiet` ≠ silent**: it repurposes per-object lines from "consumed by the progress bar
   caption" to "printed via `printMsg`", and still prints a final `accountStat`/`Stat()`
   summary table. Don't implement `--quiet` as "suppress transfer output" — implement it as
   "no animated bar, but do emit one message per object + a final summary", matching
   `cp`/`get`/`put`/`mirror`'s `pg`/`Status` branching exactly.
4. **`--json` auto-collapses to single-line only off-TTY**: `globalJSONLine = !isTerminal() &&
   json`. An interactive terminal with `--json` still gets indented multi-line JSON per
   message (one JSON blob per `printMsg` call, not one per line) — only when piped does it
   become true JSON-Lines (one compact JSON object per output line). rs3 needs to replicate
   the TTY-detection branch, not just always emit either pretty or compact.
5. **Non-Windows non-TTY stdout silently forces `--quiet`** even if the user never passed the
   flag, whenever `term.GetSize(stdout)` fails (typical for pipes/redirects). This is separate
   from the `--json`-driven `globalJSONLine` TTY check and easy to miss.
6. **`diff --json`'s `"diff"` field is a bare integer** (`differType` has no `MarshalJSON`,
   just a `String()` used only by the human renderer) — values 0–7 per the `iota` order
   documented above, not the human-readable strings (`"only-in-first"` etc.) that appear in
   the LEGEND/help text.
7. **`time.Duration` fields marshal as raw nanosecond integers**, not human strings or ISO
   durations — affects `accountStat.Duration` (cp/get/put/mirror summary) and
   `shareMessage.TimeLeft` (`share upload`/`share download`). Only the `String()`/human path
   humanizes them (`pb.Format(...).To(pb.U_DURATION)` / a custom
   `timeDurationToHumanizedDuration` helper).
8. **`mv` is literally `cp` with a flag** (`isMvCmd bool`) — same `copyMessage` struct, same
   `"Copy"` color tag, same JSON shape. There is no `"action":"move"` discriminator anywhere;
   a JSON consumer cannot tell `mv` output from `cp` output except by having invoked one or
   the other.
9. **`tree --json` doesn't emit tree-shaped JSON at all** — `treeMessage.JSON()` is a
   deliberate `fatalIf`-panic that should never be reached, because `mainTree` reroutes the
   entire `--json` case to `doList(..., isRecursive: true, ...)`, i.e. plain `ls -r --json`
   (`contentMessage`/`summaryMessage`). A faithful rs3 `tree --json` must literally alias to
   its own `ls --recursive --json` implementation, not attempt to serialize the tree
   structure.
10. **`find`'s JSON is exactly `contentMessage`'s JSON** (embedding, not a distinct schema) —
    only `String()` differs (bare key + optional `(versionID)`), so `find --json` fields
    (`type`, `lastModified`, `size`, `key`, `etag`, ...) must match `ls --json` exactly, field
    for field.
11. **`<ERROR>` is used for both fatal and non-fatal errors** — there is no `<FATAL>` tag to
    key off of in the human-mode prefix; only the JSON mode's `errorMessage.Type` field
    (`"fatal"` vs `"error"`) distinguishes fatal-and-exiting from just-an-error-continuing.
12. **The `<ERROR>`/`<DEBUG>` prefix carries the *program's own argv[0] basename*, not a fixed
    string** `"mc"` — if rs3's binary is invoked as something other than `mc` (e.g. `rs3`),
    strict byte-for-byte compatibility requires either hardcoding `"mc"` or mirroring the same
    `os.Args[0]`-basename derivation (decide deliberately; don't accidentally print `rs3:
    <ERROR>` if `mc:` output is contractually expected by scripts).
13. **Exit codes are boolean per invocation, not per-object**: any failure anywhere in a
    multi-target/recursive operation yields the same exit code `1` as a single total failure.
    Signal-based exits (130/137/143) only occur via the top-level signal trap or the
    ping-command's own local mirror of the same numbers — command `Action`s never return those
    codes themselves.
14. **`cat`/`head` are completely outside the message/printMsg system.** They write raw bytes
    (optionally control-char-filtered via `newPrettyStdout` when stdout is a TTY) directly to
    `os.Stdout`; `--json`/`--quiet`/`--no-color` have no effect on the payload, and there is no
    JSON success confirmation for a `cat`/`head` invocation — only surrounding fatal errors
    (missing object, etc.) go through the normal error machinery.
15. **`ready`'s color uses `fatih/color.GreenString`/`RedString` directly**, bypassing
    `console.Colorize`/theme tags — worth checking against `--no-color` behavior specifically
    if replicating exact ANSI-stripping semantics, since it's not gated identically to every
    other command's colorization.
16. **HTML-escaping of JSON is inconsistent**: default `colorjson.MarshalIndent` HTML-escapes
    (`<`,`>`,`&`); `shareMessage.JSON()` manually un-escapes `&`/`<`/`>`
    afterward (because share URLs contain literal `&`), while `bucketInfoMessage.JSON()`
    instead calls `enc.SetEscapeHTML(false)` up front. Most other message types never emit
    those characters so the default escaping never surfaces — but any rs3 message containing
    URLs with query strings needs one of these two treatments to match real `mc`.
17. **`rmMessage.ModTime` is `*time.Time` without `omitempty`** — expect a literal JSON `null`
    for plain (non-versioned) deletes, not a missing key.
18. **`makeBucketMessage.Region` is always emitted but always empty** (`""`) in practice — the
    field exists in the struct/JSON but the code path that constructs it never sets it.
