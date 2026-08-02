//! `diff` ([SEM] §9, [OUT] §2's `diffMessage`): compares two directory-like
//! targets (local dir or S3 alias/bucket/prefix, any combination on either
//! side) by name + size + an mtime heuristic -- never content. Reuses
//! `mirror`'s `Side`/`resolve_side` operand resolution and `Entry`
//! collection (`collect_local_entries`/`collect_s3_entries`) rather than
//! re-implementing either, since both already produce the sorted
//! `Vec<Entry>` shape `differenceInternal`'s merge-join needs.
//!
//! Unlike `mirror`, diffing two local paths against each other is valid
//! real `mc` usage -- ground-truth verified against the real binary
//! (RELEASE.2025-08-13): "Compare two folders on a local filesystem" is one
//! of `diff --help`'s own examples, and `mc diff ~/a ~/b` (both local)
//! succeeds. So this module does not carry `mirror::run_mirror`'s
//! local-to-local rejection.

use std::cmp::Ordering;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};

use crate::messages::DiffMessage;
use crate::mirror::{Entry, Side, collect_local_entries, collect_s3_entries, resolve_side};
use crate::output::print_msg;

// Raw `differType` int codes ([OUT] §2 gotcha 6) -- `differInMetadata` (3)
// and `differInType` (4) are never produced here: rs3 has no `--metadata`
// diff mode (that's `mirror --a`, out of scope for plain `diff`) and no
// directory-vs-file `Entry` variant (every `Entry` is a plain object/file),
// so a type mismatch is structurally unreachable.
const DIFF_SIZE: i64 = 2;
const DIFF_ONLY_IN_FIRST: i64 = 5;
const DIFF_ONLY_IN_SECOND: i64 = 6;
const DIFF_MM_SOURCE_MTIME: i64 = 7;

/// User-facing display URL for one entry on `side`: `alias/bucket/key` for
/// S3, the joined filesystem path for local -- matches every other
/// transfer command's own convention in this codebase (e.g.
/// `mirror::copy_entry`'s `target_display`/`source_display`). This is a
/// deliberate deviation from real `mc`'s `diff`, which prints the
/// fully-resolved raw endpoint URL instead of the alias (ground-truth
/// verified: `mc diff myalias/a myalias/b` prints
/// `http://host:port/a/x.txt`, never `myalias/a/x.txt`, regardless of
/// whether the alias was set via `mc alias set` or a one-shot `MC_HOST_*`
/// env var) -- the task brief calls for the aliased/local-path form here,
/// consistent with the rest of rs3's output and easier to assert on in
/// tests (real endpoint hosts/ports are ephemeral in the test harness).
fn display_url(side: &Side, rel: &str) -> String {
    match side {
        Side::Local(root) => root.join(rel).display().to_string(),
        Side::S3 {
            alias_name,
            bucket,
            prefix,
            ..
        } => format!(
            "{alias_name}/{bucket}/{}",
            crate::urls::join_key(prefix, rel)
        ),
    }
}

/// Collects one side's sorted entries. `required` mirrors mc's asymmetric
/// `checkDiffSyntax` ([SEM] §9): the SOURCE side must resolve to a real
/// directory (fatal otherwise), the TARGET side tolerates being missing
/// entirely (treated as an empty listing -- every source entry then reports
/// only-in-first). Ground-truth verified against real `mc`:
/// - local path that doesn't exist at all: fatal on either side (both mc
///   and rs3 -- there's no "empty listing" concept for a nonexistent local
///   directory).
/// - S3 target prefix with zero objects under an *existing* bucket: valid
///   empty listing, `exit 0`.
/// - S3 bucket that doesn't exist at all: fatal on *either* side (a
///   different underlying error than "prefix is empty" -- mc's
///   `checkDiffSyntax` only swallows the specific `ObjectMissing` case).
///   `collect_s3_entries` naturally reproduces this: it calls
///   `ListObjectsV2`, which errors with `NoSuchBucket` for a missing bucket
///   (propagated here as `Err`) but succeeds with zero entries for an
///   empty-but-existing prefix.
///
/// One narrower real-`mc` case is *not* reproduced: a SOURCE prefix that is
/// zero objects under an otherwise-existing bucket is fatal in real `mc`
/// ("Object does not exist") but succeeds here as an empty listing, same as
/// the target side would. Reproducing that exactly would need a dedicated
/// "does at least one object exist under this prefix" existence probe that
/// no other rs3 command (`mirror` included) has; not exercised by the e2e
/// suite, so left as a documented simplification rather than new machinery.
async fn collect_entries(
    side: &Side,
    required: bool,
    dispatch_ctx: Option<(
        &crate::budget::StreamBudget,
        Option<&crate::progress::ProgressUi>,
    )>,
) -> Result<Vec<Entry>> {
    match side {
        Side::Local(root) => {
            if root.is_dir() {
                {
                    // Same slot grid the S3 side's listing dispatches into:
                    // a local tree walk is work the user is waiting on, so
                    // it is a task like any other rather than dead air.
                    let scan = match dispatch_ctx.and_then(|(_, ui)| ui) {
                        Some(ui) => ui.start(crate::progress::ProgressAwareTask::count(
                            crate::progress::TransferLabel {
                                verb: crate::progress::Verb::Scanning,
                                path: root.display().to_string(),
                                part: None,
                            },
                            0,
                            "{done}/{total} dirs",
                        )),
                        None => crate::progress::ProgressNotifier::noop(),
                    };
                    // `diff` reports, it does not modify: no staging reclaim here.
                    let entries = collect_local_entries(root, &scan, false).await;
                    scan.finish();
                    entries
                }
            } else if required {
                Err(anyhow!("`{}` is not a folder.", root.display()))
            } else {
                Ok(Vec::new())
            }
        }
        Side::S3 {
            client,
            bucket,
            prefix,
            ..
        } => collect_s3_entries(client, bucket, prefix, dispatch_ctx).await,
    }
}

/// The raw-mtime fallback path of mc's `activeActiveModTimeUpdated`
/// ([SEM] §9 step 3): `srcActualModTime.After(dstActualModTime)`. rs3 has
/// no `X-Amz-Meta-Mm-Source-Mtime` metadata plumbing on either side, so the
/// `X-Amz-Meta-Mm-Source-Mtime`-aware branch of the real function is never
/// reachable here -- only this plain-timestamp fallback is. A missing
/// timestamp on either side mirrors `src.Time.IsZero() ||
/// dst.Time.IsZero()` short-circuiting to `false` (no diff emitted).
/// Ground-truth verified: two same-size objects where the *target* was
/// (re-)uploaded after the source never emit a diff line at all -- only a
/// strictly-newer *source* does, matching the strict `>` here (not `>=`).
fn source_mtime_newer(source: Option<DateTime<Utc>>, target: Option<DateTime<Utc>>) -> bool {
    matches!((source, target), (Some(s), Some(t)) if s > t)
}

fn emit_only_in_first(source: &Side, entry: &Entry) {
    print_msg(&DiffMessage {
        first: display_url(source, &entry.rel),
        second: String::new(),
        diff: DIFF_ONLY_IN_FIRST,
    });
}

fn emit_only_in_second(target: &Side, entry: &Entry) {
    print_msg(&DiffMessage {
        first: String::new(),
        second: display_url(target, &entry.rel),
        diff: DIFF_ONLY_IN_SECOND,
    });
}

/// `diff FIRST SECOND`: no flags of its own ([SEM] §9 -- `diffFlags =
/// []cli.Flag{}` beyond the CLI globals). Exit code is always `0` when
/// differences are found -- mc's `mainDiff` returns `nil` unconditionally
/// once syntax-checking passes; differences are normal output, not an
/// error condition (ground-truth verified: `mc diff` on two buckets with
/// every kind of difference present still exits `0`).
pub(crate) async fn run_diff(first: &str, second: &str) -> Result<()> {
    let source = resolve_side(first).await?;
    let target = resolve_side(second).await?;

    let ui = crate::progress::worker_ui(5);
    let budget = crate::budget::StreamBudget::new(5);
    let dispatch_ctx = Some((&budget, ui.as_ref()));
    let source_entries = collect_entries(&source, true, dispatch_ctx).await?;
    let target_entries = collect_entries(&target, false, dispatch_ctx).await?;

    // Sorted merge-join over both entry lists (mc's `differenceInternal`,
    // [SEM] §9), comparing by relative key (`Entry::rel`) -- both lists are
    // already sorted lexicographically by `collect_local_entries`/
    // `collect_s3_entries`.
    let mut i = 0usize;
    let mut j = 0usize;
    while i < source_entries.len() && j < target_entries.len() {
        let s = &source_entries[i];
        let t = &target_entries[j];
        match s.rel.cmp(&t.rel) {
            Ordering::Less => {
                emit_only_in_first(&source, s);
                i += 1;
            }
            Ordering::Greater => {
                emit_only_in_second(&target, t);
                j += 1;
            }
            Ordering::Equal => {
                if s.size != t.size {
                    print_msg(&DiffMessage {
                        first: display_url(&source, &s.rel),
                        second: display_url(&target, &t.rel),
                        diff: DIFF_SIZE,
                    });
                } else if source_mtime_newer(s.modified, t.modified) {
                    print_msg(&DiffMessage {
                        first: display_url(&source, &s.rel),
                        second: display_url(&target, &t.rel),
                        diff: DIFF_MM_SOURCE_MTIME,
                    });
                }
                // Else: equal in every dimension `diff` checks -- no line
                // emitted (`differInNone` is never printed by plain `diff`,
                // [SEM] §9 step 5).
                i += 1;
                j += 1;
            }
        }
    }
    for s in &source_entries[i..] {
        emit_only_in_first(&source, s);
    }
    for t in &target_entries[j..] {
        emit_only_in_second(&target, t);
    }

    Ok(())
}
