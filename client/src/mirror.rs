use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

use crate::messages::{MirrorMessage, TransferSession};
use crate::progress::{ui_eprintln, ui_println};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Entry {
    pub rel: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
pub(crate) struct MirrorPlan {
    pub copies: Vec<Entry>,
    pub deletes: Vec<String>,
}

pub(crate) fn plan_mirror(
    source: &[Entry],
    target: &[Entry],
    overwrite: bool,
    remove: bool,
) -> MirrorPlan {
    let target_map: BTreeMap<&str, &Entry> = target.iter().map(|e| (e.rel.as_str(), e)).collect();
    let source_set: HashSet<&str> = source.iter().map(|e| e.rel.as_str()).collect();
    let copies = source
        .iter()
        .filter(|s| {
            if overwrite {
                return true;
            }
            match target_map.get(s.rel.as_str()) {
                None => true,
                Some(t) => {
                    t.size != s.size
                        || match (s.modified, t.modified) {
                            (Some(sm), Some(tm)) => sm > tm,
                            _ => true,
                        }
                }
            }
        })
        .cloned()
        .collect();
    let deletes = if remove {
        target
            .iter()
            .filter(|t| !source_set.contains(t.rel.as_str()))
            .map(|t| t.rel.clone())
            .collect()
    } else {
        Vec::new()
    };
    MirrorPlan { copies, deletes }
}

/// Walks `root` for files to mirror.
///
/// Reports to `scan` on two axes, because the walk knows two different
/// things:
///
/// - **The bar counts directories.** It is breadth-first over a queue, so
///   at any moment "total" is every directory discovered so far and "done"
///   is every one already read -- and finding a subdirectory grows the
///   total mid-walk, which is what [`ProgressNotifier::set_total`] is for.
///   Directories are the only axis with a real denominator here.
/// - **The detail text counts matched files**, the number the user actually
///   cares about, which has no knowable total until the walk ends.
///
/// Pass [`ProgressNotifier::noop`](crate::progress::ProgressNotifier::noop)
/// to walk silently.
///
/// `reclaim` lets a walk delete abandoned staging directories as it passes
/// them ([`reclaim_staging_dir`](crate::transfer::reclaim_staging_dir)).
/// Only commands that already write should pass `true` -- `diff` reads, and
/// a read must not have side effects on the tree it is reporting about.
pub(crate) async fn collect_local_entries(
    root: &Path,
    scan: &crate::progress::ProgressNotifier,
    reclaim: bool,
) -> Result<Vec<Entry>> {
    use std::collections::VecDeque;
    let mut entries = Vec::new();
    let mut dirs = VecDeque::from([root.to_path_buf()]);
    let mut discovered = 1u64;
    scan.set_total(discovered);
    while let Some(dir) = dirs.pop_front() {
        let mut rd = tokio::fs::read_dir(&dir)
            .await
            .with_context(|| format!("read {}", dir.display()))?;
        while let Some(item) = rd.next_entry().await? {
            let path = item.path();
            let meta = item.metadata().await?;
            if meta.is_dir() {
                // rs3's own download staging is not content, and never
                // descended into -- see `is_staging_dir_name`. A walk that
                // is allowed to write also reclaims the abandoned ones,
                // which is the only garbage collection they ever get.
                let name = item.file_name();
                if name
                    .to_str()
                    .is_some_and(crate::transfer::is_staging_dir_name)
                {
                    if reclaim {
                        crate::transfer::reclaim_staging_dir(&path, &name.to_string_lossy()).await;
                    }
                    continue;
                }
                dirs.push_back(path);
                discovered += 1;
                scan.set_total(discovered);
            } else if meta.is_file() {
                let rel = path
                    .strip_prefix(root)?
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                entries.push(Entry {
                    rel,
                    size: meta.len(),
                    modified: meta.modified().ok().map(DateTime::<Utc>::from),
                });
            }
        }
        scan.advance(1);
        scan.set_detail(format!("{} files", entries.len()));
    }
    entries.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(entries)
}

pub(crate) async fn collect_s3_entries(
    client: &Client,
    bucket: &str,
    prefix: &str,
    dispatch_ctx: Option<(
        &crate::budget::StreamBudget,
        Option<&crate::progress::ProgressUi>,
    )>,
) -> Result<Vec<Entry>> {
    let objects = crate::list::collect_objects_with(client, bucket, prefix, dispatch_ctx).await?;
    // S3 prefix matching is a raw string match, so a listing prefix of `p`
    // (no trailing slash) also matches sibling keys like `p2/x.txt`. Guard
    // the boundary the same way `remove_prefix` does in main.rs before
    // stripping, so a mirror of `bucket/p` never picks up `p2/...` objects.
    let boundary_safe = prefix.is_empty() || prefix.ends_with('/');
    let descendant_prefix = format!("{prefix}/");
    let mut entries: Vec<Entry> = objects
        .into_iter()
        .filter(|o| boundary_safe || o.key == prefix || o.key.starts_with(&descendant_prefix))
        .filter_map(|o| {
            let rel = o
                .key
                .strip_prefix(prefix)
                .unwrap_or(&o.key)
                .trim_start_matches('/')
                .to_string();
            if rel.is_empty() {
                return None;
            }
            Some(Entry {
                rel,
                size: o.size,
                modified: o.modified,
            })
        })
        .collect();
    entries.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(entries)
}

/// Resolved local-vs-S3 identity for one side of a `mirror`/`diff` operand.
/// `pub(crate)` so `diff.rs` can reuse `resolve_side`'s exists()/`is_s3_url`
/// disambiguation and the same display-URL fields (`alias_name`/`bucket`/
/// `prefix`) instead of re-parsing the operand itself.
// The S3 variant is the larger one by ~248 bytes (it carries a resolved
// `Alias`), but exactly two of these are constructed per invocation -- one per
// operand -- so boxing would trade a pointer chase for no measurable saving.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Side {
    Local(std::path::PathBuf),
    S3 {
        client: Client,
        alias: crate::config::Alias,
        alias_name: String,
        bucket: String,
        prefix: String,
    },
}

pub(crate) async fn resolve_side(spec: &str) -> Result<Side> {
    let path = Path::new(spec);
    if path.exists() || !crate::urls::is_s3_url(spec) {
        return Ok(Side::Local(path.to_path_buf()));
    }
    let url = crate::urls::parse_s3_url(spec)?;
    let bucket = url
        .bucket
        .clone()
        .ok_or_else(|| anyhow!("bucket is required in `{spec}`"))?;
    let (client, alias) = crate::config::client_for_alias(&url.alias).await?;
    Ok(Side::S3 {
        client,
        alias,
        alias_name: url.alias,
        bucket,
        prefix: url.key.unwrap_or_default(),
    })
}

fn s3_key(prefix: &str, rel: &str) -> String {
    crate::urls::join_key(prefix, rel)
}

pub(crate) async fn run_mirror(args: &crate::MirrorArgs) -> Result<()> {
    use futures::stream::{self, StreamExt};

    // Validate filter grammar up front, so a bad spec is fatal before any
    // listing/transfer/delete work starts (matches cp/rm).
    crate::timefilter::validate_time_filters(
        args.older_than.as_deref(),
        args.newer_than.as_deref(),
    )?;
    let part_size = crate::urls::parse_size(&args.part_size)?;
    let parallel = args.parallel.max(1);
    let stream_budget = crate::budget::StreamBudget::new(parallel);
    let dry_run = args.dry_run || args.fake;
    let attrs = match args.attr.as_deref() {
        Some(spec) => crate::attr::parse_attrs(spec)?,
        None => BTreeMap::new(),
    };
    if args.preserve && !cfg!(unix) {
        return Err(anyhow!("--preserve is not supported on this platform"));
    }

    let source = resolve_side(&args.source).await?;
    let target = resolve_side(&args.target).await?;

    if matches!((&source, &target), (Side::Local(_), Side::Local(_))) {
        return Err(anyhow!("mirror between two local paths is not supported"));
    }
    if !attrs.is_empty() && matches!((&source, &target), (Side::S3 { .. }, Side::S3 { .. })) {
        return Err(anyhow!("--attr for S3-to-S3 copies is not implemented yet"));
    }

    // ONE display for the whole invocation -- built here, before planning,
    // and handed to the `TransferSession` below rather than replaced by a
    // second one. Planning's listing/scan tasks and the transfers that
    // follow claim slots from the same fixed grid, so the block of rows on
    // screen never changes shape between phases; a second `ProgressUi`
    // would mean a second `MultiProgress` building a fresh grid under the
    // first one's cursor accounting.
    let ui = crate::progress::transfer_ui(parallel);
    let dispatch_ctx = Some((&stream_budget, ui.as_ref()));
    let scan_task = |root: &Path| match &ui {
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
    let source_entries = match &source {
        Side::Local(root) => {
            if !root.is_dir() {
                return Err(anyhow!(
                    "mirror source `{}` is not a directory",
                    root.display()
                ));
            }
            let scan = scan_task(root);
            let entries = collect_local_entries(root, &scan, true).await?;
            scan.finish();
            entries
        }
        Side::S3 {
            client,
            bucket,
            prefix,
            ..
        } => collect_s3_entries(client, bucket, prefix, dispatch_ctx).await?,
    };
    let target_entries = match &target {
        Side::Local(root) => {
            if root.exists() {
                let scan = scan_task(root);
                let entries = collect_local_entries(root, &scan, true).await?;
                scan.finish();
                entries
            } else {
                Vec::new()
            }
        }
        Side::S3 {
            client,
            bucket,
            prefix,
            ..
        } => collect_s3_entries(client, bucket, prefix, dispatch_ctx).await?,
    };
    // Deletes must be computed from *true* source presence: mc's own
    // isOlder/isNewer are applied per diff-URL, after planning, and only
    // skip COPIES (`cmd/mirror-main.go:826-838`) -- they never affect
    // whether a source object counts as "still present" for `--remove`'s
    // extraneous-target detection. Filtering `source_entries` before
    // `plan_mirror` would make an age-excluded-but-still-present source
    // object look absent, so its still-current target twin would get
    // deleted under `--remove`. Plan against the unfiltered entries first,
    // then drop filtered-out copies from the plan afterward.
    let mut plan = plan_mirror(
        &source_entries,
        &target_entries,
        args.overwrite,
        args.remove,
    );
    if args.older_than.is_some() || args.newer_than.is_some() {
        let mut kept = Vec::with_capacity(plan.copies.len());
        for entry in plan.copies {
            if crate::timefilter::passes_filters(
                entry.modified,
                args.older_than.as_deref(),
                args.newer_than.as_deref(),
            )? {
                kept.push(entry);
            }
        }
        plan.copies = kept;
    }

    if dry_run {
        // mc has no documented `--json` contract for a dry-run mirror plan
        // ([OUT]/[SEM] research is silent on it); rather than guess at a
        // shape, `--json --dry-run` here emits nothing on stdout at all --
        // the conservative choice that keeps a JSON-lines stream from being
        // corrupted by this plain-text prose (see README's known
        // divergences).
        if !crate::output::out().json {
            for entry in &plan.copies {
                ui_println!(
                    "PUT {}/{} -> {}/{}",
                    args.source.trim_end_matches('/'),
                    entry.rel,
                    args.target.trim_end_matches('/'),
                    entry.rel
                );
            }
            for rel in &plan.deletes {
                ui_println!("DEL {}/{}", args.target.trim_end_matches('/'), rel);
            }
            ui_println!(
                "Planned {} put(s), {} delete(s).",
                plan.copies.len(),
                plan.deletes.len()
            );
        }
        return Ok(());
    }

    // --- copies, cross-object parallel ---
    // Totals are known upfront from the plan, so register them before
    // starting any transfer -- matches mc's copyMessage/mirrorMessage
    // totalCount/totalSize, which reflect the whole session's planned
    // work, not a running tally.
    // The session wraps the display planning already used, rather than
    // building a second one -- see `ui`'s comment above.
    let session = TransferSession::from_ui(ui);
    let mut planned_bytes = 0u64;
    for entry in &plan.copies {
        session.add_total(entry.size);
        planned_bytes += entry.size;
    }
    // `add_total` above feeds the *message* totals only. The display keeps
    // its own, which otherwise accretes one object at a time from inside
    // the transfer functions and so can never lead the work in progress --
    // see `ProgressUi::declare_total`. Everything is known here, so say it
    // once and the TOTAL row is honest from its first frame.
    //
    // Deletes are in the object count but not the byte count: they move no
    // payload (hence their exclusion from `add_total`), yet each one still
    // reports through `object_done`, so omitting them would walk the
    // numerator past the total on a `--remove` run.
    if let Some(ui) = session.ui() {
        ui.declare_total(
            (plan.copies.len() + plan.deletes.len()) as u64,
            planned_bytes,
        );
    }
    let failures = stream::iter(plan.copies.iter().map(|entry| {
        let source = &source;
        let target = &target;
        let session = &session;
        let attrs = &attrs;
        let stream_budget = &stream_budget;
        async move {
            let result = copy_entry(
                source,
                target,
                entry,
                part_size,
                args.disable_multipart,
                parallel,
                attrs,
                args.preserve,
                stream_budget,
                session.ui(),
            )
            .await;
            match result {
                Ok((source_display, target_display)) => {
                    let (total_count, total_size) = session.totals();
                    let msg = MirrorMessage {
                        source: source_display,
                        target: target_display,
                        size: entry.size,
                        total_count,
                        total_size,
                        removed: false,
                    };
                    session.object_done(&msg, entry.size);
                    if args.delete_source_after {
                        // `mv`'s recursive path: delete this object's
                        // source now that its copy has succeeded. Delete
                        // failures are logged but must not count toward the
                        // failure total that drives exit status ([SEM] §3).
                        delete_source_entry(source, entry, stream_budget, session.ui()).await;
                    }
                    0u64
                }
                Err(err) => {
                    ui_eprintln!("mirror: `{}` failed: {err:#}", entry.rel);
                    1u64
                }
            }
        }
    }))
    .buffer_unordered(parallel)
    .fold(0u64, |acc, n| async move { acc + n })
    .await;

    // --- deletes ---
    // Deletes carry no transferred payload, so they don't touch the
    // session's byte totals (`add_total`/`accountStat`) -- only Copy/
    // add_total feeds those, matching mc's accounter tracking copy bytes
    // only. They still emit a `MirrorMessage{removed: true}` through the
    // same session so Bar-mode suppresses them and Lines-mode prints them,
    // exactly like the copy path.
    let mut delete_failures = 0u64;
    if !plan.deletes.is_empty() {
        // The delete plan's size is known exactly here, and neither branch
        // below moves any bytes -- a count-shaped task is what actually
        // describes this phase, where a byte bar would sit at 0/0.
        let removing = match session.ui() {
            Some(ui) => ui.start(crate::progress::ProgressAwareTask::count(
                crate::progress::TransferLabel {
                    verb: crate::progress::Verb::Removing,
                    path: args.target.trim_end_matches('/').to_string(),
                    part: None,
                },
                plan.deletes.len() as u64,
                "{done}/{total} obj",
            )),
            None => crate::progress::ProgressNotifier::noop(),
        };
        match &target {
            Side::Local(root) => {
                for rel in &plan.deletes {
                    let path = root.join(rel);
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => {
                            removing.advance(1);
                            let (total_count, total_size) = session.totals();
                            let msg = MirrorMessage {
                                source: String::new(),
                                target: path.display().to_string(),
                                size: 0,
                                total_count,
                                total_size,
                                removed: true,
                            };
                            session.object_done(&msg, 0);
                        }
                        Err(err) => {
                            ui_eprintln!("mirror: remove `{}` failed: {err}", path.display());
                            delete_failures += 1;
                        }
                    }
                }
            }
            Side::S3 {
                client,
                alias_name,
                bucket,
                prefix,
                ..
            } => {
                use aws_sdk_s3::types::{Delete, ObjectIdentifier};
                for chunk in plan.deletes.chunks(1000) {
                    let ids = chunk
                        .iter()
                        .map(|rel| ObjectIdentifier::builder().key(s3_key(prefix, rel)).build())
                        .collect::<Result<Vec<_>, _>>()?;
                    let delete = Delete::builder().set_objects(Some(ids)).build()?;
                    // Built unconditionally even when `session.ui()` is
                    // `None` (dispatch just no-ops the task line then) --
                    // one string format over a <=1000-key chunk is
                    // negligible next to the network round trip it labels.
                    let label_path = match chunk {
                        [only] => s3_key(prefix, only),
                        _ => format!("{} (+{} more)", s3_key(prefix, &chunk[0]), chunk.len() - 1),
                    };
                    let resp = crate::budget::dispatch(
                        &stream_budget,
                        session.ui(),
                        crate::progress::TransferLabel {
                            verb: crate::progress::Verb::Removing,
                            path: label_path,
                            part: None,
                        },
                        "DeleteObjects",
                        client.delete_objects().bucket(bucket).delete(delete).send(),
                    )
                    .await?;
                    delete_failures += resp.errors().len() as u64;
                    let failed_keys: HashSet<&str> =
                        resp.errors().iter().filter_map(|err| err.key()).collect();
                    for err in resp.errors() {
                        ui_eprintln!(
                            "mirror: remove `{}` failed: {}",
                            err.key().unwrap_or("?"),
                            err.message().unwrap_or("unknown")
                        );
                    }
                    for rel in chunk {
                        let key = s3_key(prefix, rel);
                        if !failed_keys.contains(key.as_str()) {
                            removing.advance(1);
                            let (total_count, total_size) = session.totals();
                            let msg = MirrorMessage {
                                source: String::new(),
                                target: format!("{alias_name}/{bucket}/{key}"),
                                size: 0,
                                total_count,
                                total_size,
                                removed: true,
                            };
                            session.object_done(&msg, 0);
                        }
                    }
                }
            }
        }
        removing.finish();
    }

    session.finish();

    let total = failures + delete_failures;
    if total > 0 {
        return Err(anyhow!("{total} object(s) failed"));
    }
    Ok(())
}

/// `mv`'s recursive per-object cleanup: deletes `entry` from `source` after
/// its copy has already succeeded. Fire-and-forget -- errors are logged to
/// stderr and swallowed, matching mc's async/decoupled `removeManager`
/// ([SEM] §3): a delete failure must not affect the overall exit status.
async fn delete_source_entry(
    source: &Side,
    entry: &Entry,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) {
    let result: Result<()> = match source {
        Side::Local(root) => tokio::fs::remove_file(root.join(&entry.rel))
            .await
            .map_err(Into::into),
        Side::S3 {
            client,
            bucket,
            prefix,
            ..
        } => {
            let key = s3_key(prefix, &entry.rel);
            crate::budget::dispatch(
                budget,
                progress,
                crate::progress::TransferLabel {
                    verb: crate::progress::Verb::Removing,
                    path: key.clone(),
                    part: None,
                },
                "DeleteObject",
                client.delete_object().bucket(bucket).key(key).send(),
            )
            .await
            .map(|_| ())
            .map_err(Into::into)
        }
    };
    if let Err(err) = result {
        ui_eprintln!("mv: remove `{}` failed: {err:#}", entry.rel);
    }
}

/// Performs one entry's transfer and returns the `(source, target)`
/// user-facing aliased-path display strings for the caller's
/// `MirrorMessage`. This module (like `transfer::upload_file`/
/// `download_key_to_path`) intentionally prints nothing itself.
#[allow(clippy::too_many_arguments)]
async fn copy_entry(
    source: &Side,
    target: &Side,
    entry: &Entry,
    part_size: u64,
    disable_multipart: bool,
    parallel: usize,
    attrs: &BTreeMap<String, String>,
    preserve: bool,
    budget: &crate::budget::StreamBudget,
    progress: Option<&crate::progress::ProgressUi>,
) -> Result<(String, String)> {
    match (source, target) {
        (
            Side::Local(src_root),
            Side::S3 {
                alias_name,
                bucket,
                prefix,
                ..
            },
        ) => {
            let src = src_root.join(&entry.rel);
            let target_url = format!("{alias_name}/{bucket}/{}", s3_key(prefix, &entry.rel));
            crate::transfer::upload_file(
                &src,
                &target_url,
                part_size,
                parallel,
                disable_multipart,
                None,
                attrs,
                false,
                preserve,
                budget,
                progress,
            )
            .await?;
            Ok((src.display().to_string(), target_url))
        }
        (
            Side::S3 {
                client,
                alias_name,
                bucket,
                prefix,
                ..
            },
            Side::Local(dst_root),
        ) => {
            let key = s3_key(prefix, &entry.rel);
            let output = dst_root.join(&entry.rel);
            crate::transfer::download_key_to_path(
                client, bucket, &key, &output, part_size, parallel, preserve, budget, progress,
            )
            .await?;
            Ok((
                format!("{alias_name}/{bucket}/{key}"),
                output.display().to_string(),
            ))
        }
        (
            Side::S3 {
                client: sc,
                alias: sa,
                alias_name: san,
                bucket: sb,
                prefix: sp,
                ..
            },
            Side::S3 {
                client: tc,
                alias: ta,
                alias_name: tan,
                bucket: tb,
                prefix: tp,
                ..
            },
        ) => {
            let source_key = s3_key(sp, &entry.rel);
            let target_key = s3_key(tp, &entry.rel);
            crate::transfer::transfer_object_between_s3(
                sc,
                sa,
                sb,
                &source_key,
                tc,
                ta,
                tb,
                &target_key,
                entry.size,
                part_size,
                disable_multipart,
                parallel,
                preserve,
                budget,
                progress,
            )
            .await?;
            Ok((
                format!("{san}/{sb}/{source_key}"),
                format!("{tan}/{tb}/{target_key}"),
            ))
        }
        (Side::Local(_), Side::Local(_)) => {
            Err(anyhow!("mirror between two local paths is not supported"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn entry(rel: &str, size: u64, ts: Option<i64>) -> Entry {
        Entry {
            rel: rel.into(),
            size,
            modified: ts.map(|t| Utc.timestamp_opt(t, 0).unwrap()),
        }
    }

    #[test]
    fn copies_missing_targets() {
        let plan = plan_mirror(&[entry("a", 1, Some(100))], &[], false, false);
        assert_eq!(plan.copies.len(), 1);
        assert!(plan.deletes.is_empty());
    }

    #[test]
    fn skips_same_size_older_or_equal_source() {
        let src = [entry("a", 5, Some(100))];
        let dst = [entry("a", 5, Some(100))];
        assert!(plan_mirror(&src, &dst, false, false).copies.is_empty());
        let dst_newer = [entry("a", 5, Some(200))];
        assert!(
            plan_mirror(&src, &dst_newer, false, false)
                .copies
                .is_empty()
        );
    }

    #[test]
    fn copies_when_size_differs_or_source_newer() {
        let dst = [entry("a", 5, Some(100)), entry("b", 9, Some(100))];
        let src = [entry("a", 6, Some(100)), entry("b", 9, Some(150))];
        let plan = plan_mirror(&src, &dst, false, false);
        let rels: Vec<_> = plan.copies.iter().map(|e| e.rel.as_str()).collect();
        assert_eq!(rels, vec!["a", "b"]);
    }

    #[test]
    fn copies_when_timestamps_missing() {
        let src = [entry("a", 5, None)];
        let dst = [entry("a", 5, Some(100))];
        assert_eq!(plan_mirror(&src, &dst, false, false).copies.len(), 1);
    }

    #[test]
    fn overwrite_copies_everything() {
        let src = [entry("a", 5, Some(100))];
        let dst = [entry("a", 5, Some(200))];
        assert_eq!(plan_mirror(&src, &dst, true, false).copies.len(), 1);
    }

    #[test]
    fn remove_deletes_extraneous_targets_only_when_asked() {
        let src = [entry("a", 1, Some(100))];
        let dst = [entry("a", 1, Some(100)), entry("stale", 2, Some(50))];
        assert!(plan_mirror(&src, &dst, false, false).deletes.is_empty());
        assert_eq!(
            plan_mirror(&src, &dst, false, true).deletes,
            vec!["stale".to_string()]
        );
    }
}
