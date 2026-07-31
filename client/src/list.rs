use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub(crate) struct ListedObject {
    pub key: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

/// Optional `-P` worker-task routing for [`ObjectPaginator`]'s per-page
/// `ListObjectsV2` calls: the stream budget token + a `Listing` spinner
/// task line, same as every other S3 control-plane call. `None` (the
/// default from [`ObjectPaginator::new`]/[`ObjectPaginator::new_raw`])
/// keeps the call bare -- used by callers with no budget/ui handy
/// (`share.rs`).
type DispatchCtx = Option<(
    crate::budget::StreamBudget,
    Option<crate::progress::ProgressUi>,
)>;

pub(crate) struct ObjectPaginator {
    client: Client,
    bucket: String,
    prefix: String,
    token: Option<String>,
    done: bool,
    include_markers: bool,
    dispatch: DispatchCtx,
}

impl ObjectPaginator {
    pub(crate) fn new(client: Client, bucket: String, prefix: String) -> Self {
        Self {
            client,
            bucket,
            prefix,
            token: None,
            done: false,
            include_markers: false,
            dispatch: None,
        }
    }

    /// Like [`ObjectPaginator::new`], but does not skip zero-byte
    /// "folder marker" keys (keys ending in `/`). Destructive operations
    /// (e.g. `rm -r`, `rb --force`) must enumerate and delete these markers
    /// too, or they survive and leave the bucket non-empty.
    pub(crate) fn new_raw(client: Client, bucket: String, prefix: String) -> Self {
        Self {
            client,
            bucket,
            prefix,
            token: None,
            done: false,
            include_markers: true,
            dispatch: None,
        }
    }

    /// Route every subsequent `next_page()` call's `ListObjectsV2` through
    /// `crate::budget::dispatch` (a `-P` token + `Listing` spinner task
    /// line), like every other S3 control-plane op in the standalone
    /// commands. `ui: None` still takes a budget token but shows no line
    /// (matches `dispatch`'s own noop-when-`None` contract).
    pub(crate) fn with_dispatch(
        mut self,
        budget: crate::budget::StreamBudget,
        ui: Option<crate::progress::ProgressUi>,
    ) -> Self {
        self.dispatch = Some((budget, ui));
        self
    }

    pub(crate) async fn next_page(&mut self) -> Result<Option<Vec<ListedObject>>> {
        if self.done {
            return Ok(None);
        }
        let req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .set_continuation_token(self.token.take());
        let resp = match &self.dispatch {
            Some((budget, ui)) => {
                crate::budget::dispatch(
                    budget,
                    ui.as_ref(),
                    crate::progress::TransferLabel {
                        verb: crate::progress::Verb::Listing,
                        path: crate::progress::bucket_prefix_label(&self.bucket, &self.prefix),
                        part: None,
                    },
                    "ListObjectsV2",
                    req.send(),
                )
                .await?
            }
            None => req.send().await?,
        };
        let mut page = Vec::new();
        for obj in resp.contents() {
            let Some(key) = obj.key() else { continue };
            let size = obj.size().unwrap_or_default() as u64;
            if !self.include_markers && key.ends_with('/') && size == 0 {
                continue; // folder marker
            }
            page.push(ListedObject {
                key: key.to_string(),
                size,
                modified: obj
                    .last_modified()
                    .and_then(|t| DateTime::<Utc>::from_timestamp(t.secs(), t.subsec_nanos())),
            });
        }
        if resp.is_truncated().unwrap_or(false) {
            self.token = resp.next_continuation_token().map(String::from);
            if self.token.is_none() {
                self.done = true;
            }
        } else {
            self.done = true;
        }
        Ok(Some(page))
    }
}

pub(crate) async fn collect_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<ListedObject>> {
    collect_objects_with(client, bucket, prefix, None).await
}

/// Like [`collect_objects`], but routes every page's `ListObjectsV2` call
/// through the `-P` worker-task dispatch when `dispatch_ctx` is `Some`
/// (standalone commands with a budget/ui handy: `stat -r`, `du`, `find`,
/// `diff`, `mirror`'s planning listing). `None` keeps `collect_objects`'s
/// original bare-call behavior (`share.rs`, and every pre-task-3 caller).
pub(crate) async fn collect_objects_with(
    client: &Client,
    bucket: &str,
    prefix: &str,
    dispatch_ctx: Option<(
        &crate::budget::StreamBudget,
        Option<&crate::progress::ProgressUi>,
    )>,
) -> Result<Vec<ListedObject>> {
    let mut pager = ObjectPaginator::new(client.clone(), bucket.to_string(), prefix.to_string());
    if let Some((budget, ui)) = dispatch_ctx {
        pager = pager.with_dispatch(budget.clone(), ui.cloned());
    }
    let mut all = Vec::new();
    while let Some(page) = pager.next_page().await? {
        all.extend(page);
    }
    Ok(all)
}
