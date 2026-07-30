use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub(crate) struct ListedObject {
    pub key: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

pub(crate) struct ObjectPaginator {
    client: Client,
    bucket: String,
    prefix: String,
    token: Option<String>,
    done: bool,
    include_markers: bool,
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
        }
    }

    pub(crate) async fn next_page(&mut self) -> Result<Option<Vec<ListedObject>>> {
        if self.done {
            return Ok(None);
        }
        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .set_continuation_token(self.token.take())
            .send()
            .await?;
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
    let mut pager = ObjectPaginator::new(client.clone(), bucket.to_string(), prefix.to_string());
    let mut all = Vec::new();
    while let Some(page) = pager.next_page().await? {
        all.extend(page);
    }
    Ok(all)
}
