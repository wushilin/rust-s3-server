use anyhow::Result;
use aws_sdk_s3::Client;

#[derive(Debug, Clone)]
pub(crate) struct ListedObject {
    pub(crate) key: String,
    pub(crate) size: u64,
}

pub(crate) async fn list_s3_objects(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<ListedObject>> {
    let mut token = None;
    let mut objects = Vec::new();
    loop {
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_continuation_token(token)
            .send()
            .await?;
        for obj in resp.contents() {
            let Some(key) = obj.key() else {
                continue;
            };
            if key.ends_with('/') {
                continue;
            }
            objects.push(ListedObject {
                key: key.to_string(),
                size: obj.size().unwrap_or_default() as u64,
            });
        }
        if resp.is_truncated().unwrap_or(false) {
            token = resp.next_continuation_token().map(String::from);
        } else {
            break;
        }
    }
    Ok(objects)
}
