//! `GET` / `PUT` / `DELETE /{bucket}/{key}?tagging` — the tags of one
//! version of an object (the one `versionId` names, else the current one).
//! Tags are the one mutable part of a version: replacing them changes
//! neither the ETag nor the version. Reading tags needs `s3:GetObject`,
//! writing them `s3:PutObject`, on the object.

use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{Method, StatusCode};
use axum::response::Response;

use crate::server as srv;
use crate::server::handlers::ObjectCtx;
use crate::server::unescape_xml;
use crate::server::xml::escape_xml;
use crate::storage::errors::StorageError;
use crate::storage::metadata::validate_tags;
use crate::storage::store::LocalObjectStore;

pub(crate) async fn handle(store: LocalObjectStore, ctx: ObjectCtx, body: Body) -> Response {
    let resource = ctx.resource();
    let version_id = ctx.query.get("versionId").filter(|v| !v.is_empty()).cloned();
    match ctx.method {
        Method::GET => match store.object_tags(&ctx.bucket, &ctx.key, version_id.as_deref()).await {
            Ok((tags, version_id)) => {
                let set = tags
                    .iter()
                    .map(|(k, v)| format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", escape_xml(k), escape_xml(v)))
                    .collect::<String>();
                srv::with_version_id(
                    srv::xml_response(
                        StatusCode::OK,
                        format!(r#"<?xml version="1.0" encoding="UTF-8"?><Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>{set}</TagSet></Tagging>"#),
                    ),
                    Some(&version_id),
                )
            }
            Err(err) => srv::storage_error_response(err, &resource),
        },
        Method::PUT | Method::DELETE => {
            // A replicated tag change carries the version's stamp.
            let options = match srv::write_options(&store, &ctx, &resource).await {
                Ok(options) => options,
                Err(response) => return response,
            };
            let tags = if ctx.method == Method::DELETE {
                BTreeMap::new()
            } else {
                let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
                    Ok(bytes) => bytes,
                    Err(_) => return srv::s3_error(StatusCode::BAD_REQUEST, "MalformedXML", "The XML you provided was not well-formed", &resource),
                };
                match parse_tagging_xml(&String::from_utf8_lossy(&bytes)) {
                    Ok(tags) => tags,
                    Err(message) => return srv::s3_error(StatusCode::BAD_REQUEST, "InvalidTag", message, &resource),
                }
            };
            let author = srv::replicated_author(&ctx.query, &options.origin);
            match store.set_object_tags_by(&ctx.bucket, &ctx.key, version_id.as_deref(), tags, options.origin, author.as_deref()).await {
                Ok(version_id) => {
                    let status = if ctx.method == Method::DELETE { StatusCode::NO_CONTENT } else { StatusCode::OK };
                    srv::with_version_id(srv::empty_response(status), Some(&version_id))
                }
                Err(StorageError::InvalidObjectKey(message)) => srv::s3_error(StatusCode::BAD_REQUEST, "InvalidTag", message, &resource),
                Err(err) => srv::storage_error_response(err, &resource),
            }
        }
        _ => srv::s3_error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "The specified method is not allowed against this resource.", &resource),
    }
}

/// Parses a `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag>…`
/// document. A repeated key, a missing key, or more than 10 tags is an
/// error.
pub(crate) fn parse_tagging_xml(xml: &str) -> Result<BTreeMap<String, String>, String> {
    let mut tags = BTreeMap::new();
    let tag_re = regex::Regex::new(r"(?s)<Tag>(.*?)</Tag>").unwrap();
    let key_re = regex::Regex::new(r"(?s)<Key>(.*?)</Key>").unwrap();
    let value_re = regex::Regex::new(r"(?s)<Value>(.*?)</Value>").unwrap();
    if !xml.contains("<Tagging") || !xml.contains("<TagSet") {
        return Err("The XML you provided was not well-formed or did not validate against our published schema".to_string());
    }
    for tag in tag_re.captures_iter(xml) {
        let body = tag.get(1).map(|m| m.as_str()).unwrap_or("");
        let key = key_re.captures(body).and_then(|c| c.get(1)).map(|m| unescape_xml(m.as_str())).ok_or_else(|| "The TagKey you have provided is invalid".to_string())?;
        let value = value_re.captures(body).and_then(|c| c.get(1)).map(|m| unescape_xml(m.as_str())).unwrap_or_default();
        if tags.insert(key, value).is_some() {
            return Err("Cannot provide multiple Tags with the same key".to_string());
        }
    }
    validate_tags(&tags)?;
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagging_xml_parses_and_validates() {
        let tags = parse_tagging_xml("<Tagging><TagSet><Tag><Key>a</Key><Value>1 + 2</Value></Tag><Tag><Key>b</Key><Value></Value></Tag></TagSet></Tagging>").unwrap();
        assert_eq!(tags.get("a").map(String::as_str), Some("1 + 2"));
        assert_eq!(tags.get("b").map(String::as_str), Some(""));
        assert!(parse_tagging_xml("<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>").is_err());
        assert!(parse_tagging_xml("<Tagging><TagSet><Tag><Key>bad*key</Key><Value>1</Value></Tag></TagSet></Tagging>").is_err());
        assert!(parse_tagging_xml("<Nope/>").is_err());
        let many = (0..11).map(|i| format!("<Tag><Key>k{i}</Key><Value>v</Value></Tag>")).collect::<String>();
        assert!(parse_tagging_xml(&format!("<Tagging><TagSet>{many}</TagSet></Tagging>")).is_err());
    }
}
