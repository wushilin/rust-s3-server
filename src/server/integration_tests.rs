    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use futures::StreamExt;
    use tower::ServiceExt;

    use crate::storage::store::LocalObjectStore;

    use super::{router, router_with_metrics, TrafficMetrics};

    fn make_app(tmp: &tempfile::TempDir) -> axum::Router {
        router(
            LocalObjectStore::new(tmp.path()),
            std::sync::Arc::new(super::config::AppConfig::default()),
        )
    }

    fn make_app_with_store(tmp: &tempfile::TempDir) -> (axum::Router, LocalObjectStore) {
        let store = LocalObjectStore::new(tmp.path());
        (
            router(
                store.clone(),
                std::sync::Arc::new(super::config::AppConfig::default()),
            ),
            store,
        )
    }

    fn make_app_with_metrics(
        tmp: &tempfile::TempDir,
    ) -> (axum::Router, std::sync::Arc<TrafficMetrics>) {
        let metrics = std::sync::Arc::new(TrafficMetrics::default());
        (
            router_with_metrics(
                LocalObjectStore::new(tmp.path()),
                super::auth::AuthState {
                    config: std::sync::Arc::new(super::config::AppConfig::default()),
                    iam: None,
                },
                metrics.clone(),
                super::registry::TaskRegistry::new(),
            ),
            metrics,
        )
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8_lossy(&bytes).to_string()
    }

    fn aws_chunked_body(payload: &[u8]) -> Vec<u8> {
        let mut body = format!("{:x};chunk-signature=abc\r\n", payload.len()).into_bytes();
        body.extend_from_slice(payload);
        body.extend_from_slice(b"\r\n0;chunk-signature=def\r\n\r\n");
        body
    }

    fn gzip_helloworld() -> Vec<u8> {
        vec![
            0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0xcb, 0x48, 0xcd, 0xc9,
            0xc9, 0x2f, 0xcf, 0x2f, 0xca, 0x49, 0x01, 0x00, 0xad, 0x20, 0xeb, 0xf9, 0x0a, 0x00,
            0x00, 0x00,
        ]
    }

    fn extract_all_xml_tags(xml: &str, tag: &str) -> Vec<String> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let mut results = Vec::new();
        let mut pos = 0;
        while let Some(rel_start) = xml[pos..].find(&open) {
            let abs_start = pos + rel_start + open.len();
            let Some(rel_end) = xml[abs_start..].find(&close) else {
                break;
            };
            results.push(xml[abs_start..abs_start + rel_end].to_string());
            pos = abs_start + rel_end + close.len();
        }
        results
    }

    fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = xml.find(&open)? + open.len();
        let end = xml[start..].find(&close)? + start;
        Some(&xml[start..end])
    }

    #[tokio::test]
    async fn put_get_delete_object_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        // Create bucket
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // PUT object
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket/hello.txt")
                    .header("content-type", "text/plain")
                    .body(Body::from("hello world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.headers().contains_key("etag"));

        // GET object
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/test-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "hello world");

        // DELETE object
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/test-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        // GET after delete → 404
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/test-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchKey</Code>"));
    }

    #[tokio::test]
    async fn put_object_rejects_invalid_storage_class() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/sc-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // A valid S3 storage class is accepted.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/sc-bucket/ok.txt")
                    .header("x-amz-storage-class", "REDUCED_REDUNDANCY")
                    .body(Body::from("hi"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // An unknown storage class is rejected with InvalidStorageClass.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/sc-bucket/bad.txt")
                    .header("x-amz-storage-class", "REDUCED")
                    .body(Body::from("hi"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>InvalidStorageClass</Code>"));

        // The rejected object must not have been stored.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/sc-bucket/bad.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn gzip_content_encoding_is_preserved_with_raw_stored_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);
        let gzip_helloworld = gzip_helloworld();

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/gzip-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/gzip-bucket/hello.txt")
                    .header("content-type", "text/plain")
                    .header("content-encoding", "gzip")
                    .body(Body::from(gzip_helloworld.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/gzip-bucket/hello.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("gzip")
        );
        let returned = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(returned.as_ref(), gzip_helloworld.as_slice());
        assert_ne!(String::from_utf8_lossy(&returned), "helloworld");
    }

    #[tokio::test]
    async fn missing_bucket_returns_no_such_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/no-such-bucket/key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchBucket</Code>"));
    }

    #[tokio::test]
    async fn range_request_returns_partial_content() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket/data")
                    .body(Body::from("0123456789"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/rng-bucket/data")
                    .header("range", "bytes=2-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            res.headers()
                .get("content-range")
                .unwrap()
                .to_str()
                .unwrap(),
            "bytes 2-5/10"
        );
        assert_eq!(body_text(res).await, "2345");
    }

    #[tokio::test]
    async fn traffic_metrics_count_streamed_body_bytes_not_content_length_header() {
        let tmp = tempfile::tempdir().unwrap();
        let (app, metrics) = make_app_with_metrics(&tmp);

        let before_bucket = metrics.snapshot();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/metric-bucket")
                    .header("content-length", "999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let after_bucket = metrics.snapshot();
        assert_eq!(after_bucket.bytes_in, before_bucket.bytes_in);
        assert_eq!(after_bucket.put_requests - before_bucket.put_requests, 1);

        let before_put = metrics.snapshot();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/metric-bucket/object")
                    .body(Body::from("hello world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let _ = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let after_put = metrics.snapshot();
        assert_eq!(after_put.bytes_in - before_put.bytes_in, 11);
        assert_eq!(after_put.put_requests - before_put.put_requests, 1);

        let before_head = metrics.snapshot();
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/metric-bucket/object")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let _ = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let after_head = metrics.snapshot();
        assert_eq!(after_head.bytes_out, before_head.bytes_out);
        assert_eq!(after_head.head_requests - before_head.head_requests, 1);

        let before_get = metrics.snapshot();
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/metric-bucket/object")
                    .header("range", "bytes=1-4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(body_text(res).await, "ello");
        let after_get = metrics.snapshot();
        assert_eq!(after_get.bytes_out - before_get.bytes_out, 4);
        assert_eq!(after_get.get_requests - before_get.get_requests, 1);
        assert_eq!(after_get.request_total(), 4);
    }

    #[tokio::test]
    async fn range_unsatisfiable_returns_416() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket-2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rng-bucket-2/small")
                    .body(Body::from("hi"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/rng-bucket-2/small")
                    .header("range", "bytes=100-200")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert!(res
            .headers()
            .get("content-range")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("bytes */"));
    }

    #[tokio::test]
    async fn list_objects_with_prefix_and_delimiter() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/list-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        for key in [
            "/list-bucket/a/1",
            "/list-bucket/a/2",
            "/list-bucket/b/1",
            "/list-bucket/c",
        ] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(key)
                        .body(Body::from("x"))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/list-bucket?prefix=&delimiter=/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<Prefix>a/</Prefix>"));
        assert!(body.contains("<Prefix>b/</Prefix>"));
        assert!(body.contains("<Key>c</Key>"));
    }

    #[tokio::test]
    async fn list_objects_accepts_bucket_path_with_trailing_slash() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/doris")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/doris/run.sh")
                    .body(Body::from("echo ok"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/doris/?list-type=2&delimiter=%2F&max-keys=1000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<Name>doris</Name>"));
        assert!(body.contains("<Key>run.sh</Key>"));
    }

    #[tokio::test]
    async fn multipart_upload_full_flow() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mpu-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mpu-bucket/large.bin?uploads")
                    .header("x-amz-meta-randomstuff", "multipart-meta")
                    .header("x-amz-storage-class", "STANDARD_IA")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let xml = body_text(res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        // Upload parts
        let part1 = vec![b'a'; 5 * 1024 * 1024];
        let res1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mpu-bucket/large.bin?uploadId={upload_id}&partNumber=1"
                    ))
                    .body(Body::from(part1.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res1.status(), StatusCode::OK);
        let etag1 = res1
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let res2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mpu-bucket/large.bin?uploadId={upload_id}&partNumber=2"
                    ))
                    .body(Body::from(" world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res2.status(), StatusCode::OK);
        let etag2 = res2
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Complete
        let complete_xml = format!(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag></Part></CompleteMultipartUpload>"#
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mpu-bucket/large.bin?uploadId={upload_id}"))
                    .body(Body::from(complete_xml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<ETag>"));

        // Verify content
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mpu-bucket/large.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.len(), part1.len() + " world".len());
        assert!(body.starts_with(&part1));
        assert!(body.ends_with(b" world"));

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/mpu-bucket/large.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get("x-amz-meta-randomstuff")
                .and_then(|v| v.to_str().ok()),
            Some("multipart-meta")
        );
        assert_eq!(
            res.headers()
                .get("x-amz-storage-class")
                .and_then(|v| v.to_str().ok()),
            Some("STANDARD_IA")
        );
    }

    #[tokio::test]
    async fn multipart_upload_part_copy_returns_copy_part_xml() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        for bucket in ["/mpu-copy-src", "/mpu-copy-dst"] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(bucket)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mpu-copy-src/source.txt")
                    .body(Body::from("abcdefgh"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mpu-copy-dst/copied.txt?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let xml = body_text(res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/mpu-copy-dst/copied.txt?uploadId={upload_id}&partNumber=1"
                    ))
                    .header("x-amz-copy-source", "/mpu-copy-src/source.txt")
                    .header("x-amz-copy-source-range", "bytes=2-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let xml = body_text(res).await;
        assert!(xml.contains("<CopyPartResult"));
        let etag = extract_xml_tag(&xml, "ETag").unwrap().to_string();

        let complete_xml = format!(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"#
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mpu-copy-dst/copied.txt?uploadId={upload_id}"))
                    .body(Body::from(complete_xml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mpu-copy-dst/copied.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "cdef");
    }

    #[tokio::test]
    async fn multipart_get_survives_delete_after_stream_starts() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/mpu-read-delete-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mpu-read-delete-bucket/big.bin?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let xml = body_text(res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        let parts = [
            vec![b'a'; 5 * 1024 * 1024],
            vec![b'b'; 5 * 1024 * 1024],
            vec![b'c'; 5 * 1024 * 1024],
            vec![b'd'; 384 * 1024],
        ];
        let mut expected = Vec::new();
        let mut etags = Vec::new();
        for (idx, part) in parts.iter().enumerate() {
            expected.extend_from_slice(part);
            let part_number = idx + 1;
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!(
                            "/mpu-read-delete-bucket/big.bin?uploadId={upload_id}&partNumber={part_number}"
                        ))
                        .body(Body::from(part.clone()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            etags.push(
                res.headers()
                    .get("etag")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
            );
        }

        let complete_parts = etags
            .iter()
            .enumerate()
            .map(|(idx, etag)| {
                format!(
                    "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                    idx + 1,
                    etag
                )
            })
            .collect::<String>();
        let complete_xml =
            format!("<CompleteMultipartUpload>{complete_parts}</CompleteMultipartUpload>");
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/mpu-read-delete-bucket/big.bin?uploadId={upload_id}"
                    ))
                    .body(Body::from(complete_xml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mpu-read-delete-bucket/big.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let mut stream = res.into_body().into_data_stream();

        let mut downloaded = Vec::new();
        let first = stream
            .next()
            .await
            .expect("GET body should produce the first chunk")
            .expect("first chunk should be readable");
        downloaded.extend_from_slice(&first);

        let delete_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/mpu-read-delete-bucket/big.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_res.status(), StatusCode::NO_CONTENT);

        let head_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/mpu-read-delete-bucket/big.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_res.status(), StatusCode::NOT_FOUND);

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("open multipart GET should survive concurrent delete");
            downloaded.extend_from_slice(&chunk);
        }

        assert_eq!(downloaded, expected);
    }

    #[tokio::test]
    async fn delete_nonempty_bucket_returns_conflict() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/full-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/full-bucket/obj")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/full-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);
        let body = body_text(res).await;
        assert!(body.contains("<Code>BucketNotEmpty</Code>"));
    }

    #[tokio::test]
    async fn pagination_iterates_all_35_objects_exactly_once() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/pg-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Put 35 zero-padded objects so lexicographic order matches numeric order.
        for i in 1..=35u32 {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/pg-bucket/obj-{i:02}"))
                        .body(Body::from(format!("data-{i}")))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let mut all_keys: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0usize;

        loop {
            let uri = match &token {
                None => "/pg-bucket?list-type=2&max-keys=10".to_string(),
                Some(t) => format!("/pg-bucket?list-type=2&max-keys=10&continuation-token={t}"),
            };
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri(&uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let body = body_text(res).await;
            pages += 1;

            let keys = extract_all_xml_tags(&body, "Key");
            assert!(!keys.is_empty(), "page {pages} returned no keys");
            all_keys.extend(keys);

            let is_truncated = extract_xml_tag(&body, "IsTruncated") == Some("true");
            token = extract_xml_tag(&body, "NextContinuationToken").map(str::to_string);

            if !is_truncated {
                break;
            }
            assert!(
                token.is_some(),
                "truncated page must have NextContinuationToken"
            );
            assert!(pages < 20, "pagination did not terminate");
        }

        assert_eq!(
            all_keys.len(),
            35,
            "must return all 35 objects across pages"
        );
        // Keys must arrive in ascending order
        let mut sorted = all_keys.clone();
        sorted.sort();
        assert_eq!(all_keys, sorted, "keys must be in ascending order");
        // No duplicates
        let unique: std::collections::HashSet<_> = all_keys.iter().collect();
        assert_eq!(unique.len(), 35, "no duplicate keys");
    }

    #[tokio::test]
    async fn list_objects_rejects_invalid_max_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/invalid-max-keys-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/invalid-max-keys-bucket?max-keys=-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>InvalidArgument</Code>"));
    }

    #[tokio::test]
    async fn aborted_multipart_not_visible_on_get() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/ab-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ab-bucket/upload.bin?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let xml = body_text(res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        // Upload a part
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/ab-bucket/upload.bin?uploadId={upload_id}&partNumber=1"
                    ))
                    .body(Body::from("some data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Abort
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/ab-bucket/upload.bin?uploadId={upload_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        // GET must return 404 — upload was aborted, nothing committed
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ab-bucket/upload.bin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchKey</Code>"));

        // List must show empty bucket — no object was committed
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ab-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let list_body = body_text(res).await;
        assert!(
            !list_body.contains("<Key>"),
            "aborted upload must not appear in listing"
        );
    }

    #[tokio::test]
    async fn rebuild_index_endpoint_returns_202_and_index_can_be_queried() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rebuild-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rebuild-bucket/alpha")
                    .body(Body::from("a"))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/rebuild-bucket/beta")
                    .body(Body::from("b"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Trigger rebuild via HTTP endpoint
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rebuild-bucket?rebuildIndex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);

        // While the rebuild runs the bucket answers 503 SlowDown by design;
        // poll until it completes instead of racing a fixed sleep.
        let mut res = None;
        for _ in 0..200 {
            let attempt = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/rebuild-bucket")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            if attempt.status() != StatusCode::SERVICE_UNAVAILABLE {
                res = Some(attempt);
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        }
        let res = res.expect("rebuild did not finish within 5s");
        assert_eq!(res.status(), StatusCode::OK);
        let _unused = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/rebuild-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<Key>alpha</Key>"));
        assert!(body.contains("<Key>beta</Key>"));
    }

    #[tokio::test]
    async fn head_object_returns_metadata_without_body() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/head-bucket/file.txt")
                    .header("content-type", "text/plain")
                    .header("x-amz-meta-randomstuff", "abc123")
                    .header("x-amz-storage-class", "REDUCED_REDUNDANCY")
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/head-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            "3"
        );
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "text/plain"
        );
        assert_eq!(
            res.headers()
                .get("x-amz-meta-randomstuff")
                .and_then(|v| v.to_str().ok()),
            Some("abc123")
        );
        assert_eq!(
            res.headers()
                .get("x-amz-storage-class")
                .and_then(|v| v.to_str().ok()),
            Some("REDUCED_REDUNDANCY")
        );
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn head_object_returns_content_language() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/lang-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/lang-bucket/file.txt")
                    .header("content-language", "en-US")
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/lang-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get("content-language")
                .and_then(|v| v.to_str().ok()),
            Some("en-US")
        );
    }

    #[tokio::test]
    async fn browser_style_post_upload_creates_object() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/post-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let boundary = "post-boundary";
        let body = concat!(
            "--post-boundary\r\n",
            "Content-Disposition: form-data; name=\"key\"\r\n\r\n",
            "posted.txt\r\n",
            "--post-boundary\r\n",
            "Content-Disposition: form-data; name=\"x-amz-meta-origin\"\r\n\r\n",
            "browser\r\n",
            "--post-boundary\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"posted.txt\"\r\n",
            "Content-Type: text/plain\r\n\r\n",
            "hello post\r\n",
            "--post-boundary--\r\n"
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/post-bucket")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/post-bucket/posted.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get("x-amz-meta-origin")
                .and_then(|v| v.to_str().ok()),
            Some("browser")
        );
        assert_eq!(body_text(res).await, "hello post");
    }

    #[tokio::test]
    async fn browser_post_upload_without_signature_is_rejected_when_auth_enabled() {
        // Regression guard: with auth enabled, an unsigned multipart/form-data
        // POST must not be able to write an object (it used to be silently
        // treated as root).
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);
        let boundary = "sec-boundary";
        let body = concat!(
            "--sec-boundary\r\n",
            "Content-Disposition: form-data; name=\"key\"\r\n\r\n",
            "hack.txt\r\n",
            "--sec-boundary\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"hack.txt\"\r\n",
            "Content-Type: text/plain\r\n\r\n",
            "pwned\r\n",
            "--sec-boundary--\r\n"
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/any-bucket")
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn put_object_rejects_user_metadata_over_s3_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/meta-limit-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let too_large = "x".repeat(super::MAX_USER_META_BYTES + 1);
        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/meta-limit-bucket/file.txt")
                    .header("x-amz-meta-large", too_large)
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>InvalidArgument</Code>"));
    }

    #[tokio::test]
    async fn put_rejects_payload_hash_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/hash-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/hash-bucket/file.txt")
                    .header(
                        "x-amz-content-sha256",
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"));
    }

    #[tokio::test]
    async fn put_rejects_invalid_payload_hash_value() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/invalid-hash-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/invalid-hash-bucket/file.txt")
                    .header("x-amz-content-sha256", "invalid-sha256")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"));
    }

    #[tokio::test]
    async fn list_object_versions_includes_overwritten_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/versions-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/versions-bucket?versioning")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        for body in ["first", "second"] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/versions-bucket/object.txt")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/versions-bucket?versions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<ListVersionsResult"));
        assert_eq!(body.matches("<Version>").count(), 2);
        assert_eq!(body.matches("<Key>object.txt</Key>").count(), 2);
        assert!(body.contains("<IsLatest>true</IsLatest>"));
        assert!(body.contains("<IsLatest>false</IsLatest>"));
    }

    #[tokio::test]
    async fn get_with_missing_physical_part_returns_error_not_success() {
        let tmp = tempfile::tempdir().unwrap();
        let (app, store) = make_app_with_store(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/corrupt-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/corrupt-bucket/file.txt")
                    .body(Body::from("abc"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let read = store
            .read_object("corrupt-bucket", "file.txt")
            .await
            .unwrap();
        tokio::fs::remove_file(read.object_dir.join("part.1"))
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/corrupt-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_text(res).await;
        assert!(body.contains("<Code>InternalError</Code>"));
    }

    #[tokio::test]
    async fn aws_chunked_put_stream_decodes_body() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/chunked-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let chunks = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"5;chunk-signature=abc\r\nhe")),
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(
                b"llo\r\n0;chunk-signature=def\r\n\r\n",
            )),
        ]);
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/chunked-bucket/file.txt")
                    .header("content-encoding", "aws-chunked")
                    .body(Body::from_stream(chunks))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/chunked-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(!res.headers().contains_key("content-encoding"));
        assert_eq!(body_text(res).await, "hello");
    }

    #[tokio::test]
    async fn aws_chunked_with_gzip_preserves_gzip_content_encoding_only() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);
        let gzip_helloworld = gzip_helloworld();

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/chunked-gzip-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/chunked-gzip-bucket/file.txt")
                    .header("content-type", "text/plain")
                    .header("content-encoding", "aws-chunked, gzip")
                    .body(Body::from(aws_chunked_body(&gzip_helloworld)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/chunked-gzip-bucket/file.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("gzip")
        );
        let returned = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert_eq!(returned.as_ref(), gzip_helloworld.as_slice());
    }

    // ---------------------------------------------------------------------------
    // POST /{bucket}?delete — Delete Multiple Objects
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn delete_multiple_objects_removes_listed_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/del-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        for key in ["/del-bucket/a", "/del-bucket/b", "/del-bucket/c"] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(key)
                        .body(Body::from("x"))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let delete_xml =
            r#"<Delete><Object><Key>a</Key></Object><Object><Key>c</Key></Object></Delete>"#;
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/del-bucket?delete")
                    .body(Body::from(delete_xml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<DeleteResult"));
        assert!(body.contains("<Deleted>"));
        let deleted_keys = extract_all_xml_tags(&body, "Key");
        assert!(deleted_keys.contains(&"a".to_string()));
        assert!(deleted_keys.contains(&"c".to_string()));

        // 'a' and 'c' gone, 'b' remains
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/del-bucket/a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/del-bucket/b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn delete_multiple_objects_quiet_mode_returns_no_deleted_elements() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/del-quiet-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/del-quiet-bucket/foo")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        let delete_xml = r#"<Delete><Quiet>true</Quiet><Object><Key>foo</Key></Object></Delete>"#;
        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/del-quiet-bucket?delete")
                    .body(Body::from(delete_xml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(
            !body.contains("<Deleted>"),
            "quiet mode must omit <Deleted>"
        );
        assert!(!body.contains("<Error>"), "no errors expected");
    }

    #[tokio::test]
    async fn prefix_policy_authorizes_each_batch_delete_key_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path());
        store.create_bucket("scoped-delete").await.unwrap();
        for key in ["allowed/a.txt", "private/b.txt"] {
            store
                .put_object("scoped-delete", key, b"data", None, None, false)
                .await
                .unwrap();
        }
        let policy = super::policy::compile_rules(&[super::policy::PolicyRule {
            effect: super::policy::Effect::Allow,
            access: super::policy::RuleAccess::Write,
            bucket: "scoped-delete".to_string(),
            prefix: "allowed/".to_string(),
        }])
        .unwrap();
        let delete_xml = r#"<Delete><Object><Key>allowed/a.txt</Key></Object><Object><Key>private/b.txt</Key></Object></Delete>"#;

        let authority = super::identity::Identity::iam("scoped-user".to_string(), Some(policy));
        let response = super::bucket_route(
            axum::extract::State(store.clone()),
            axum::extract::Path("scoped-delete".to_string()),
            axum::extract::RawQuery(Some("delete".to_string())),
            axum::http::HeaderMap::new(),
            axum::http::Method::POST,
            None,
            Some(axum::Extension(authority.clone())),
            None,
            None,
            Body::from(delete_xml),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("<Key>allowed/a.txt</Key>"));
        assert!(body.contains("<Code>AccessDenied</Code>"));
        assert!(body.contains("<Key>private/b.txt</Key>"));
        assert!(store.read_object("scoped-delete", "allowed/a.txt").await.is_err());
        assert!(store.read_object("scoped-delete", "private/b.txt").await.is_ok());

        let denied = super::bucket_route(
            axum::extract::State(store.clone()),
            axum::extract::Path("scoped-delete".to_string()),
            axum::extract::RawQuery(Some("delete".to_string())),
            axum::http::HeaderMap::new(),
            axum::http::Method::POST,
            None,
            Some(axum::Extension(authority)),
            None,
            None,
            Body::from(r#"<Delete><Object><Key>private/b.txt</Key></Object></Delete>"#),
        )
        .await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        assert!(store.read_object("scoped-delete", "private/b.txt").await.is_ok());
    }

    // ---------------------------------------------------------------------------
    // PUT /{bucket}/{key} with x-amz-copy-source — Object Copy
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn copy_object_creates_independent_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/src-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/dst-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Upload source
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/src-bucket/original.txt")
                    .header("content-type", "text/plain")
                    .header("x-amz-meta-source", "keep")
                    .header("x-amz-storage-class", "ONEZONE_IA")
                    .body(Body::from("copy me"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Copy
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/dst-bucket/copy.txt")
                    .header("x-amz-copy-source", "/src-bucket/original.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(
            body.contains("<CopyObjectResult"),
            "must return copy result XML"
        );
        assert!(body.contains("<ETag>"), "must include ETag");

        // Read the copy
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/dst-bucket/copy.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "copy me");

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/dst-bucket/copy.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get("x-amz-meta-source")
                .and_then(|v| v.to_str().ok()),
            Some("keep")
        );
        assert_eq!(
            res.headers()
                .get("x-amz-storage-class")
                .and_then(|v| v.to_str().ok()),
            Some("ONEZONE_IA")
        );

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/dst-bucket/replaced.txt")
                    .header("x-amz-copy-source", "/src-bucket/original.txt")
                    .header("x-amz-metadata-directive", "REPLACE")
                    .header("x-amz-meta-source", "replaced")
                    .header("x-amz-storage-class", "GLACIER")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/dst-bucket/replaced.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.headers()
                .get("x-amz-meta-source")
                .and_then(|v| v.to_str().ok()),
            Some("replaced")
        );
        assert_eq!(
            res.headers()
                .get("x-amz-storage-class")
                .and_then(|v| v.to_str().ok()),
            Some("GLACIER")
        );
    }

    #[tokio::test]
    async fn copy_object_missing_source_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cp-src")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cp-dst")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cp-dst/out.txt")
                    .header("x-amz-copy-source", "/cp-src/nonexistent.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchKey</Code>"));
    }

    #[tokio::test]
    async fn copy_object_honors_source_etag_preconditions() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond/source.txt")
                    .body(Body::from("copy condition source"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let etag = put
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond/ok-match.txt")
                    .header("x-amz-copy-source", "/copy-cond/source.txt")
                    .header("x-amz-copy-source-if-match", &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond/fail-match.txt")
                    .header("x-amz-copy-source", "/copy-cond/source.txt")
                    .header("x-amz-copy-source-if-match", "\"not-the-etag\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PRECONDITION_FAILED);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond/ok-none-match.txt")
                    .header("x-amz-copy-source", "/copy-cond/source.txt")
                    .header("x-amz-copy-source-if-none-match", "\"not-the-etag\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/copy-cond/fail-none-match.txt")
                    .header("x-amz-copy-source", "/copy-cond/source.txt")
                    .header("x-amz-copy-source-if-none-match", etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::PRECONDITION_FAILED);
    }

    // ---------------------------------------------------------------------------
    // Conditional GET / HEAD — If-None-Match, If-Modified-Since
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn get_if_none_match_returns_304_on_etag_match() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cond-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let put_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cond-bucket/file.txt")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let etag = put_res
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Matching ETag → 304
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/cond-bucket/file.txt")
                    .header("if-none-match", &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_MODIFIED);
        let body_bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert!(body_bytes.is_empty(), "304 must have no body");

        // Different ETag → 200 with body
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/cond-bucket/file.txt")
                    .header("if-none-match", "\"different-etag\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn head_if_none_match_returns_304_on_etag_match() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cond-head-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let put_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/cond-head-bucket/obj")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let etag = put_res
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let res = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/cond-head-bucket/obj")
                    .header("if-none-match", &etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn get_if_modified_since_returns_304_when_not_modified() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/ims-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/ims-bucket/file.txt")
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // A far-future date means "not modified since then" → 304
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ims-bucket/file.txt")
                    .header("if-modified-since", "Fri, 01 Jan 2100 00:00:00 GMT")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_MODIFIED);

        // A date in the past means the file was modified after that → 200
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ims-bucket/file.txt")
                    .header("if-modified-since", "Thu, 01 Jan 1970 00:00:00 GMT")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    // ---------------------------------------------------------------------------
    // GET /{bucket}/{key}?uploadId=... — List Parts
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn list_parts_returns_uploaded_part_numbers_and_etags() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/lp-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let init_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lp-bucket/big.bin?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let xml = body_text(init_res).await;
        let upload_id = extract_xml_tag(&xml, "UploadId").unwrap().to_string();

        // Upload 2 parts
        let p1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/lp-bucket/big.bin?uploadId={upload_id}&partNumber=1"
                    ))
                    .body(Body::from("part-one"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(p1.status(), StatusCode::OK);

        let p2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/lp-bucket/big.bin?uploadId={upload_id}&partNumber=2"
                    ))
                    .body(Body::from("part-two"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(p2.status(), StatusCode::OK);

        // List parts
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/lp-bucket/big.bin?uploadId={upload_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<ListPartsResult"));
        assert!(body.contains("<UploadId>"));
        let part_numbers = extract_all_xml_tags(&body, "PartNumber");
        assert!(part_numbers.contains(&"1".to_string()));
        assert!(part_numbers.contains(&"2".to_string()));
        assert!(body.contains("<ETag>"));
        assert!(body.contains("<Size>"));
    }

    #[tokio::test]
    async fn list_parts_invalid_upload_id_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/lp2-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lp2-bucket/key?uploadId=0_0000000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = body_text(res).await;
        assert!(body.contains("<Code>NoSuchUpload</Code>"));
    }

    // ---------------------------------------------------------------------------
    // GET /{bucket}?uploads — List Multipart Uploads
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn list_multipart_uploads_shows_initiated_uploads() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_app(&tmp);

        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/lmu-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Initiate two uploads
        let r1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lmu-bucket/file-a?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let xml1 = body_text(r1).await;
        let uid1 = extract_xml_tag(&xml1, "UploadId").unwrap().to_string();

        let r2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lmu-bucket/file-b?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let xml2 = body_text(r2).await;
        let uid2 = extract_xml_tag(&xml2, "UploadId").unwrap().to_string();

        // List uploads
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lmu-bucket?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = body_text(res).await;
        assert!(body.contains("<ListMultipartUploadsResult"));
        assert!(body.contains(&uid1));
        assert!(body.contains(&uid2));
        let keys = extract_all_xml_tags(&body, "Key");
        assert!(keys.contains(&"file-a".to_string()));
        assert!(keys.contains(&"file-b".to_string()));

        // After abort, upload disappears from the list
        app.clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/lmu-bucket/file-a?uploadId={uid1}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/lmu-bucket?uploads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_text(res).await;
        assert!(!body.contains(&uid1), "aborted upload must not appear");
        assert!(body.contains(&uid2), "active upload must still appear");
    }

    // ---------------------------------------------------------------------------
    // SigV4 presigned URL tests
    // ---------------------------------------------------------------------------

    const TEST_ACCESS_KEY: &str = "TESTKEY";
    const TEST_SECRET_KEY: &str = "TESTSECRET";
    const TEST_REGION: &str = "us-east-1";
    const TEST_HOST: &str = "localhost";

    fn make_auth_app(tmp: &tempfile::TempDir) -> axum::Router {
        let mut config = super::config::AppConfig::default();
        config.auth.enabled = true;
        config.auth.credentials.push(super::config::Credential {
            access_key: TEST_ACCESS_KEY.to_string(),
            secret_key: TEST_SECRET_KEY.to_string(),
        });
        // Configure the public hostname so presigned URL verification is
        // proxy-safe: the server substitutes this value for the `host` signed
        // header rather than reading the incoming HTTP Host header.
        config.auth.public_hostname = Some(TEST_HOST.to_string());
        router(
            LocalObjectStore::new(tmp.path()),
            std::sync::Arc::new(config),
        )
    }

    fn now_datetime() -> String {
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
    }

    /// Sends a regular SigV4-signed request through the app.
    async fn signed_request(
        app: axum::Router,
        method: &str,
        path: &str,
        query: &str,
        body: Body,
    ) -> axum::response::Response {
        let datetime = now_datetime();
        let auth = crate::server::auth::compute_auth_header(
            method,
            path,
            query,
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
        );
        let uri = if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        };
        app.oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("host", TEST_HOST)
                .header("x-amz-date", &datetime)
                .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                .header("authorization", auth)
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn presigned_get_valid_signature_returns_200_with_body() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        // Set up: create bucket + upload object using regular SigV4 auth.
        let res = signed_request(app.clone(), "PUT", "/ps-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = signed_request(
            app.clone(),
            "PUT",
            "/ps-bucket/secret.txt",
            "",
            Body::from("topsecret"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        // Generate valid presigned GET URL.
        let datetime = now_datetime();
        let qs = crate::server::auth::presign_query(
            "GET",
            "/ps-bucket/secret.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
            3600,
            &[],
        );

        // No `host` header — the server uses its configured public_hostname.
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/ps-bucket/secret.txt?{qs}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "topsecret");
    }

    #[tokio::test]
    async fn presigned_put_valid_signature_uploads_object() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/ps-put-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);

        // Presigned PUT.
        let datetime = now_datetime();
        let qs = crate::server::auth::presign_query(
            "PUT",
            "/ps-put-bucket/upload.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
            3600,
            &[],
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/ps-put-bucket/upload.txt?{qs}"))
                    .body(Body::from("via-presign"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Verify with regular auth.
        let res = signed_request(
            app.clone(),
            "GET",
            "/ps-put-bucket/upload.txt",
            "",
            Body::empty(),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "via-presign");
    }

    #[tokio::test]
    async fn presigned_put_with_signed_payload_hash_reaches_hash_validation() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/ps-hash-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);

        let expected_hash = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let datetime = now_datetime();
        let qs = crate::server::auth::presign_query_with_signed_headers(
            "PUT",
            "/ps-hash-bucket/upload.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
            3600,
            &[("host", TEST_HOST), ("x-amz-content-sha256", expected_hash)],
        );
        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/ps-hash-bucket/upload.txt?{qs}"))
                    .header("x-amz-content-sha256", expected_hash)
                    .body(Body::from("not-hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let body = body_text(res).await;
        assert!(body.contains("<Code>XAmzContentSHA256Mismatch</Code>"));
    }

    #[tokio::test]
    async fn presigned_put_rejects_missing_signed_payload_hash_header() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(
            app.clone(),
            "PUT",
            "/ps-missing-signed-header",
            "",
            Body::empty(),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let expected_hash = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let datetime = now_datetime();
        let qs = crate::server::auth::presign_query_with_signed_headers(
            "PUT",
            "/ps-missing-signed-header/upload.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
            3600,
            &[("host", TEST_HOST), ("x-amz-content-sha256", expected_hash)],
        );
        let res = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/ps-missing-signed-header/upload.txt?{qs}"))
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = body_text(res).await;
        assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"));
    }

    #[tokio::test]
    async fn presigned_url_expired_returns_403() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/ps-exp-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = signed_request(
            app.clone(),
            "PUT",
            "/ps-exp-bucket/f.txt",
            "",
            Body::from("x"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        // Signed at epoch 0 with 1-second expiry — expired long ago.
        let qs = crate::server::auth::presign_query(
            "GET",
            "/ps-exp-bucket/f.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            "19700101T000000Z",
            1,
            &[],
        );
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/ps-exp-bucket/f.txt?{qs}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = body_text(res).await;
        assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"));
    }

    #[tokio::test]
    async fn presigned_url_tampered_signature_returns_403() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/ps-tamp-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = signed_request(
            app.clone(),
            "PUT",
            "/ps-tamp-bucket/f.txt",
            "",
            Body::from("x"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let datetime = now_datetime();
        let mut qs = crate::server::auth::presign_query(
            "GET",
            "/ps-tamp-bucket/f.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
            3600,
            &[],
        );

        // Flip the last hex digit of X-Amz-Signature (which is always appended last).
        let last = qs.len() - 1;
        let flipped = if qs.as_bytes()[last] == b'a' {
            b'b'
        } else {
            b'a'
        };
        unsafe { qs.as_bytes_mut()[last] = flipped };

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/ps-tamp-bucket/f.txt?{qs}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = body_text(res).await;
        assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"));
    }

    #[tokio::test]
    async fn presigned_url_wrong_access_key_returns_403() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/ps-wk-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = signed_request(
            app.clone(),
            "PUT",
            "/ps-wk-bucket/f.txt",
            "",
            Body::from("x"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let datetime = now_datetime();
        let qs = crate::server::auth::presign_query(
            "GET",
            "/ps-wk-bucket/f.txt",
            TEST_HOST,
            "UNKNOWNKEY", // not registered in the server
            "anysecret",
            TEST_REGION,
            &datetime,
            3600,
            &[],
        );
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/ps-wk-bucket/f.txt?{qs}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn unauthenticated_request_when_auth_enabled_returns_403() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/any-bucket/any-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = body_text(res).await;
        assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"));
    }

    #[tokio::test]
    async fn regular_sigv4_auth_header_accepted() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/sigv4-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);

        let res = signed_request(
            app.clone(),
            "PUT",
            "/sigv4-bucket/obj.txt",
            "",
            Body::from("hello"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let res = signed_request(
            app.clone(),
            "GET",
            "/sigv4-bucket/obj.txt",
            "",
            Body::empty(),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body_text(res).await, "hello");
    }

    #[tokio::test]
    async fn presigned_url_works_even_when_proxy_rewrites_host_header() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let res = signed_request(app.clone(), "PUT", "/ps-proxy-bucket", "", Body::empty()).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = signed_request(
            app.clone(),
            "PUT",
            "/ps-proxy-bucket/f.txt",
            "",
            Body::from("data"),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        // Client signs with TEST_HOST ("localhost") — which is the server's public_hostname.
        let datetime = now_datetime();
        let qs = crate::server::auth::presign_query(
            "GET",
            "/ps-proxy-bucket/f.txt",
            TEST_HOST,
            TEST_ACCESS_KEY,
            TEST_SECRET_KEY,
            TEST_REGION,
            &datetime,
            3600,
            &[],
        );

        // Request arrives with a *different* Host header (simulating proxy rewrite).
        // Server uses its configured public_hostname for verification, so it still passes.
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/ps-proxy-bucket/f.txt?{qs}"))
                    .header("host", "internal-loadbalancer.corp:9999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::OK,
            "proxy-rewritten Host must not break presigned URL"
        );
        assert_eq!(body_text(res).await, "data");
    }

    #[tokio::test]
    async fn wrong_secret_key_in_auth_header_returns_403() {
        let tmp = tempfile::tempdir().unwrap();
        let app = make_auth_app(&tmp);

        let datetime = now_datetime();
        let auth = crate::server::auth::compute_auth_header(
            "GET",
            "/any-bucket/any-key",
            "",
            TEST_HOST,
            TEST_ACCESS_KEY,
            "WRONGSECRET",
            TEST_REGION,
            &datetime,
        );
        let res = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/any-bucket/any-key")
                    .header("host", TEST_HOST)
                    .header("x-amz-date", &datetime)
                    .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                    .header("authorization", auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }
