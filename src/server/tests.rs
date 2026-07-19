    use super::*;

    #[test]
    fn complete_parts_xml_accepts_quoted_etags() {
        let parts = parse_complete_parts_xml(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part><Part><PartNumber>3</PartNumber><ETag>def</ETag></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].number, 1);
        assert_eq!(parts[0].etag, "abc");
        assert_eq!(parts[1].number, 3);
    }

    #[test]
    fn complete_parts_xml_accepts_aws_cli_order() {
        let parts = parse_complete_parts_xml(
            r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><ETag>"abc"</ETag><PartNumber>1</PartNumber></Part><Part><ETag>"def"</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts[0].number, 1);
        assert_eq!(parts[0].etag, "abc");
        assert_eq!(parts[1].number, 2);
    }

    #[test]
    fn complete_parts_xml_accepts_xml_escaped_etags() {
        let parts = parse_complete_parts_xml(
            r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;900150983cd24fb0d6963f7d28e17f72&quot;</ETag></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts[0].number, 1);
        assert_eq!(parts[0].etag, "900150983cd24fb0d6963f7d28e17f72");
    }

    #[test]
    fn human_bytes_formats_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
    }

    #[test]
    fn qps_formats_window_rate() {
        assert_eq!(qps(0), "0.00");
        assert_eq!(qps(25), "2.50");
    }

    #[test]
    fn log_content_length_uses_put_request_size() {
        let headers = HeaderMap::new();
        assert_eq!(log_content_length(&Method::PUT, None, 11, &headers), "11");
        assert_eq!(
            log_content_length(&Method::PUT, Some("7"), 33, &headers),
            "7"
        );
    }

    #[test]
    fn log_content_length_uses_response_size_for_non_put() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("10805888"));
        assert_eq!(
            log_content_length(&Method::HEAD, None, 0, &headers),
            "10805888"
        );
    }

    #[test]
    fn operation_log_classifies_common_s3_requests() {
        let uri: axum::http::Uri = "/bucket/object.txt".parse().unwrap();
        assert_eq!(
            operation_and_target(&Method::PUT, &uri, false),
            ("UPLOAD", "/bucket/object.txt".to_string())
        );
        assert_eq!(
            operation_and_target(&Method::DELETE, &uri, false),
            ("DELETE", "/bucket/object.txt".to_string())
        );
        let list: axum::http::Uri = "/bucket?list-type=2&prefix=objects%2F".parse().unwrap();
        assert_eq!(
            operation_and_target(&Method::GET, &list, false),
            ("LIST", "/bucket/objects/".to_string())
        );
        let bucket: axum::http::Uri = "/bucket".parse().unwrap();
        assert_eq!(
            operation_and_target(&Method::PUT, &bucket, false).0,
            "CREATE_BUCKET"
        );
        assert_eq!(
            operation_and_target(&Method::DELETE, &bucket, false).0,
            "DELETE_BUCKET"
        );
    }

    #[test]
    fn operation_actor_includes_username_and_access_key() {
        assert_eq!(
            OperationActor {
                username: Some("hms_test".to_string()),
                access_key: Some("AK323434".to_string()),
            }
            .label(),
            "hms_test/AK323434"
        );
        assert_eq!(OperationActor::default().label(), "anonymous");
    }

    #[test]
    fn object_content_encoding_strips_aws_chunked_framing() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        assert_eq!(object_content_encoding(&headers).as_deref(), Some("gzip"));

        headers.insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static("aws-chunked"),
        );
        assert_eq!(object_content_encoding(&headers), None);

        headers.insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static("aws-chunked, gzip"),
        );
        assert_eq!(object_content_encoding(&headers).as_deref(), Some("gzip"));
    }

    #[test]
    fn s3_query_parser_preserves_literal_plus() {
        let query = parse_s3_query("prefix=a+b%2Bc&delimiter=%2F");
        assert_eq!(query.get("prefix").map(String::as_str), Some("a+b+c"));
        assert_eq!(query.get("delimiter").map(String::as_str), Some("/"));
    }

        
