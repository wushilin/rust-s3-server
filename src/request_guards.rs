use std::convert::Infallible;
use lazy_static::lazy_static;
use regex::Regex;
use rocket::{request::FromRequest, http::Status};

#[derive(Debug, Clone)]
pub struct RangeHeader {
    pub begin: Option<usize>,
    pub end: Option<usize>,
}

lazy_static! {
    pub static ref RANGE_START:Regex = Regex::new(r"^bytes=(\d+)-$").unwrap();
    pub static ref RANGE_START_END:Regex = Regex::new(r"^bytes=(\d+)-(\d+)$").unwrap();
}

lazy_static! {
    pub static ref CREATE_BUCKET_URI_PATTERN:Regex = {
        Regex::new(r"^[a-zA-Z0-9.\\-_]{1,255}$").unwrap()
    };
}

#[derive(Debug)]
pub struct ChunkedEncoding(pub bool);

impl RangeHeader {
    fn match_str_and_get_group(input:&str, r:&Regex) -> Option<Vec<String>> {
        match r.captures(input) {
            Some(group) => {
                let number_of_groups = group.len();
                let mut result = Vec::with_capacity(number_of_groups);
                for i in 0..number_of_groups {
                    result.push(group.get(i).unwrap().as_str().to_string());
                }
                Some(result)
            },
            None => None
        }
    }
    fn parse_range(hdr:Option<&str>) -> (Option<usize>, Option<usize>) {
        if hdr.is_none() {
            return (None, None);
        }
    
        let hdr_str = hdr.unwrap();
    
        let start_try = Self::match_str_and_get_group(hdr_str, &RANGE_START);
        if start_try.is_some() {
            let start_try = start_try.unwrap();
            let g1 = start_try.get(1).unwrap();
            let parse_result:usize =  g1.parse().unwrap();
            return (Some(parse_result), None);
        }
    
        let start_end_try = Self::match_str_and_get_group(hdr_str, &RANGE_START_END);
        if start_end_try.is_some() {
            let start_end_try = start_end_try.unwrap();
            let g1 = start_end_try.get(1).unwrap();
            let g2 = start_end_try.get(2).unwrap();
            let parse1:usize = g1.parse().unwrap();
            let parse2:usize = g2.parse().unwrap();
            return (Some(parse1), Some(parse2));
        }
        (None, None)
    }
    
}
#[rocket::async_trait]
impl<'r> FromRequest<'r> for ChunkedEncoding {
    type Error = Infallible;
    //Transfer-Encoding
    async fn from_request(request: &'r rocket::Request<'_>) -> 
    rocket::request::Outcome<Self, Self::Error> {
        let range = request.headers().get_one("transfer-encoding");
        let flag1 = range.is_some() && range.unwrap().to_ascii_lowercase() == "chunked";

        let decoded_length = request.headers().get_one("x-amz-decoded-content-length");
        let content_length = request.headers().get_one("content-length");
        let mut flag2 = false;
        if decoded_length.is_some() && content_length.is_some() {
            let decoded_length = decoded_length.unwrap();
            let content_length = content_length.unwrap();
            if decoded_length.len() > 0 && content_length.len() > 0 {
                let decoded_length:i32 = decoded_length.parse().unwrap();
                let content_length: i32 = content_length.parse().unwrap();
                flag2 = content_length > decoded_length;
            }

        }
        rocket::request::Outcome::Success(
            ChunkedEncoding(flag1 || flag2) 
        )
    }
}
#[rocket::async_trait]
impl<'r> FromRequest<'r> for RangeHeader {
    type Error = Infallible;

    async fn from_request(request: &'r rocket::Request<'_>) -> 
        rocket::request::Outcome<Self, Self::Error> {
        
        let range = request.headers().get_one("range");
        let (begin, end) = Self::parse_range(range);

        
        rocket::request::Outcome::Success(
            RangeHeader { 
                begin, end
            }
        )
    }
}



#[derive(Debug, Clone)]
pub struct CreateBucket(bool);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for CreateBucket {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_put =  method == "PUT";
        let segments = request.uri().path().segments().len();
        if is_put && segments == 1 {
            rocket::request::Outcome::Success(
                CreateBucket(true)
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}
pub struct ListBucket;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for ListBucket {
    type Error = Infallible;
    
    #[allow(unused)]
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
            rocket::request::Outcome::Success(
                ListBucket
            )
    }
}

pub struct PutObject;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for PutObject {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let segments = request.uri().path().segments().len();

        let method = request.method().as_str();
        let is_put =  method == "PUT";
        let upload_id = request.query_value::<String>("uploadId");
        let part_number = request.query_value::<String>("partNumber");
        if is_put && part_number.is_none() && upload_id.is_none() && segments > 1 {
            rocket::request::Outcome::Success(
                PutObject
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}
pub struct CreateMultipartUpload;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for CreateMultipartUpload {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_post =  method == "POST";
        let segments = request.uri().path().segments().len();
        let uploads = request.query_value::<String>("uploads");
        if is_post && segments > 1 && uploads.is_some() {
            rocket::request::Outcome::Success(
                CreateMultipartUpload
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}
pub struct UploadMultipart;

pub struct AbortMultipartUpload;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for AbortMultipartUpload {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_delete =  method == "DELETE";
        let segments = request.uri().path().segments().len();
        let upload_id = request.query_value::<String>("uploadId");
        if is_delete && segments == 1 && upload_id.is_some() {
            rocket::request::Outcome::Success(
                AbortMultipartUpload
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}

pub struct DeleteObjects;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for DeleteObjects {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_delete =  method == "POST";
        let segments = request.uri().path().segments().len();
        let delete = request.query_value::<String>("delete");
        if is_delete && segments <= 2 && delete.is_some(){
            rocket::request::Outcome::Success(
                DeleteObjects
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}
pub struct DeleteObject;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for DeleteObject {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_delete =  method == "DELETE";
        let segments = request.uri().path().segments().len();
        if is_delete && segments > 1 {
            rocket::request::Outcome::Success(
                DeleteObject
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}

pub struct CompleteMultipartUpload;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for CompleteMultipartUpload {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_post =  method == "POST";
        let segments = request.uri().path().segments().len();
        let upload_id = request.query_value::<String>("uploadId");
        if is_post && segments > 1 && upload_id.is_some() {
            rocket::request::Outcome::Success(
                CompleteMultipartUpload
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}

pub struct GetObject;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for GetObject {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_get =  method == "GET";
        let segments = request.uri().path().segments().len();
        let location = request.query_value::<String>("location");
        if is_get && location.is_none() && segments > 1{
            rocket::request::Outcome::Success(
                GetObject
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}

pub struct GetBucketLocation;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for GetBucketLocation {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_delete =  method == "GET";
        let segments = request.uri().path().segments().len();
        let location = request.query_value::<String>("location");
        if is_delete && location.is_some() && segments == 1{
            rocket::request::Outcome::Success(
                GetBucketLocation
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}

pub struct PutObjectPart;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for PutObjectPart {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_put =  method == "PUT";
        let segments = request.uri().path().segments().len();

        let upload_id = request.query_value::<String>("uploadId");
        let part_number = request.query_value::<String>("partNumber");
        if is_put && part_number.is_some() && segments > 1 && upload_id.is_some() {
            rocket::request::Outcome::Success(
                PutObjectPart
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}
pub struct ListObjects;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for ListObjects {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_get =  method == "GET";
        let segments = request.uri().path().segments().len();
        let location = request.query_value::<String>("location");
        let list_type = request.query_value::<String>("list-type");
        if is_get && location.is_none() && segments > 0 && list_type.is_none(){
            rocket::request::Outcome::Success(
                ListObjects
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}


pub struct ListObjectsV2;
#[rocket::async_trait]
impl<'r> FromRequest<'r> for ListObjectsV2 {
    type Error = Infallible;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let method = request.method().as_str();
        let is_get =  method == "GET";
        let segments = request.uri().path().segments().len();
        let location = request.query_value::<String>("location");
        let list_type = request.query_value::<String>("list-type");
        if is_get && location.is_none() && segments > 0 && list_type.is_some() && list_type.unwrap().unwrap() == "2" {
            rocket::request::Outcome::Success(
                ListObjectsV2
            )
        } else {
            rocket::request::Outcome::Forward(())
        }
    }
}
pub struct AuthCheckPassed;
#[derive(Debug)]
pub enum AuthError {
    NoKeySupplied,
    InvalidKey,
    InvalidSignature,
}
#[rocket::async_trait]
impl<'r> FromRequest<'r> for AuthCheckPassed {
    type Error = AuthError;
    
    async fn from_request(request: &'r rocket::Request<'_>) ->
        rocket::request::Outcome<Self, Self::Error> {
        let api_key = request.headers().get_one("x-api-key");
        if api_key.is_some() {
            if api_key.unwrap() == "secret" {
                return rocket::request::Outcome::Success(AuthCheckPassed);
            } else {
                return rocket::request::Outcome::Failure((Status::Forbidden, AuthError::InvalidKey));
            }
        } else {
            return rocket::request::Outcome::Failure((Status::Forbidden, AuthError::NoKeySupplied));
        }
    }
}
