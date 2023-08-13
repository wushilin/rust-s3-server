pub mod fsapi;
pub mod sequencing;
pub mod cachedfile;
pub mod s3error;
pub mod s3resp;
pub mod chunk_to_raw;
pub mod request_guards;
pub mod ioutil;
pub mod states;

use asyncio_utils::LimitSeekerReader;
use lazy_static::__Deref;
use regex::Regex;
use states::CliArg;
use request_guards::{ChunkedEncoding, CreateBucket, AbortMultipartUpload, AuthCheckPassed, CompleteMultipartUpload, CreateMultipartUpload, GetObject};
use s3resp::S3Response;
use cachedfile::CachedFile;
use rocket::Data;
use rocket::response::Debug;
use ubyte::ToByteUnit;
use std::error::Error;
use std::io::SeekFrom;

use fsapi::{FS, Bucket};
#[macro_use] 
extern crate rocket;

extern crate log;
#[allow(unused)]
use log::{info, debug, warn, error, trace};

use rocket::State;

#[derive(FromForm, Debug)]
struct ListQuery {
    delimiter: Option<String>,
    #[allow(unused)]
    #[field(name = "encoding-type")]
    encoding_type: Option<String>,
    marker:Option<String>,
    #[field(name = "max-keys")]
    max_keys:Option<usize>,
    prefix: Option<String>,
    #[field(name="list-type")]
    list_type: Option<i32>,
    #[field(name="continuation-token")]
    continuation_token: Option<String>
}
#[get("/<bucket>?<query..>", rank=99)]
async fn list_objects_ext(bucket:String, query:ListQuery, #[allow(unused)] marker:request_guards::ListObjects, fs:&State<FS>) 
    -> Option<String> {
    let action = "list_objects_ext";
    info!("{action}: bucket `{bucket}`, query `{query:?}`");
    return list_objects(bucket, query, fs).await;
}

#[get("/<bucket>?<query..>", rank=98)]
async fn list_objects_ext_v2(bucket:String, query:ListQuery, #[allow(unused)] marker:request_guards::ListObjectsV2, fs:&State<FS>) 
-> Option<String> {
    let action = "list_objects_ext_v2";
    info!("{action}: bucket `{bucket}`, query `{query:?}`");
    return list_objects_v2(bucket, query, fs).await;
}

fn list_bucket<'a>(bucket:&'a Bucket, prefix:&str, delimiter:&Option<String>, after:&str, limit:usize) -> Vec<fsapi::S3Object<'a>> {
    match delimiter {
        Some(_str) => {
            bucket.list_objects_short(prefix, after, limit)
        },
        None => {
            bucket.list_objects(prefix, after, limit)
        }
    }
}

async fn list_objects_v2(bucket:String, query:ListQuery, fs:&State<FS>) -> Option<String> {
    let action = "list_objects_v2";
    let bucket_arg = bucket.clone();
    let bucket = fs.get_bucket(&bucket);
    match bucket {
        None => {
            return None;
        }
        _ => {

        }
    }
    #[allow(unused)]
    let list_type = query.list_type.unwrap_or(-1);

    let bucket = bucket.unwrap();
    let actual_prefix:String = query.prefix.unwrap_or(String::from(""));
    let ct = query.continuation_token.unwrap_or(String::from(""));
    let max_keys = query.max_keys.unwrap_or(100);
    let objects:Vec<fsapi::S3Object> = list_bucket(&bucket, &actual_prefix, &query.delimiter, &ct, max_keys);
    let mut is_truncated = false;
    let delimiter = query.delimiter.unwrap_or(String::from(""));
    if delimiter != "/" && delimiter != "" {
        panic!("delimeter must be / or empty");
    }
    let mut objects_string = String::new();
    info!("{action}: returned {} objects", objects.len());
    let mut next_ct = "".to_string();
    for next in objects.iter() {
        let entry_xml = next.format().await;
        objects_string.push_str(&entry_xml);
    }
    if objects.len() > 0 {
        let last_obj = objects.last().unwrap();
        next_ct = last_obj.object_key().to_string();
        is_truncated = true;
        next_ct = format!("<NextContinuationToken>{next_ct}</NextContinuationToken>");
    }
    let copy_count = objects.len();
    let result = format!(r###"<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>{bucket_arg}</Name>
      <Prefix>{actual_prefix}</Prefix>
      <KeyCount>{copy_count}</KeyCount>
      <ContinuationToken>{ct}</ContinuationToken>
      {next_ct}
      <MaxKeys>{max_keys}</MaxKeys>
      <Delimiter>{delimiter}</Delimiter>
      <IsTruncated>{is_truncated}</IsTruncated>
        {objects_string}
      <EncodingType>url</EncodingType>
    </ListBucketResult>
    "###);   
    println!("{result}");
    return Some(result);    
}
async fn list_objects(bucket:String, query:ListQuery, fs:&State<FS>) -> Option<String> {
    let action = "list_objects";
    let bucket_arg = bucket.clone();
    let bucket = fs.get_bucket(&bucket);
    match bucket {
        None => {
            return None;
        }
        _ => {

        }
    }
    #[allow(unused)]
    let list_type = query.list_type.unwrap_or(-1);

    let bucket = bucket.unwrap();
    let actual_prefix:String = query.prefix.unwrap_or(String::from(""));
    let marker = query.marker.unwrap_or(String::from(""));
    let max_keys = query.max_keys.unwrap_or(100);
    let objects:Vec<fsapi::S3Object> = list_bucket(&bucket, &actual_prefix, &query.delimiter, &marker, max_keys);
    let mut is_truncated = false;
    let delimiter = query.delimiter.unwrap_or(String::from(""));
    if delimiter != "/" && delimiter != "" {
        panic!("delimeter must be / or empty");
    }
    let mut objects_string = String::new();
    info!("{action}: returned {} objects", objects.len());
    let mut next_marker = "".to_string();
    for (index, next) in objects.iter().enumerate() {
        let entry_xml = next.format().await;
        objects_string.push_str(&entry_xml);
    }
    if objects.len() > 0 {
        let last_obj = objects.last().unwrap();
        next_marker = last_obj.object_key().to_string();
        is_truncated = true;
        next_marker = format!("<NextMarker>{next_marker}</NextMarker>");
    }
    let result = format!(r###"<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>{bucket_arg}</Name>
      <Prefix>{actual_prefix}</Prefix>
      <Marker>{marker}</Marker>
      {next_marker}
      <MaxKeys>{max_keys}</MaxKeys>
      <Delimiter>{delimiter}</Delimiter>
      <IsTruncated>{is_truncated}</IsTruncated>
        {objects_string}
      <EncodingType>url</EncodingType>
    </ListBucketResult>
    "###);   
    println!("{result}");
    return Some(result);    
}

#[get("/<bucket>?<location>", rank=4)]
async fn get_bucket_location(bucket:String, #[allow(unused)] marker:request_guards::GetBucketLocation, #[allow(unused)] location:String) -> Option<String> {
    let action = "get_bucket_location";
    info!("{action}: bucket: `{bucket}`. using hardcoded `ap-southeast-1`");
    return Some(String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">ap-southeast-1</LocationConstraint>
        "#
    ));
}

#[allow(non_snake_case)]
#[put("/<bucket>/<key..>?<partNumber>&<uploadId>", data = "<data>", rank=3)]
async fn put_object_part<'a>(bucket:String, key:std::path::PathBuf, partNumber:u32, uploadId:String, chunked:ChunkedEncoding,
    fs:&State<FS>, #[allow(unused)] marker:request_guards::PutObjectPart, data: rocket::data::Data<'a>) 
    -> Result<s3resp::S3Response<'a>, Debug<Box<dyn Error>>> {
    let action = "put_object_part";
    let key = key.to_str().unwrap();
    info!("{action}: object `{bucket}/{key}` uploadId `{uploadId}`, partNumber `{partNumber}`, chunked `{chunked:?}`");
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        info!("{action}: bucket `{bucket}` not found");
        return Ok(S3Response::not_found())
    }


    let bucket_obj = bucket_obj.unwrap();
    use rocket::data::ToByteUnit;
    let mut data_stream = data.open(5.gigabytes());
    let write_result = bucket_obj.save_object_part(&key, &uploadId, partNumber, &mut data_stream, chunked.0).await;
    if write_result.is_err() {
        info!("{action}: io error: {}", write_result.err().unwrap());
        return Ok(S3Response::server_error());
    }
    let wr = write_result.unwrap();
    info!("{action}: `{bucket}/{key}` `{uploadId}@{partNumber}` written {} bytes, md5 is `{}`", wr.0, wr.1);
    //data_stream.stream_to(writer).await;
    let mut ok = S3Response::ok();
    ok.add_header("ETag", &format!("{}", wr.1));
    return Ok(ok);
}

#[put("/<bucket>/<key..>", data = "<data>", rank=2)]
async fn put_object<'a>(bucket:String, key:std::path::PathBuf, 
    chunked:ChunkedEncoding,
    fs:&State<FS>,
    #[allow(unused)]
    marker:request_guards::PutObject,
    data: rocket::data::Data<'a>) -> Result<s3resp::S3Response<'a>, Debug<Box<dyn Error>>> {
    let action = "put_object";
    let key = key.to_str().unwrap();
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket {bucket} does not exist");
        return Ok(S3Response::not_found())
    }


    let bucket = bucket_obj.unwrap();
    use rocket::data::ToByteUnit;
    let mut data_stream = data.open(5.gigabytes());
    //test(&data_stream);
    let write_result = bucket.save_object(&key, &mut data_stream, chunked.0).await;
    if write_result.is_err() {
        warn!("{action}: io error: {}", write_result.err().unwrap());
        return Ok(S3Response::server_error());
    }
    let wr = write_result.unwrap();
    info!("{action}: `{key}` written {} bytes, md5 is `{}`", wr.0, wr.1);
    //data_stream.stream_to(writer).await;
    let mut ok = S3Response::ok();
    ok.add_header("ETag", &format!("{}", wr.1));
    return Ok(ok);

}


#[delete("/<bucket>?<uploadId>", rank=2)]
async fn cleanup_multipart_upload<'r>(bucket:String,
    #[allow(unused)]
    marker:AbortMultipartUpload,
    fs:&State<FS>,
    #[allow(non_snake_case)]
    uploadId:String) -> Result<S3Response<'r>, Debug<Box<dyn Error>>> {

    let action = "cleanup_multipart_upload";
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket `{bucket}` not found");
        return Err(Debug(Box::new(s3error::S3Error::not_found())));
    }
    let bucket = bucket_obj.unwrap();

    bucket.cleanup_upload_id(&uploadId);
    info!("{action}: cleaned up upload id {uploadId}");
    Ok(S3Response::ok())
}

#[allow(non_snake_case)]
#[post("/<bucket>/<key_full..>?<uploadId>", data="<data>", rank=5)]
async fn complete_multipart_upload<'a>(bucket:String, key_full:std::path::PathBuf, uploadId:String, 
    #[allow(unused)] data:rocket::data::Data<'a>, fs:&State<FS>,
    #[allow(unused)] marker:CompleteMultipartUpload
) -> Result<S3Response<'a>, Debug<Box<dyn Error>>> {
    let action = "complete_multipart_upload";
    let key = key_full.to_str().unwrap().to_string();
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket `{bucket}` not found");
        return Ok(S3Response::not_found());
    }

    let bucket_obj = bucket_obj.unwrap();
    
    let (size, hash) = bucket_obj.merge_part(&key, &uploadId).await?;
    info!("{action}: merged `{uploadId}` into `{bucket}/{key}`, {size}+`{hash}`");
    let response = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
     <Location>http://127.0.0.1:8000/{bucket}/{key}</Location>
     <Bucket>{bucket}</Bucket>
     <Key>{key}</Key>
     <ETag>"{hash}"</ETag>
    </CompleteMultipartUploadResult>"#);

    let mut ok = S3Response::ok();
    ok.body(&response);
    return Ok(ok);

}

#[post("/<bucket>/<key_full..>?<uploads>", data="<data>", rank=99)]
async fn create_multipart_upload<'a>(bucket:String, fs:&State<FS>, key_full:std::path::PathBuf, 
    #[allow(unused)] uploads:String, 
    #[allow(unused)] marker: CreateMultipartUpload,
    #[allow(unused)] data:Data<'a>) -> Result<S3Response<'a>, Debug<Box<dyn Error>>> {
    let action = "create_multipart_upload";
    let key = key_full.to_str().unwrap().to_string();
    
    let bucket_name = bucket.clone();
    info!("{action}: object: `{bucket}/{key}`");
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket `{bucket}` not found");
        return Err(Debug(Box::new(s3error::S3Error::not_found())));
    }

    let bucket = bucket_obj.unwrap();
    let part = bucket.gen_upload_id();
    info!("{action}: uploadId -> `{part}`");
    let result = format!(r###"<?xml version="1.0" encoding="UTF-8"?>
    <InitiateMultipartUploadResult>
       <Bucket>{bucket_name}</Bucket>
       <Key>{key}</Key>
       <UploadId>{part}</UploadId>
    </InitiateMultipartUploadResult>"###);

    let mut ok = S3Response::ok();
    ok.body(&result);
    
    return Ok(ok);
}

#[get("/<bucket>/<key_full..>")]
async fn get_object(bucket:String, 
    #[allow(unused)]
    marker: GetObject,
    key_full:std::path::PathBuf, fs:&State<FS>, range:request_guards::RangeHeader) -> Option<CachedFile> {
    let action = "get_object";
    info!("{action}: bucket: `{bucket}`, key: `{key_full:?}`, range: `{range:?}`");
    let key = key_full.to_str().unwrap().to_string();
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket `{bucket}` not found");
        return None;
    }
    let bucket_obj = bucket_obj.unwrap();
    let obj = bucket_obj.get_object_by_key(&key);
    if obj.is_none() {
        warn!("{action}: object `{bucket}/{key}` not found");
        return None;
    }
    let obj = obj.unwrap();
    let path = obj.path().as_path();
    let mut tokiof = tokio::fs::File::open(path).await.unwrap();
    //return Some(NamedFile::open(obj.path().as_path().to_str().unwrap());
    let etag = obj.checksum().unwrap();
    let last_modified = obj.meta().unwrap().modified().unwrap();
    let file_size = obj.len().unwrap();
    let mut content_length: usize = file_size as usize;
    let begin = range.begin;
    let end = range.end;
    
    if begin.is_some() {
        use tokio::io::AsyncSeekExt;
        let seek_target = begin.unwrap();
        let seek_result = tokiof.seek(SeekFrom::Start(seek_target as u64)).await;
        match seek_result {
            Err(some_err) => {
                error!("{action}: seek to {seek_target} failed: {some_err:?}");
                return None;
            },
            Ok(_) => {}
        }
        if end.is_some() {
            // x -y range
            content_length = end.unwrap() - begin.unwrap() + 1;
        } else {
            content_length = file_size as usize - begin.unwrap() as usize;
        }
    }
    info!("{action}: delegating to `LimitedReader` with content-length -> `{content_length}`");
    let reader = LimitSeekerReader::new(tokiof, Some(content_length));
    return Some(CachedFile {
        reader: reader, 
        etag, 
        modified_time: last_modified,
        size: content_length,
        file_name: obj.get_short_name(),
        partial: begin.is_some() || end.is_some(),
    });
}

fn parse_delete_keys(data:&str) -> Vec<(String, String)> {
    let data = data.replace("\r", "");
    let data = data.replace("\n", "");
    println!("Parsing {data}");
    let p = r"<Object>\s*<Key>(\S+?)</Key>\s*(<VersionId>\s*(\S+?)\s*</VersionId>)?\s*</Object>";

    let pattern = Regex::new(p).unwrap();
    let mut start = 0;
    let mut result = vec!();
    loop {
        let find_result = pattern.captures_at(&data, start);
        if find_result.is_none() {
            break;
        }

        let find_result = find_result.unwrap();
        let match_full_length = find_result.get(0).unwrap().end();
        start = match_full_length;

        let key = find_result.get(1).unwrap().as_str();
        let version = find_result.get(3);
        let mut version_string = "";
        if version.is_some() {
            version_string = version.unwrap().as_str();
        }
        result.push((String::from(key), String::from(version_string)));
    }
    info!("{:?}", result);
    return result;
}


#[post("/<bucket>/<empty>?<delete>", data="<data>", rank=4)]
async fn delete_object_multi_alt<'r>(
    bucket:String, 
    #[allow(unused)]
    delete:String,
    empty:String,
    #[allow(unused)]
    marker:request_guards::DeleteObjects, fs:&State<FS>,
    data: Data<'_>) -> Result<S3Response<'r>, Debug<Box<dyn Error>>> {
    return delete_object_multi(bucket, delete, marker, fs,  data).await;

}
#[post("/<bucket>?<delete>", data="<data>", rank=3)]
#[allow(non_snake_case)]
async fn delete_object_multi<'r>(bucket:String, 
    #[allow(unused)]
    delete:String,
    #[allow(unused)]
    marker:request_guards::DeleteObjects, fs:&State<FS>,
    data: Data<'_>
) -> Result<S3Response<'r>, Debug<Box<dyn Error>>> {
    let action = "delete_object_multi";
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket `{bucket}` not found");
        return Ok(S3Response::not_found());
    }

    let bucket_obj = bucket_obj.unwrap();
    let body = data.open(10.mebibytes()).into_string().await;
    if body.is_err() {
        warn!("{action}: body parse error: {}", body.err().unwrap());
        return Ok(S3Response::invalid_request());
    }
    let body = body.unwrap();
    let body = body.as_str();
    let keys_and_versions = parse_delete_keys(body);
    let mut result = true;
    for (key, version) in &keys_and_versions {
        if version != "" {
            warn!("{action}: versioning not supported but `{version}` is requested.");
        }
        let local_result = bucket_obj.delete_object(&key);
        if !local_result {
            result = false;
        }
    }
    let mut item_string = String::new();
    for (key, version) in &keys_and_versions {
        if version == "" {
            let my_str = format!(r#"<Deleted>
            <Key>{key}</Key>
          </Deleted>"#, );
          item_string.push_str(&my_str);
        } else {
            let my_str = format!(r#"<Deleted>
            <DeleteMarker>false</DeleteMarker>
            <Key>{key}</Key>
            <VersionId>{version}</VersionId>
            </Deleted>"#);
            item_string.push_str(&my_str);
        }
    }
    let response = format!(r###"<?xml version="1.0" encoding="UTF-8"?>
    <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
     {item_string}
    </DeleteResult>"###);
    if result {
        info!("{action}: delete `{bucket}/{keys_and_versions:?}` succeeded.");
    } else {
        warn!("{action}: delete `{bucket}/{keys_and_versions:?}` not found/error.");
    }
    let mut ok = S3Response::ok();
    ok.body(&response);
    return Ok(ok);
}
#[delete("/<bucket>/<full_key..>?<versionId>", rank=1)]
#[allow(non_snake_case)]
async fn delete_object<'r>(bucket:String, full_key:std::path::PathBuf, 
    #[allow(non_snake_case)]
    #[allow(unused)]
    versionId:Option<String>,
    #[allow(unused)]
    marker:request_guards::DeleteObject, fs:&State<FS>
) -> Result<S3Response<'r>, Debug<Box<dyn Error>>> {
    let action = "delete_object";
    let key = full_key.to_str().unwrap().to_string();
    let bucket_obj = fs.get_bucket(&bucket);
    if bucket_obj.is_none() {
        warn!("{action}: bucket `{bucket}` not found");
        return Ok(S3Response::not_found());
    }

    let bucket_obj = bucket_obj.unwrap();
    let result = bucket_obj.delete_object(&key);
    if result {
        info!("{action}: delete `{bucket}/{key}` succeeded.");
        return Ok(S3Response::ok());
    } else {
        warn!("{action}: delete `{bucket}/{key}` not found/error.");
        return Ok(S3Response::not_found());
    }
}

#[put("/<bucket>", data = "<data>", rank=1)]
async fn create_bucket<'r>(bucket:String, 
    #[allow(unused)] 
    marker:CreateBucket, fs:&State<FS>,
    #[allow(unused)]
    data: rocket::data::Data<'r>
) -> Result<S3Response<'r>, Debug<Box<dyn Error>>> {
    let action = "create_bucket";
    info!("{action}: creating bucket `{bucket}`");
    let cr = fs.make_bucket(&bucket);
    match cr {
        Ok(_) => {
            info!("{action}: creating bucket `{bucket}` ok");
        },
        Err(some_err) => {
            error!("{action}: creating bucket `{bucket}` err: `{some_err}`");
        }
    }
    Ok(S3Response::ok())
}

#[get("/backdoor?<secret_token>")]
async fn backdoor(secret_token:String, 
    #[allow(unused)] auth:AuthCheckPassed) -> String {
    let action = "backdoor";
    info!("{action}: backdoor accessed `{secret_token}` revealed");
    format!("Secret Revealed: {secret_token}")
}


#[get("/")]
async fn list_all_buckets<'r>(fs:&State<FS>) -> Result<S3Response<'r>, Debug<Box<dyn Error>>> {
    let action = "list_all_buckets";
    let mut buckets = String::new();
    let bucket_list = fs.get_all_buckets();
    info!("{action}: found {} buckets", bucket_list.len());
    for (name, bucket) in bucket_list.iter() {
        let creation_time = bucket.get_creation_time()?;
        let next_bucket = format!(r#"      
        <Bucket>
            <CreationDate>{creation_time}</CreationDate>
            <Name>{name}</Name>
        </Bucket>"#);
        buckets.push_str(&next_bucket);

    }

    let body = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
    <ListAllMyBucketsResult><Buckets>{buckets}</Buckets></ListAllMyBucketsResult>"#);
    let mut ok = S3Response::ok();
    ok.body(&body);

    Ok(ok)
}

pub fn setup_logger(log_conf_file: &str) -> Result<(), Box<dyn Error>> {
    if log_conf_file.len() > 0 {
        log4rs::init_file(log_conf_file, Default::default())?;
        println!("logs will be sent according to config file {log_conf_file}");
    } else {
        println!("no log config file specified.");
        use log::LevelFilter;
        use log4rs::append::console::ConsoleAppender;
        use log4rs::config::{Appender, Config, Root};
        let stdout = ConsoleAppender::builder().build();

        let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(LevelFilter::Info))
        .unwrap();
        let _ = log4rs::init_config(config)?;
    }
    Ok(())
}

#[launch]
fn rocket() -> _ {
    use clap::Parser;
    let args = CliArg::parse();

    setup_logger(args.log4rs_config_file()).unwrap();
    //trace!("I AM TRACE");
    //debug!("I AM DEBUG");
    //info!("I AM INFO");
    //warn!("I AM WARN");
    //error!("I AM ERROR");
    
    let mut base_fs = FS::new();
    base_fs.set_base(args.base_dir());
    base_fs.initialize();


    let figment = rocket::Config::figment()
        .merge(("port", args.bind_port()))
        .merge(("address", args.bind_address()))
        .merge(("log_level", "normal"))
        ;
    rocket::custom(figment)
        .manage(args)
        .manage(base_fs)
        .mount("/", routes![list_objects_ext, list_objects_ext_v2, get_object, 
        put_object, put_object_part, create_multipart_upload, 
        cleanup_multipart_upload, list_all_buckets,
        create_bucket, delete_object, delete_object_multi, delete_object_multi_alt, get_bucket_location,
        complete_multipart_upload, backdoor])
}
