use std::path::PathBuf;
use std::error::Error;
use std::fs::{Metadata, metadata, self};
use crate::s3error::S3Error;
use crate::sequencing::{self, Sequence};
use crate::chunk_to_raw::copy_chunk_to_raw;

use std::collections::HashMap;
use rand::{distributions::Alphanumeric, Rng};
use serde_json;
use md5::{Md5, Digest};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::fmt;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::fs as tokiofs;
use std::sync::Arc;
use std::io::Read;
use std::io::Write;
use chrono::offset::Utc;
use chrono::DateTime;

#[derive(Debug)]
pub enum S3FSErrorKind {
    InvalidBucketName,
    InvalidObjectKey,
    BucketNotFound,
    BucketAlreadyExists,
    KeyNotFound,
    InvalidMeta,
    IncompleteWrite,
    InputOutput,
    ObjectTooLarge
}

impl std::fmt::Display for S3FSErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}
#[derive(Debug)]
pub struct S3FSError {
    pub kind: S3FSErrorKind,
    pub message: Option<String>
}

impl std::fmt::Display for S3FSError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut h = String::new();
        match &self.message {
            None => {},
            Some(c) => 
            {
                let msg = format!(" Message: {}", c);
                h.push_str(&msg);
            }
        };
        write!(f, "S3FSError: Kind: {}{}", &self.kind, &h)
    }
}

impl Error for S3FSError {
}

impl S3FSError {
    pub fn io(msg: Option<String>) -> S3FSError {
        S3FSError { kind: S3FSErrorKind::InputOutput, message: msg }
    }

    pub fn invalid_bucket_name (name: &str) -> S3FSError {
        S3FSError { kind: S3FSErrorKind::InvalidBucketName, message: Some(name.to_string()) }
    }

    pub fn bucket_not_found(name: &str) -> S3FSError {
        S3FSError {kind:S3FSErrorKind::BucketNotFound, message:Some(name.to_string())}
    }

    pub fn of_kind_and_message(kind:S3FSErrorKind, message:&str) -> S3FSError {
        S3FSError {kind:kind, message:Some(message.to_string())}
    }

    pub fn of_kind(kind:S3FSErrorKind) -> S3FSError {
        S3FSError {kind:kind, message: None}
    }
    pub fn boxed(self) -> Box<Self> {
        return Box::new(self);
    }
}
pub struct FS {
    base: PathBuf,
    seq:Arc<sequencing::Sequence>,
}

impl FS {
    pub fn new() -> FS {
        let result = FS { base: PathBuf::from("."), seq:Arc::new(Default::default()) };
        return result;
    }

    pub fn set_base(&mut self, new_base:&str) {
        self.base = PathBuf::from(new_base);
    }

    pub fn initialize(&self) {
        self.create_staging_directory();
    }

    pub fn create_staging_directory(&self) {
        let staging_dir = self.get_staging_dir();
        let _ = std::fs::create_dir_all(staging_dir);
    }
    pub fn make_bucket(&self,bucket:&str) -> Result<Bucket, Box<dyn Error>> {
        if !Bucket::valid_bucket(&bucket) {
            return Err(S3FSError::of_kind_and_message(
                S3FSErrorKind::InvalidBucketName, bucket).into());
        }
        if self.get_bucket(&bucket).is_some() {
            return Err(S3FSError::of_kind_and_message(
                S3FSErrorKind::BucketAlreadyExists, 
                bucket).into());
        }

        let mut base_path = PathBuf::from(&self.base);
        base_path.push(&bucket);
        let mb_result = std::fs::create_dir_all(base_path);
        if mb_result.is_err() {
            return Err(S3FSError::of_kind_and_message(S3FSErrorKind::InputOutput, 
                &format!("Failed to create directory {}: `{}`", 
                bucket,
                mb_result.err().unwrap())).into());
        }
        let result = self.get_bucket(bucket);
        if result.is_none() {
            return Err(S3FSError::of_kind_and_message(S3FSErrorKind::InputOutput, 
                "Bucket did not exist after creation").into());
        }

        return Ok(result.unwrap());

    }
    pub fn get_staging_dir(&self) -> String {
        let mut target = self.base.clone();
        target.push("_staging");
        return target.to_str().unwrap().to_string();
    }

    pub fn get_bucket(&self, name:&str) -> Option<Bucket>{
        if !Bucket::valid_bucket(name) {
            return None;
        }
        let mut path_buf = PathBuf::from(&self.base);
        path_buf.push(name);
        let meta = 
            std::fs::metadata(path_buf.clone());
        if meta.is_err() {
            return None;
        }
        let meta = meta.unwrap();
        if !meta.is_dir() {
            return None;
        }

        let result = Bucket::from(path_buf.to_str().unwrap(), 
            &self.get_staging_dir(), Arc::clone(&self.seq));

        if result.is_err() {
            return None;
        }

        return Some(result.unwrap());
    }

    pub fn get_all_buckets(&self) -> HashMap<String, Bucket> {
        let base_path: PathBuf = self.base.clone();
        let mut staging_dir = base_path.clone();
        staging_dir.push("_staging");
        let _ = std::fs::create_dir_all(&staging_dir);
        let mut result = HashMap::new();

        let read_result = std::fs::read_dir(self.base.clone());
        if read_result.is_err() {
            return result;
        }

        let dir_entries = read_result.unwrap();
        for next in dir_entries {
            if next.is_err() {
                continue;
            }

            let next = next.unwrap();
            if !next.path().is_dir() {
                continue;
            }
            let name = next.file_name();
            let name = name.to_str();
            if name.is_none() {
                continue;
            }

            let name = name.unwrap();
            if !Bucket::valid_bucket(name) {
                continue;
            }

            let staging_dir_local = staging_dir.clone();
            let bucket = Bucket::from(next.path().to_str().unwrap(), 
                staging_dir_local.to_str().unwrap(), 
                Arc::clone(&self.seq));
            if bucket.is_err() {
                continue;
            }
            let bucket = bucket.unwrap();

            result.insert(name.to_string(), bucket);
        }
        return result;
    }
}

#[derive(Debug, Clone)]
pub struct Bucket {
    base: PathBuf,
    base_path:String,
    staging: PathBuf,
    seq:Arc<Sequence>
}

#[derive(Deserialize,  Serialize, Debug)]
pub struct FileMeta {
    pub etag: String,
    pub size: usize,
}

impl Bucket {
    pub const META_SUFFIX:&str = "@@META@@";
    pub fn from(path: &str, staging_path:&str, seq:Arc<sequencing::Sequence>) -> Result<Bucket, Box<dyn Error>> {
        let pathb = PathBuf::from(path);
        let pathbx = pathb.canonicalize()?;
        let pathbxc = pathbx.clone();
        let mut path_base = pathbxc.as_path().to_str().unwrap().to_string();
        if !path_base.ends_with("/") {
            path_base.push_str("/");
        }
        Ok(Bucket {
            base: pathbx,
            base_path: path_base,
            staging: PathBuf::from(staging_path),
            seq
        })
    }

    fn get_sibling(&self, obj:S3Object) -> Vec<S3Object> {
        let parent = obj.target.parent();
        if parent.is_none() {
            return vec!();
        }
        let parent = parent.unwrap().to_path_buf();
        let mut parent_key = obj.key;
        if parent_key.ends_with("/") {
            parent_key = parent_key.trim_end_matches("/").into();
        }

        let last_slash = parent_key.rfind("/");
        match last_slash {
            Some(idx) => {
                let self_key_str = &parent_key[0..idx];
                parent_key = self_key_str.into();
            },
            None =>{
                parent_key = "".into();
            }
        }
        //self_key = self_key.trim_end_matches("").into();
        let parent_obj = S3Object {
            bucket:self,
            target: parent,
            kind: FileType::Directory,
            key: parent_key,
        };
        return self.get_children(parent_obj);
    }

    fn get_children<'a>(&'a self, parent:S3Object) -> Vec<S3Object<'a>>{
        let path = parent.target.clone();
        let parent_key = parent.key;
        let dir_iter = std::fs::read_dir(path.clone());
        if dir_iter.is_err() {
            return vec!();
        }
        let dir_iter = dir_iter.unwrap();
        let mut result = Vec::new();
        for next in dir_iter {
            if next.is_err() {
                continue;
            }
            let next = next.unwrap();
            let name = next.file_name();
            let name = name.to_str();
            if name.is_none() {
                // invalid name
                continue;
            }
            let name = name.unwrap();
            if name == "." || name == ".." || name.ends_with(Bucket::META_SUFFIX) {
                continue;
            }
            let meta = next.metadata();
            if meta.is_err() {
                continue;
            }
            let meta = meta.unwrap();

            if meta.is_dir() {
                let mut this_path = path.clone();
                this_path.push(name);
                let entry = S3Object {
                    bucket: self,
                    target: this_path,
                    kind: FileType::Directory,
                    key: format!("{parent_key}/{name}")
                };
                result.push(entry);
            } else if meta.is_file() {
                let mut this_path = path.clone();
                this_path.push(name);
                let entry = S3Object {
                    bucket: self,
                    target: this_path,
                    kind: FileType::File,
                    key: format!("{parent_key}/{name}")
                };
                result.push(entry);
            }

        }
        result.sort_by(|a, b| a.target.partial_cmp(&b.target).unwrap());
        return result;
    }

    fn collect_children<'a, 'b>(&'b self, obj:S3Object, result:&mut Vec<S3Object<'a>>)
        where 'b : 'a
    {
        let children = self.get_children(obj);
        for next_child in children {
            let child:S3Object = next_child.clone();
            result.push(child);
            if next_child.is_dir() {
                self.collect_children(next_child, result);
            }
        }
    }

    pub fn list_objects(&self, prefix:&str, after:&str, limit: usize) -> Vec<S3Object> {
        if !Bucket::valid_key(prefix) {
            return vec!();
        }
        let target = self.file_for_key(prefix);
        let file_name = target.get_short_name();
        let parent = target.target.parent();
        let mut result = Vec::new();

        if parent.is_none() {
            return vec!();
        }

        if target.is_dir() {
            result.push(target.clone());
            self.collect_children(target, &mut result);
        } else {
            let siblings = self.get_sibling(target);
            for next in siblings {
                let next_file_name = next.get_short_name();
                if next_file_name.starts_with(&file_name) {
                    if next.is_dir() {
                        self.collect_children(next, &mut result);
                    } else {
                        result.push(next.clone());
                    }
                }
            }
         }
         return result.into_iter()
            .filter(|x| x.kind == FileType::File)
            .filter(|x| x.has_meta())
            .filter(|x| x.object_key() > after)
            .take(limit)
            .collect();
    }

    pub fn list_objects_short(&self, key:&str, after:&str, limit:usize) -> Vec<S3Object> {
        if !Bucket::valid_key(key) {
            return vec!();
        }
        let mut path = self.base.clone();
        path.push(key);
        let target = self.file_for_key(key);
        let file_name = target.get_short_name();
        let parent = target.target.parent();
        let mut result = Vec::new();
        if parent.is_none() {
            return vec!();
        }
        if target.is_dir() {
            result.push(target.clone());
            let children = self.get_children(target);
            for next in children {
                result.push(next);
            }
        } else {
            let siblings = self.get_sibling(target);
            for next in siblings {
                let next_file_name = next.get_short_name();
                if next_file_name.starts_with(&file_name) {
                    result.push(next);
                }
            }
         }
        return result.into_iter()
            .filter(|x| x.kind == FileType::Directory || (x.kind == FileType::File && x.has_meta()))
            .filter(|x| x.object_key() > after)
            .take(limit)
            .collect();

    }

    pub fn gen_upload_id(&self) -> String {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        return format!("upload_{s}");
    }


    pub fn get_creation_time(&self) -> Result<String, Box<dyn Error>> {
        let meta = std::fs::metadata(&self.base)?;
        let created = meta.created()?;
        let datetime: DateTime<Utc> = created.into();
        // 2009-10-12T17:50:30.000Z
        return Ok(format!("{}", datetime.format("%Y-%m-%dT%H:%M:%S%.3f%:z")));
    }
    pub fn cleanup_upload_id(&self, id:&str) {
        let ids = id.to_string();
        //target.push(id.to_string());
        let mut counter = 0;
        loop {
            counter +=1;
            let mut file_target = self.staging.clone();
            file_target.push(format!("{id}_{counter}"));
            let mut meta_target = self.staging.clone();

            meta_target.push(format!("{}_{}{}", ids, counter, Self::META_SUFFIX));
            if meta_target.exists() {
                let _ = std::fs::remove_file(meta_target);
            }
            if file_target.exists() {
                let _ = std::fs::remove_file(file_target);
            } else {
                if counter > 10 {
                    break;
                }
            }
            if counter > 1000 {
                break;
            }
        }
    }

    pub fn valid_bucket(bucket:&str) -> bool {
        if bucket == "_staging" {
            return false;
        }
        let reg = "^[a-zA-Z0-9.\\-_]{1,255}$";
        let pattern = Regex::new(reg).unwrap();
        if pattern.is_match(bucket) {
            return true;
        }
        return false;
    }
    fn valid_key(key:&str) -> bool {
        let reg = ".*[\\\\><|:&\\$].*";
        let pattern = Regex::new(reg).unwrap();
        if pattern.is_match(key) {
            return false;
        }

        if key.contains("/./") || key.contains("/../") || key.contains("//") {
            return false;
        }

        if key.starts_with("/") {
            return false;
        }
        if key.ends_with(Self::META_SUFFIX) {
            return false;
        }

        return true;

    }
    pub fn list_all_objects(&self) -> Vec<S3Object> {
        let mut files = Vec::new();
        Self::scan(&self.base, &mut files);
        let mut result = Vec::new();
        let base = &self.base_path;
        for item in files {
            let full_path = String::from(item.to_str().unwrap());
            let key = full_path.strip_prefix(base).unwrap();
            if !full_path.ends_with(Self::META_SUFFIX) {
                result.push(
                    S3Object {
                        bucket:&self,
                        target: item,
                        kind: FileType::File,
                        key:String::from(key)
                    }
                );
            }
        }
        return result;
    }

    fn scan(directory:&PathBuf, result: &mut Vec<PathBuf>) {
        let paths = fs::read_dir(directory.as_path());
        if let Err(_) = paths {
            return;
        }
        let paths = paths.unwrap();
        for next in paths {
            match next {
                Ok(dir) => {
                    let next_path = dir.path();
                    let meta = fs::metadata(next_path.as_path());
                    match meta {
                        Err(_) => continue,
                        Ok(metadata) => {
                            if metadata.is_dir() {
                                // directory
                                Self::scan(&next_path, result);
                            } else if metadata.is_file(){
                                result.push(next_path);
                            }
                        }
                    }
                },
                Err(_) => {
                    continue;
                }
            }
        }
    }

    pub fn delete_object(&self, key:&str) -> bool {
        if !Self::valid_key(key) {
            return false;
        }
        let full_path = format!("{}{}", &self.base_path, &key);
        let full_meta_path = format!("{}{}{}", &self.base_path, &key, Self::META_SUFFIX);
        let pb = PathBuf::from(full_path);
        let pbc = pb.clone();
        let _ = std::fs::remove_file(&pb);
        let _ = std::fs::remove_file(&full_meta_path);
        let _ = std::fs::remove_dir(&pb);
        if key.contains("/") {
            let parent = pbc.parent().unwrap();
            let _ = std::fs::remove_dir(parent);
        }
        return true;
    }

    pub fn read_object(&self, key:&str) -> Result<std::fs::File, Box<dyn Error>> {
        if !Self::valid_key(key) {
            return Err(S3FSError::of_kind_and_message(
                S3FSErrorKind::InvalidObjectKey, key).into());
        }
        let file = self.file_for_key(key);
        let path = file.target;
        return Ok(std::fs::File::open(path.as_path())?);
    }

    pub fn get_object_meta(&self, key:&str) -> Result<FileMeta, Box<dyn Error>> {
        if !Self::valid_key(key) {
            return Err(S3FSError::of_kind_and_message(S3FSErrorKind::InvalidObjectKey, key).into());
        }
        let meta_file = self.meta_file_for_key(key);
        let path = meta_file.target;
        let mut input_file = std::fs::File::open(path.as_path())?;
        let mut buffer = [0u8; 4096];
        let mut nread:usize = 0;
        loop {
            let rr = input_file.read(&mut buffer[nread..])?;
            if rr == 0 {
                break;
            }
            nread += rr;
        }
        let string = std::str::from_utf8(&buffer[0..nread]).unwrap();
        let result:FileMeta = serde_json::from_str(string)?;
        return Ok(result);
    }

    pub async fn merge_part(&self, key:&str, upload_id:&str) -> Result<(usize, String), Box<dyn Error>> {
        let staging_path = self.staging.clone();
        let mut counter = 0;
        let dest = self.file_for_key(key);
        dest.ensure_parent();
        let mut dest_file = tokio::fs::File::create(dest.target).await?;
        let mut total:usize = 0;
        let mut hasher = Md5::new();
        loop {
            counter = counter + 1;
            let mut next_file_p = staging_path.clone();
            next_file_p.push(format!("{}_{}", upload_id, counter));
            let mut next_meta_file_p = staging_path.clone();
            next_meta_file_p.push(format!("{}_{}{}", upload_id, counter, Self::META_SUFFIX));
            if next_file_p.exists() {
                let mut next_f = tokio::fs::File::open(next_file_p.as_path()).await?;
                let copied = Self::merge_into_with_hash(&mut dest_file, &mut next_f, &mut hasher).await?;
                total += copied;
                let _ = std::fs::remove_file(next_file_p);
                let _ = std::fs::remove_file(next_meta_file_p);
            } else {
                break;
            }
        }
        let hash = format!("{:x}", hasher.finalize());

        let meta_dest_f = self.meta_file_for_key(key);
        let mut meta_dest = std::fs::File::create(meta_dest_f.path())?;
        Self::save_meta(&mut meta_dest, FileMeta {
            etag: hash.clone(),
            size: total
        })?;
        return Ok((total, hash));
    }

    async fn merge_into_with_hash(dst:&mut tokio::fs::File, src:&mut tokio::fs::File, hasher:&mut Md5) ->Result<usize, Box<dyn Error>> {
        let mut buf = [0u8; 4096];
        let mut copied = 0;
        loop {
            let rr = src.read(&mut buf).await?;
            if rr == 0 {
                break;
            }
            hasher.update(&buf[..rr]);
            let wr = dst.write(&mut buf[..rr]).await?;
            if wr != rr {
                return Err(Box::new(S3Error::io_error()))
            }
            copied += wr;
        }
        return Ok(copied);
    }

    pub async fn save_object_part<D>(&self, _key:&str, upload_id:&str, part_number:u32,reader: &mut D, chunked:bool) -> Result<(usize,String), Box<dyn Error>> 
        where D:tokio::io::AsyncRead + Unpin
    {
        let mut path = self.staging.clone();
        path.push(format!("{upload_id}_{part_number}"));
        let object_path = path;
        let mut path_after = self.staging.clone();
        path_after.push(format!("{upload_id}_{part_number}"));

        let mut meta_path = self.staging.clone();
        meta_path.push(format!("{}_{}{}", upload_id, part_number, Self::META_SUFFIX));
        let object_meta_path = meta_path;
        let mut tmp_file = tokiofs::File::create(object_path.clone()).await.unwrap();
        let mut tmp_meta = std::fs::File::create(object_meta_path.clone()).unwrap();

        let (copied, md5) = copy_chunk_to_raw(reader, &mut tmp_file, chunked).await?;
        
        Self::save_meta(&mut tmp_meta, FileMeta{etag:md5.clone(), size:copied})?;
        return Ok((copied, md5));
    }


    pub async fn save_object<D>(&self, key:&str, reader: &mut D, chunked:bool) -> Result<(usize, String), Box<dyn Error>> 
        where D:tokio::io::AsyncRead + Unpin
    {
        if !Self::valid_key(key) {
            return Err(S3FSError::of_kind_and_message(
                S3FSErrorKind::InvalidObjectKey, key).into())
        }
        let file = self.file_for_key(key);
        let meta = self.meta_file_for_key(key);

        let seq = self.seq.next();
        let staging = self.staging.clone();
        let staging = staging.as_path().to_str().unwrap();
        let object_tmp_name = format!("{staging}/{seq}");
        let object_tmp_meta_name = format!("{}/{}{}", staging, seq, Self::META_SUFFIX);
        let mut tmp_file = tokiofs::File::create(object_tmp_name.clone()).await.unwrap();
        let mut tmp_meta = std::fs::File::create(object_tmp_meta_name.clone()).unwrap();
        file.ensure_parent();

        let (copied_size, md5) = copy_chunk_to_raw(
            reader, &mut tmp_file, chunked).await?;

        Self::save_meta(&mut tmp_meta, FileMeta{etag:md5.clone(), size:copied_size})?;

        let _ = std::fs::rename(object_tmp_name, file.target);

        let _ = std::fs::rename(object_tmp_meta_name, meta.target);
        return Ok((copied_size, md5));
    }

    fn save_meta(out:&mut std::fs::File, meta:FileMeta) -> Result<usize, Box<dyn Error>> 
    {
        let to_write = serde_json::to_string(&meta)?;
        let written = out.write(to_write.as_bytes())?;
        return Ok(written);
    }
   

    fn try_canicalize(input:PathBuf) ->PathBuf {
        let can = input.clone().canonicalize();
        if can.is_err() {
            return input;
        }
        return can.unwrap();
    }
    fn file_for_key(&self, key:&str)->S3Object {
        let full_path = format!("{}{}", self.base_path, key);
        let path_buf = Self::try_canicalize(PathBuf::from(full_path));
        let actual_key = path_buf.to_str().unwrap().to_string();
        let base_string = self.base_path.clone();
        let mut key_short:String = "".into();
        if actual_key.len() > base_string.len() {
            key_short = (&actual_key[base_string.len()..]).to_string();
        }
        let meta = std::fs::metadata(&path_buf);
        let mut obj_type = FileType::Uninitialized;
        match meta {
            Err(_) => {

            },
            Ok(some_meta) => {
                if some_meta.is_dir() {
                    obj_type = FileType::Directory;
                } else if some_meta.is_file() {
                    obj_type = FileType::File;
                }
            }
        }
        return S3Object {
            bucket: &self,
            target:path_buf,
            kind: obj_type,
            key: key_short
        }
    }

    pub fn get_object_by_key(&self, key:&str) -> Option<S3Object> {
        let file = self.file_for_key(key);
        if !file.exists() {
            return None;
        }

        if !file.meta().unwrap().is_file()  {
            return None;
        }
        return Some(file);
    }

    fn meta_file_for_key(&self, key:&str) -> S3Object {
        let full_path = format!("{}{}{}", self.base_path, key, Self::META_SUFFIX);
        let path_buf = Self::try_canicalize(PathBuf::from(full_path));
        let meta = std::fs::metadata(&path_buf);
        let mut obj_type = FileType::Uninitialized;
        match meta {
            Err(_) => {

            },
            Ok(some_meta) => {
                if some_meta.is_dir() {
                    obj_type = FileType::Directory;
                } else if some_meta.is_file() {
                    obj_type = FileType::File;
                }
            }
        }
        return S3Object {
            bucket: &self,
            target: path_buf,
            kind: obj_type,
            key: String::from(key)
        }
    }
    pub fn list_objects_old(&self, prefix:&str) -> Vec<S3Object> {
        let mut result = Vec::new();
        let all = self.list_all_objects();
        for next in all {
            if next.key.starts_with(prefix) {
                result.push(next)
            }
        }
        return result;
    }

    pub fn list_objects_short_old(&self, prefix:&str) -> Vec<S3Object> {
        return vec!();
    }

}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FileType {
    Uninitialized,
    File,
    Directory
}
#[derive(Debug, Clone)]
pub struct S3Object<'a> {
    bucket: &'a Bucket,
    target: PathBuf,
    kind: FileType,
    key: String
}


impl<'a> S3Object<'a> {
    pub async fn open_for_read(&self) -> Result<tokiofs::File, Box<dyn Error>> {
        let meta = self.meta()?;
        if meta.is_dir() {
            return Err(S3FSError::of_kind_and_message(S3FSErrorKind::InvalidObjectKey, 
                "read target is folder").into());
        }
        let open_result = tokiofs::File::open(self.target.clone()).await?;
        return Ok(open_result);
    }

    pub fn kind(&self) -> &FileType {
        &self.kind
    }
    
    pub fn read_as_string(&self) -> Result<String, Box<dyn Error>>{
        let bytes = self.read_as_bytes()?;
        return Ok(String::from_utf8(bytes)?);
    }

    pub fn read_as_bytes(&self) ->Result<Vec<u8>, Box<dyn Error>> {
        let mut file = std::fs::File::open(self.target.clone())?;
        let meta = file.metadata()?;
        if meta.len() > 10 * 1024 * 1024 {
            return Err(Box::new(S3FSError::of_kind_and_message(S3FSErrorKind::ObjectTooLarge, 
                "Object larger than 10MiB can't be read as a whole!")));
        }
        let mut buf = Vec::new();
        let _ = file.read_to_end(&mut buf)?;
        return Ok(buf);
    }

    pub fn is_dir(&self) -> bool {
        let meta = self.meta();
        if meta.is_err() {
            return false;
        }
        return meta.unwrap().is_dir();
    }
    pub fn is_file(&self) -> bool {
        let meta = self.meta();
        if meta.is_err() {
            return false;
        }
        return meta.unwrap().is_file();
    }

    pub fn exists(&self) -> bool {
        let meta = self.meta();
        if meta.is_err() {
            return false;
        }
        return true;
    }

    pub fn len(&self) -> Result<u64, Box<dyn Error>> {
        let meta = self.meta()?;
        Ok(meta.len())
    }

    pub fn bucket(&self) -> &Bucket {
        return self.bucket;
    }

    pub fn metafile(&self) -> S3Object {
        return self.bucket.meta_file_for_key(&self.key);
    }


    pub fn checksum(&self) -> Result<String, Box<dyn Error>> {
        let meta = self.bucket.get_object_meta(&self.key)?;
        return Ok(meta.etag);
    }

    pub fn last_modified_formatted(&self) -> Result<String, Box<dyn Error>> {
        let modified = self.meta()?.modified()?;
        let datetime: DateTime<Utc> = modified.into();
        // 2009-10-12T17:50:30.000Z
        return Ok(format!("{}", datetime.format("%Y-%m-%dT%H:%M:%S%.3f%:z")));
    }

    pub fn object_key(&self) -> &str {
        return &self.key;
    }

    pub fn get_short_name(&self) -> String {
        return self.target.file_name().unwrap().to_str().unwrap().to_string();
    }

    pub fn object_key_encoded(&self) -> String {
        let to_encode = &self.key;
        return urlencoding::encode(to_encode).to_string();
    }

    pub fn ensure_parent(&self) -> bool {
        let parent = self.target.parent().unwrap();
        let created = std::fs::create_dir_all(parent);
        if created.is_err() {
            return false;
        }
        return true;
    }

    fn type_for(path: &PathBuf) -> Option<FileType> {
        let meta = std::fs::metadata(path);
        if meta.is_err() {
            return None;
        }

        let meta = meta.unwrap();
        if meta.is_dir() {
            return Some(FileType::Directory);
        }

        if meta.is_file() {
            return Some(FileType::File);
        }
        return None;
    }

    pub fn meta(&self) -> Result<Metadata, Box<dyn Error>> {
        Ok(metadata(self.target.as_path())?)
    }

    pub fn has_meta(&self) -> bool {
        let meta_file = self.metafile();
        return meta_file.is_file();
    }

    pub fn full_path(&self) -> Option<String> {
        let target_str = self.target.to_str()?;
        return Some(target_str.to_string());
    }

    pub fn path(&self) -> &PathBuf {
        &self.target
    }

    pub async fn format(&self) -> String {
        let object_key = self.object_key();
        if self.kind == FileType::Directory {
            let mut object_key = object_key.clone().to_string();
            if !object_key.ends_with("/") {
                object_key.push_str("/");
            }
            return format!(r#"<CommonPrefixes><Prefix>{object_key}</Prefix></CommonPrefixes>"#);
        }
        let object_last_modified = self.last_modified_formatted().unwrap();
        let mut object_etag = self.checksum();
        if object_etag.is_err() {
            object_etag = Ok("".into());
        }
        let object_etag = object_etag.unwrap();
        let object_size = self.len().unwrap();
        let entry_xml = format!(r#"<Contents>
        <Key>{object_key}</Key>
        <LastModified>{object_last_modified}</LastModified>
        <ETag>"{object_etag}"</ETag>
        <Size>{object_size}</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>"#);
      return entry_xml;
    }
}
