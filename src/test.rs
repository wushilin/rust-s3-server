pub mod s3error;
pub mod chunk_to_raw;
pub mod sequencing;
pub mod fsapi;
use fsapi::{Bucket, FS};
use tokio::fs::File;
use std::path::PathBuf;
#[tokio::main]
async fn main() {
    let mut fs = FS::new();
    fs.set_base("rusts3-data-debug");
    fs.initialize();
    let b = fs.get_bucket("abcde").unwrap();
    let result = b.list_objects("te", "", 10);
    for next in result {
        println!("{} -> {:?}", next.object_key(), next.kind());
    }

    let mut v  = Vec::<i32>::new();
    test(&mut v);
}

fn test(v:&mut Vec<i32>) {
    v.push(1);
    if v.len() < 5 {
        test(v);
    }
}

