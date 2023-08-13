use rocket::http::Status;
use std::time::SystemTime;
use asyncio_utils::LimitSeekerReader;
use tokio::fs::File;
//limit_reader::LimitedReader;

pub struct CachedFile {
    pub reader:LimitSeekerReader<File>,
    pub file_name:String, 
    pub size:usize, 
    pub modified_time:SystemTime,
    pub etag:String,
    pub partial: bool,
}

#[rocket::async_trait]
impl<'r,'o> rocket::response::Responder<'r,'r> for CachedFile
{
    
    fn respond_to(self, _req: &'r rocket::Request) -> rocket::response::Result<'r> {
        //let etag = self.1.sha256sum().unwrap();
        //let last_modified = self.1.meta().unwrap().modified().unwrap();
        let etag = self.etag;
        let htd = httpdate::fmt_http_date(self.modified_time);

        let response = rocket::response::Response::build().sized_body(
            self.size, self.reader).finalize();

        let mut actual_builder = &mut rocket::response::Response::build_from(response);
        actual_builder = actual_builder.raw_header("Cache-control", "max-age=86400")
                    .raw_header("Last-Modified", htd) //  24h (24*60*60)
                    .raw_header("ETag", etag)
                    .raw_header("Content-Type", "application-octetstream")
                    .raw_header("Content-Disposition", format!("attachment; filename=\"{}\"", self.file_name));
        if self.partial {
            actual_builder = actual_builder.status(Status::PartialContent);
        }
        
        actual_builder.raw_header("content-length", format!("{}", self.size));

        return Ok(actual_builder.finalize());
                    
    }
} 
