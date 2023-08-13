use rocket::{http::Status, response::Responder};
use std::{io::Cursor, convert::Infallible};
use rocket::http::Header;
use rocket::response::Debug;
pub struct S3Response<'r> {
    body: String,  
    headers: Vec<Header<'r>>, 
    status: Status,
}

impl<'r,'o> Responder<'r, 'r> for S3Response<'r>
    where 'o:'r
{
    fn respond_to(self, _req: &'r rocket::Request) -> Result<rocket::Response<'r>, Status> {
        let size = self.body.len();
        use rocket::response::Builder;
        let mut resp:Builder<'r> = rocket::Response::build();
        resp.status(self.status);
        resp.sized_body(size, Cursor::new(self.body));
        for next in self.headers {
            resp.header(next);
        }

        return Ok(resp.finalize());
    }
}

impl<'r> Into<Result<S3Response<'r>, Debug<Infallible>>> for S3Response<'r> {
    fn into(self) -> Result<S3Response<'r>, Debug<Infallible>> {
        return Ok(self);
    }
}
impl <'r> S3Response<'r> {
    pub fn ok() -> Self {
        S3Response { body: "".to_string(), headers: vec![], status: Status::Ok }
    }

    pub fn not_found() -> Self {
        S3Response {
            body: "".to_string(),
            headers: vec!(),
            status: Status::NotFound
        }
    }

    pub fn invalid_request() -> Self {
        S3Response { 
            body: "".to_string(),
            headers: vec![], 
            status: Status::BadRequest 
        }
    }

    pub fn server_error() -> Self {
        S3Response {
            body: "".to_string(),
            headers: vec![],
            status: Status::InternalServerError
        }
    }

    pub fn add_header(&mut self, key:&str, value:&str) -> &mut Self {
        let new_header = Header::new(key.to_string(), value.to_string());
        self.headers.push(new_header);
        return self;
    }

    pub fn status_code(&mut self, code: u16) -> &mut Self {
        self.status = Status::new(code);
        return self;
    }

    pub fn body(&mut self,  text:&str) -> &mut Self {
        self.body = String::from(text);
        return self;
    }
}