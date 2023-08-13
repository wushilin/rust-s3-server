use std::fmt;
#[derive(Debug)]
pub struct S3Error {
    pub kind:ErrorKind,
    pub message:String,
}

#[derive(Debug)]
pub enum ErrorKind {
    InvalidKey,
    NotFound,
    InputOutput
}

impl S3Error {
    pub fn invalid_key() -> Self {
        S3Error {
            kind: ErrorKind::InvalidKey,
            message: String::from("")
        }
    }

    pub fn not_found() -> Self {
        S3Error {
            kind: ErrorKind::NotFound,
            message: String::from("")
        }
    }

    pub fn io_error() -> Self {
        S3Error {
            kind: ErrorKind::InputOutput,
            message: String::from("")
        }
    }
}
impl fmt::Display for ErrorKind {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> { 
        return write!(w, "{:?}", self);
    }
}
impl fmt::Display for S3Error {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> { 
        return write!(w, "{:?}", self);
    }
}

impl std::error::Error for S3Error {

}