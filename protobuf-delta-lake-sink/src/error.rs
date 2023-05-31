use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("aws error")]
    Aws(#[from] aws_sdk_s3::Error),
    #[error("io error")]
    IoError(#[from] std::io::Error),
}

impl Error {
    pub fn s3_error<T>(err: T) -> Self
    where
        T: Into<aws_sdk_s3::Error>,
    {
        Self::from(err.into())
    }
}
