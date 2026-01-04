use std::io::Error as IoError;

use aws_sdk_s3::error::SdkError;
use aws_smithy_types::byte_stream::error::Error as AWSSmithyError;
use color_eyre::eyre::Report;
use thiserror::Error;
use url::ParseError;

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("AWS S3 error")]
    AwsS3(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("IO error")]
    IoError(#[from] IoError),

    #[error("AWSSmithy error")]
    AWSSmithyError(#[from] AWSSmithyError),

    #[error("UrlParseError error")]
    UrlParseError(#[from] ParseError),

    #[error("InvalidS3Uri error")]
    InvalidS3Uri,

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}

impl<E> From<SdkError<E>> for UtilsError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: SdkError<E>) -> Self {
        UtilsError::AwsS3(Box::new(err))
    }
}
