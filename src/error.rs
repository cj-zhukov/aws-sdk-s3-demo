use std::io::Error as IoError;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_smithy_types::byte_stream::error::Error as AWSSmithyError;
use color_eyre::eyre::Report;
use thiserror::Error;
use url::ParseError;

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("IO error")]
    IoError(#[from] IoError),

    #[error("AWS GetObjectError error")]
    GetObjectError(#[from] SdkError<GetObjectError>),

    #[error("AWS ListObjectsV2Error error")]
    ListObjectsV2Error(#[from] SdkError<ListObjectsV2Error>),

    #[error("AWS CreateMultipartUploadError error")]
    CreateMultipartUploadError(#[from] SdkError<CreateMultipartUploadError>),

    #[error("AWS CompleteMultipartUploadError error")]
    CompleteMultipartUploadError(#[from] SdkError<CompleteMultipartUploadError>),

    #[error("AWS PutObjectError error")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("AWS UploadPartError error")]
    UploadPartError(#[from] SdkError<UploadPartError>),

    #[error("AWSSmithy error")]
    AWSSmithyError(#[from] AWSSmithyError),

    #[error("UrlParseError error")]
    UrlParseError(#[from] ParseError),

    #[error("InvalidS3Uri error")]
    InvalidS3Uri,

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}
