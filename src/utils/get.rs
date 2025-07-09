use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    config::Builder,
    operation::get_object::{GetObjectError, GetObjectOutput},
    Client,
};
use tokio::io::AsyncReadExt;

use crate::error::UtilsError;
use crate::utils::constants::*;

/// Get AWS Client
pub async fn get_aws_client(region: String) -> Client {
    let region = Region::new(region);
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let config_builder = Builder::from(&sdk_config)
        .retry_config(RetryConfig::standard().with_max_attempts(AWS_MAX_RETRIES));
    let config = config_builder.build();
    Client::from_conf(config)
}

/// Get AWS GetObjectOutput
pub async fn get_aws_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<GetObjectOutput, UtilsError> {
    Ok(client.get_object().bucket(bucket).key(key).send().await?)
}

/// Get None if key doesn't exist in AWS S3
pub async fn try_get_file(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<GetObjectOutput>, UtilsError> {
    let resp = client.get_object().bucket(bucket).key(key).send().await;
    match resp {
        Ok(res) => Ok(Some(res)),
        Err(sdk_err) => match sdk_err.into_service_error() {
            GetObjectError::NoSuchKey(_) => Ok(None),
            err => Err(UtilsError::UnexpectedError(err.into())),
        },
    }
}

/// Read file from AWS S3
pub async fn read_file(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>, UtilsError> {
    let object = get_aws_object(client, bucket, key).await?;
    let length = object.content_length().unwrap_or(0) as u64;
    let mut body = object.body;
    if length <= CHUNK_SIZE {
        let mut buf = Vec::with_capacity(length as usize);
        let mut reader = body.into_async_read();
        reader.read_to_end(&mut buf).await?;
        Ok(buf)
    } else {
        let mut buf = Vec::with_capacity(length as usize);
        while let Some(chunk) = body.try_next().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}
