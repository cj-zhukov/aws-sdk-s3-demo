use std::sync::Arc;

use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    config::Builder,
    operation::get_object::{GetObjectError, GetObjectOutput},
    Client,
};
use bytes::Bytes;
use tokio::{io::AsyncReadExt, sync::Semaphore};

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

/// Get file from AWS S3
pub async fn read_file(
    client: &Client, 
    bucket: &str, 
    key: &str,
    chunk_suze: Option<u64>,
) -> Result<Vec<u8>, UtilsError> {
    let object = get_aws_object(client, bucket, key).await?;
    let length = object.content_length().unwrap_or(0) as u64;
    let mut body = object.body;
    if length <= chunk_suze.unwrap_or(CHUNK_SIZE) {
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

/// Get file from AWS S3 by reading chunks in parallel
pub async fn read_file_big(
    client: &Client, 
    bucket: &str, 
    key: &str,
    chunk_size: Option<u64>,
    chunks_workers: Option<usize>,
) -> Result<Vec<u8>, UtilsError> {
    let object = get_aws_object(client, bucket, key).await?;
    let size = object.content_length().unwrap_or(0) as u64;
    let mut ranges = vec![];
    for start in (0..size).step_by(chunk_size.unwrap_or(CHUNK_SIZE) as usize) {
        let end = (start + chunk_size.unwrap_or(CHUNK_SIZE) - 1).min(size - 1);
        ranges.push((start, end));
    }

    let semaphore = Arc::new(Semaphore::new(chunks_workers.unwrap_or(CHUNKS_WORKERS))); 
    let mut tasks = vec![];
    let ranges_len = ranges.clone().len();
    for (i, (start, end)) in ranges.into_iter().enumerate() {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let permit = semaphore.clone().acquire_owned().await
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?;

        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let range = format!("bytes={}-{}", start, end);
            let out = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .range(range)
                .send()
                .await?;
            let bytes = out.body.collect().await?.into_bytes();
            Ok::<(usize, Bytes), UtilsError>((i, bytes))
        }));
    }

    let mut results = vec![Bytes::new(); ranges_len];
    for task in tasks {
        let (i, chunk) = task.await
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?;
        results[i] = chunk;
    }

    let total_size: usize = results.iter().map(|b| b.len()).sum();
    let mut buf = Vec::with_capacity(total_size);
    for chunk in results {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}
