use std::{collections::HashMap, path::Path};

use async_stream::stream;
use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    config::Builder, error::SdkError, operation::{get_object::{GetObjectError, GetObjectOutput}, list_objects_v2::ListObjectsV2Error}, primitives::ByteStream, types::{CompletedMultipartUpload, CompletedPart}, Client
};
use aws_smithy_types::byte_stream::Length;
use color_eyre::eyre::eyre;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};
use tokio_stream::Stream;

use crate::error::UtilsError;
use crate::utils::{AWS_MAX_RETRIES, CHUNK_SIZE, MAX_CHUNKS};

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
    client: Client,
    bucket: &str,
    key: &str,
) -> Result<GetObjectOutput, UtilsError> {
    Ok(client.get_object().bucket(bucket).key(key).send().await?)
}

/// Get None if key doesn't exist in AWS S3
pub async fn try_get_file(
    client: Client,
    bucket: &str,
    key: &str,
) -> Result<Option<GetObjectOutput>, UtilsError> {
    let resp = client.get_object().bucket(bucket).key(key).send().await;

    match resp {
        Ok(res) => Ok(Some(res)),
        Err(sdk_err) => match sdk_err.into_service_error() {
            GetObjectError::NoSuchKey(_) => Ok(None),
            err @ _ => Err(UtilsError::UnexpectedError(err.into())),
        },
    }
}

/// Read file from AWS S3
pub async fn read_file(client: Client, bucket: &str, key: &str) -> Result<Vec<u8>, UtilsError> {
    let mut buf = Vec::new();
    let mut object = get_aws_object(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await? {
        buf.extend(bytes.to_vec());
    }
    Ok(buf)
}

pub async fn download_file(
    client: Client,
    bucket: &str,
    key: &str,
    file_path: &str,
) -> Result<(), UtilsError> {
    let res = get_aws_object(client.clone(), bucket, key).await?;
    let mut data = res.body;
    let file = File::create(&file_path).await?;
    let mut buf_writer = BufWriter::new(file);
    while let Some(bytes) = data.try_next().await? {
        let _n = buf_writer.write(&bytes).await?;
    }
    buf_writer.flush().await?;
    Ok(())
}

pub async fn upload_file(
    client: Client,
    bucket: &str,
    file_path: &str,
    key: &str,
) -> Result<(), UtilsError> {
    let body = ByteStream::from_path(file_path).await?;
    let _resp = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(())
}

/// Get files names
pub async fn list_keys(
    client: Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, UtilsError> {
    let mut stream = client
        .list_objects_v2()
        .prefix(prefix)
        .bucket(bucket)
        .into_paginator()
        .send();

    let mut files = Vec::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for obj in objects.contents() {
            if let Some(key) = obj.key() {
                if !key.ends_with('/') {
                    files.push(key.to_string());
                }
            }
        }
    }
    Ok(files)
}

/// Get files names and size
pub async fn list_keys_to_map(
    client: Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, i64>, UtilsError> {
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut files: HashMap<String, i64> = HashMap::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for obj in objects.contents() {
            if let Some(key) = obj.key() {
                if !key.ends_with('/') {
                    let file_name = key.to_string();
                    let file_size = obj.size().unwrap_or(0);
                    files.insert(file_name, file_size);
                }
            }
        }
    }
    Ok(files)
}

/// List keys using stream
/// let mut stream = Box::pin(list_keys_stream(client, "bucket", "prefix/").await.take(10));
pub async fn list_keys_stream<'a>(
    client: Client,
    bucket: &'a str,
    prefix: &'a str,
) -> impl Stream<Item = Result<String, SdkError<ListObjectsV2Error>>> + use<'a> {
    stream! {
        let mut continuation_token = None;

        loop {
            let mut request = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let result = request.send().await;

            match result {
                Ok(response) => {
                    if let Some(contents) = response.contents {
                        for object in contents {
                            if let Some(key) = object.key {
                                yield Ok(key);
                            }
                        }
                    }

                    if response.next_continuation_token.is_none() {
                        break;
                    } else {
                        continuation_token = response.next_continuation_token;
                    }
                }
                Err(e) => {
                    yield Err(e);
                    break;
                }
            }
        }
    }
}

/// Upload file by chunks with checking size
pub async fn upload_object_multipart(
    client: Client,
    bucket: &str,
    file_name: &str,
    key: &str,
    file_size: Option<u64>,
    chunk_size: Option<u64>,
    max_chunks: Option<u64>,
) -> Result<(), UtilsError> {
    println!("Uploading file: {}", file_name);

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().unwrap_or_default();
    let path = Path::new(&file_name);
    let file_size = match file_size {
        Some(val) => val,
        None => File::open(file_name).await?.metadata().await?.len(),
    };
    let chunk_size = chunk_size.unwrap_or(CHUNK_SIZE);
    let max_chunks = max_chunks.unwrap_or(MAX_CHUNKS);
    let mut chunk_count = (file_size / chunk_size) + 1;
    let mut size_of_last_chunk = file_size % chunk_size;

    if size_of_last_chunk == 0 {
        size_of_last_chunk = chunk_size;
        chunk_count -= 1;
    }
    if file_size == 0 {
        let err = eyre!(format!("Bad file size for: {}", file_name));
        return Err(UtilsError::UnexpectedError(err.into()));
    }
    if chunk_count > max_chunks {
        let err = eyre!(format!(
            "Too many chunks file: {}. Try increasing your chunk size",
            file_name
        ));
        return Err(UtilsError::UnexpectedError(err.into()));
    }

    let mut upload_parts = Vec::new();
    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            chunk_size
        };
        let stream = ByteStream::read_from()
            .path(path)
            .offset(chunk_index * chunk_size)
            .length(Length::Exact(this_chunk))
            .build()
            .await?;

        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await?;

        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;

    println!("Uploaded file: {}", file_name);

    let data: GetObjectOutput = get_aws_object(client, bucket, key).await?;
    let data_length = data.content_length().unwrap_or(0) as u64;
    if file_size != data_length {
        let err = eyre!("Failed checking data size after upload");
        return Err(UtilsError::UnexpectedError(err.into()));
    }
    Ok(())
}
