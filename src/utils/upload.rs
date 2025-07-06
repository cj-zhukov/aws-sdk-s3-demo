use std::path::Path;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::Length;
use color_eyre::eyre::eyre;
use tokio::fs::File;

use crate::error::UtilsError;
use crate::utils::constants::*;

/// Upload file
pub async fn upload_file(
    client: &Client,
    bucket: &str,
    file_path: &str,
    key: &str,
) -> Result<(), UtilsError> {
    let body = ByteStream::from_path(file_path).await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(())
}

/// Upload file by chunks with checking size
pub async fn upload_object_multipart(
    client: &Client,
    bucket: &str,
    file_path: &str,
    key: &str,
    file_size: Option<u64>,
    chunk_size: Option<u64>,
    max_chunks: Option<u64>,
) -> Result<(), UtilsError> {
    println!("Uploading file: {}", file_path);

    let path = Path::new(file_path);
    let file_len = match file_size {
        Some(val) => val,
        None => File::open(file_path).await?.metadata().await?.len(),
    };
    if file_len == 0 {
        return Err(UtilsError::UnexpectedError(eyre!(
            "File is empty: {}",
            file_path
        )));
    }

    let chunk_size = chunk_size.unwrap_or(CHUNK_SIZE);
    let max_chunks = max_chunks.unwrap_or(MAX_CHUNKS);
    let chunk_count = (file_len + chunk_size - 1) / chunk_size;
    if chunk_count > max_chunks {
        return Err(UtilsError::UnexpectedError(eyre!(
            "Too many chunks for {}: {}. Increase chunk size or max_chunks.",
            file_path,
            chunk_count
        )));
    }

    let multipart_upload_res = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res.upload_id().ok_or_else(|| {
        UtilsError::UnexpectedError(eyre!("No upload ID returned for file: {}", file_path))
    })?;

    let mut completed_parts = Vec::with_capacity(chunk_count as usize);
    for part_index in 0..chunk_count {
        let offset = part_index * chunk_size;
        let this_chunk_size = std::cmp::min(chunk_size, file_len - offset);

        let stream = ByteStream::read_from()
            .path(path)
            .offset(offset)
            .length(Length::Exact(this_chunk_size))
            .build()
            .await?;

        let part_number = (part_index + 1) as i32;

        let upload_part = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(stream)
            .send()
            .await?;

        let e_tag = upload_part.e_tag.ok_or_else(|| {
            UtilsError::UnexpectedError(eyre!("Missing ETag for part {}", part_number))
        })?;

        completed_parts.push(
            CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part_number)
                .build(),
        );
    }

    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    // Verify the upload
    let result = client.get_object().bucket(bucket).key(key).send().await?;
    let uploaded_size = result.content_length().unwrap_or(0) as u64;
    if uploaded_size != file_len {
        return Err(UtilsError::UnexpectedError(eyre!(
            "Size mismatch after upload. Expected {}, got {}",
            file_len,
            uploaded_size
        )));
    }
    println!("Uploaded file: {}", file_path);
    Ok(())
}
