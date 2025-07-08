use std::path::Path;
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::Length;
use color_eyre::eyre::eyre;
use tokio::fs::File;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

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

/// Upload file by chunks in parallel with checking size (can be not performant if network is slow)
pub async fn upload_object_multipart_parallel(
    client: &Client,
    bucket: &str,
    file_path: &str,
    key: &str,
    file_size: Option<u64>,
    chunk_size: Option<u64>,
    max_chunks: Option<u64>,
) -> Result<(), UtilsError> {
    println!("Uploading file: {}", file_path);

    let path = Arc::new(Path::new(file_path).to_path_buf());
    let file_len = match file_size {
        Some(val) => val,
        None => File::open(&*path).await?.metadata().await?.len(),
    };
    if file_len == 0 {
        return Err(UtilsError::UnexpectedError(eyre!("File is empty: {}", file_path)));
    }

    let chunk_size = chunk_size.unwrap_or(CHUNK_SIZE);
    let max_chunks = max_chunks.unwrap_or(MAX_CHUNKS);
    let chunk_count = (file_len + chunk_size - 1) / chunk_size;
    if chunk_count > max_chunks {
        return Err(UtilsError::UnexpectedError(eyre!(
            "Too many chunks for {}: {}. Increase chunk size or max_chunks.",
            file_path, chunk_count
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

    let semaphore = Arc::new(Semaphore::new(CHUNKS_WORKERS));
    let mut tasks = JoinSet::new();

    for part_index in 0..chunk_count {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let path = Arc::clone(&path);
        let permit = Arc::clone(&semaphore).acquire_owned().await
            .map_err(|e| UtilsError::UnexpectedError(eyre!("Can't acquire lock: {e}")))?;

        tasks.spawn(async move {
            let offset = part_index * chunk_size;
            let this_chunk_size = std::cmp::min(chunk_size, file_len - offset);
            let part_number = (part_index + 1) as i32;

            let stream = ByteStream::read_from()
                .path(&*path)
                .offset(offset)
                .length(Length::Exact(this_chunk_size))
                .build()
                .await
                .map_err(|e| UtilsError::UnexpectedError(eyre!(e)))?;

            let upload_part = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(stream)
                .send()
                .await
                .map_err(|e| UtilsError::UnexpectedError(eyre!(e)))?;

            let e_tag = upload_part.e_tag.ok_or_else(|| {
                UtilsError::UnexpectedError(eyre!("Missing ETag for part {}", part_number))
            })?;

            drop(permit);

            Ok(CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part_number)
                .build()) as Result<_, UtilsError>
        });
    }

    let mut completed_parts = Vec::with_capacity(chunk_count as usize);
    while let Some(result) = tasks.join_next().await {
        let res: CompletedPart = result
            .map_err(|e| UtilsError::UnexpectedError(eyre!(e)))?
            .map_err(|e| UtilsError::UnexpectedError(eyre!(e)))?;
        completed_parts.push(res);
    }

    completed_parts.sort_by_key(|part| part.part_number());
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

/// Upload file by chunks in parallel with retry and checking size (can be not performant if network is slow)
pub async fn upload_object_multipart_parallel_retry(
    client: &Client,
    bucket: &str,
    file_path: &str,
    key: &str,
    file_size: Option<u64>,
    chunk_size: Option<u64>,
    max_chunks: Option<u64>,
) -> Result<(), UtilsError> {
    println!("Uploading file: {}", file_path);

    let path = Arc::new(Path::new(file_path).to_path_buf());
    let file_len = match file_size {
        Some(val) => val,
        None => File::open(&*path).await?.metadata().await?.len(),
    };
    if file_len == 0 {
        return Err(UtilsError::UnexpectedError(eyre!("File is empty: {}", file_path)));
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

    let semaphore = Arc::new(Semaphore::new(CHUNKS_WORKERS));
    let mut tasks = JoinSet::new();

    for part_index in 0..chunk_count {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let path = Arc::clone(&path);
        let permit = Arc::clone(&semaphore).acquire_owned().await
            .map_err(|e| UtilsError::UnexpectedError(eyre!("Can't acquire semaphore: {e}")))?;

        tasks.spawn(async move {
            let _permit = permit;
            let offset = part_index * chunk_size;
            let this_chunk_size = std::cmp::min(chunk_size, file_len - offset);
            let part_number = (part_index + 1) as i32;

            let mut last_err = None;

            for attempt in 1..=CHUNKS_MAX_RETRY {
                let stream_result = ByteStream::read_from()
                    .path(&*path)
                    .offset(offset)
                    .length(Length::Exact(this_chunk_size))
                    .build()
                    .await;

                let stream = match stream_result {
                    Ok(s) => s,
                    Err(e) => {
                        last_err = Some(UtilsError::UnexpectedError(eyre!("ByteStream error: {e}")));
                        continue;
                    }
                };

                let result = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(stream)
                    .send()
                    .await;

                match result {
                    Ok(upload_part) => {
                        let e_tag = upload_part.e_tag.ok_or_else(|| {
                            UtilsError::UnexpectedError(eyre!("Missing ETag for part {part_number}"))
                        })?;

                        return Ok(
                            CompletedPart::builder()
                                .e_tag(e_tag)
                                .part_number(part_number)
                                .build()
                        );
                    }
                    Err(e) => {
                        last_err = Some(UtilsError::UnexpectedError(eyre!(
                            "Failed to upload part {part_number}, attempt {attempt}: {e}"
                        )));
                        tokio::time::sleep(std::time::Duration::from_millis(300 * attempt)).await;
                    }
                }
            }

            Err(last_err.unwrap_or_else(|| {
                UtilsError::UnexpectedError(eyre!("Part {part_number} failed with unknown error"))
            }))
        });
    }

    let mut completed_parts = Vec::with_capacity(chunk_count as usize);
    while let Some(result) = tasks.join_next().await {
        let res: CompletedPart = result
            .map_err(|e| UtilsError::UnexpectedError(eyre!(e)))?
            .map_err(|e| UtilsError::UnexpectedError(eyre!(e)))?;
        completed_parts.push(res);
    }

    completed_parts.sort_by_key(|part| part.part_number());
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

    let result = client.get_object().bucket(bucket).key(key).send().await?;
    let uploaded_size = result.content_length().unwrap_or(0) as u64;
    if uploaded_size != file_len {
        return Err(UtilsError::UnexpectedError(eyre!(
            "Size mismatch after upload. Expected {}, got {}",
            file_len, uploaded_size
        )));
    }

    println!("Uploaded file: {}", file_path);
    Ok(())
}
