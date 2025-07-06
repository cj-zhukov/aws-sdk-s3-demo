use std::collections::HashMap;

use async_stream::stream;
use aws_sdk_s3::{error::SdkError, operation::list_objects_v2::ListObjectsV2Error, Client};
use tokio_stream::Stream;

use crate::error::UtilsError;

/// Get files names
pub async fn list_keys(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, UtilsError> {
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut keys = Vec::new();
    while let Some(page) = paginator.next().await.transpose()? {
        keys.extend(page.contents().iter().filter_map(|obj| {
            let key = obj.key()?;
            (!key.ends_with('/')).then(|| key.to_string())
        }));
    }
    Ok(keys)
}

/// Get files names and size
pub async fn list_keys_to_map(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, i64>, UtilsError> {
    let mut paginator = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut files: HashMap<String, i64> = HashMap::new();
    while let Some(page) = paginator.next().await.transpose()? {
        files.extend(page.contents().iter().filter_map(|obj| {
            let key = obj.key()?;
            (!key.ends_with('/')).then(|| (key.to_string(), obj.size().unwrap_or(0)))
        }));
    }
    Ok(files)
}

/// List keys using stream
/// let mut stream = Box::pin(list_keys_stream(client, "bucket", "prefix/").await.take(10));
pub async fn list_keys_stream<'a>(
    client: &'a Client,
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

            match request.send().await {
                Ok(response) => {
                    if let Some(objects) = response.contents {
                        for obj in objects {
                            if let Some(key) = obj.key() {
                                if !key.ends_with('/') {
                                    yield Ok(key.to_string());
                                }
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
