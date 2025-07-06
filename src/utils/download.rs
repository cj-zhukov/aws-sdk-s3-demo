use aws_sdk_s3::Client;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};

use crate::{
    error::UtilsError,
    utils::{get_aws_object, CHUNK_SIZE},
};

pub async fn download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    file_path: &str,
) -> Result<(), UtilsError> {
    let object = get_aws_object(client, bucket, key).await?;
    let content_length = object.content_length().unwrap_or(0) as u64;
    let mut body = object.body;
    let file = File::create(file_path).await?;
    let mut writer = BufWriter::new(file);

    if content_length <= CHUNK_SIZE {
        let mut reader = body.into_async_read();
        let mut buf = Vec::with_capacity(content_length as usize);
        reader.read_to_end(&mut buf).await?;
        writer.write_all(&buf).await?;
    } else {
        while let Some(chunk) = body.try_next().await? {
            writer.write_all(&chunk).await?;
        }
    }

    writer.flush().await?;
    Ok(())
}
