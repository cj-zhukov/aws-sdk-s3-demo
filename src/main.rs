use rs_aws_sdk_s3_demo::{download_file, get_aws_client, list_keys, list_keys_to_map, try_get_file, upload_file, upload_object_multipart};

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let client = get_aws_client("eu-central-1").await;

    let res = try_get_file(client.clone(), "bucket", "file-doesnot-exist").await?; 
    println!("{:?}", res); // None

    let files = list_keys(client.clone(), "bucket", "path/to/data/foo").await?;
    println!("{:?}", files);

    let files = list_keys_to_map(client.clone(), "bucket", "path/to/data/foo").await?;
    println!("{:?}", files);

    upload_file(client.clone(), "bucket", "foo.txt", "path/to/data/foo").await?;

    download_file(client.clone(), "bucket", "path/to/data/foo", "foo").await?;

    upload_object_multipart(client.clone(), "bucket", "foo", "path/to/data/foo", None, None, None).await?;

    Ok(())
}
