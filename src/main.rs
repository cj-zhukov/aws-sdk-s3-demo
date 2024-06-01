use rs_aws_sdk_s3_demo::{get_aws_client, list_keys, list_keys_to_map, upload_file, download_file, upload_object_multipart,  Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = get_aws_client("eu-central-1").await;

    let files = list_keys(client.clone(), "bucket", "path/to/data/").await?;
    println!("{:?}", files);

    let files = list_keys_to_map(client.clone(), "bucket", "path/to/data/").await?;
    println!("{:?}", files);

    upload_file(client.clone(), "bucket", "foo.txt", "path/to/data/foo.txt").await?;

    download_file(client.clone(), "bucket", "path/to/data/foo.txt", "foo.txt").await?;

    upload_object_multipart(client.clone(), "bucket", "foo.zip", "path/to/data/foo.zip", None, None, None).await?;

    Ok(())
}
