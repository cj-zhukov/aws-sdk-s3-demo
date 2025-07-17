use url::Url;

use crate::error::UtilsError;

#[derive(Debug, PartialEq)]
pub struct S3Path {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl S3Path {
    pub fn new(bucket: String, prefix: Option<String>) -> Result<Self, UtilsError> {
        let uri = format!("s3://{}/{}", bucket, prefix.unwrap_or_default());
        parse_s3_uri(&uri)
    }

    pub fn from_uri(uri: &str) -> Result<Self, UtilsError> {
        parse_s3_uri(uri)
    }

    pub fn uri(&self) -> String {
        let bucket = &self.bucket;
        let prefix = self.prefix.as_deref().unwrap_or_default();
        format!("s3://{bucket}/{prefix}")
    }
}

fn parse_s3_uri(uri: &str) -> Result<S3Path, UtilsError> {
    let parsed = Url::parse(uri)?;
    let bucket = parsed.host_str().ok_or(UtilsError::InvalidS3Uri)?.to_string();
    let prefix = parsed.path()
        .strip_prefix('/')
        .filter(|s| !s.is_empty())
        .map(String::from);
    Ok(S3Path { bucket, prefix })
}

#[cfg(test)]
mod tests {
    use color_eyre::Result;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("s3://bucket/path/to/data/", S3Path { bucket: "bucket".to_string(), prefix: Some(String::from("path/to/data/")) })]
    #[case("s3://bucket/path/to/data", S3Path { bucket: "bucket".to_string(), prefix: Some(String::from("path/to/data")) })]
    #[case("s3://bucket/", S3Path { bucket: "bucket".to_string(), prefix: None })]
    #[case("s3://bucket", S3Path { bucket: "bucket".to_string(), prefix: None })]
    fn test_parse_s3_uri(
        #[case] uri: &str,
        #[case] expected: S3Path,
    ) -> Result<()> {
        let res = parse_s3_uri(uri)?;
        assert_eq!(expected, res);
        Ok(())
    }

    #[rstest]
    #[case("bucket/path/to/data/")]
    #[case("foo")]
    #[case("s3")]
    #[case("")]
    fn test_parse_s3_uri_err(#[case] uri: &str) -> Result<()> {
        let res = parse_s3_uri(uri);
        assert!(res.is_err());
        Ok(())
    }

    #[rstest]
    #[case(S3Path { bucket: "bucket".to_string(), prefix: Some("path/to/data/".to_string()) }, "s3://bucket/path/to/data/")]
    #[case(S3Path { bucket: "bucket".to_string(), prefix: Some("path/to/data".to_string()) }, "s3://bucket/path/to/data")]
    #[case(S3Path { bucket: "bucket".to_string(), prefix: None }, "s3://bucket/")]
    fn test_uri(
        #[case] s3path: S3Path,
        #[case] expected: &str,
    ) -> Result<()> {
        assert_eq!(expected, s3path.uri());
        Ok(())
    }
}