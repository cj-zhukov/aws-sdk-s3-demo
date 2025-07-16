use url::Url;

use crate::error::UtilsError;

#[derive(Debug, PartialEq)]
pub struct S3Path {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
}

pub fn parse_s3_uri(uri: &str) -> Result<S3Path, UtilsError> {
    let parsed = Url::parse(uri)?;
    let bucket = parsed.host_str().map(String::from);
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
    #[case("s3://bucket/path/to/data/", S3Path { bucket: Some(String::from("bucket")), prefix: Some(String::from("path/to/data/")) })]
    #[case("s3://bucket/path/to/data", S3Path { bucket: Some(String::from("bucket")), prefix: Some(String::from("path/to/data")) })]
    #[case("s3://bucket/", S3Path { bucket: Some(String::from("bucket")), prefix: None })]
    #[case("s3://bucket", S3Path { bucket: Some(String::from("bucket")), prefix: None })]
    #[case("s3://", S3Path { bucket: None, prefix: None })]
    #[case("s3:/", S3Path { bucket: None, prefix: None })]
    #[case("s3:", S3Path { bucket: None, prefix: None })]
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
}