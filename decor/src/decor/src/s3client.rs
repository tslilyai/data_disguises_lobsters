use rusoto_core::request::{HttpClient, TlsError};
use rusoto_core::Region;
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    CompleteMultipartUploadOutput, GetObjectOutput, GetObjectRequest, PutObjectOutput,
    PutObjectRequest, S3Client, StreamingBody, S3,
};


/*pub fn new_s3client_with_credentials(
    region: Region,
    access_key: String,
    secret_key: String,
) -> Result<S3Client, TlsError> {
    Ok(S3Client::new_with(
        HttpClient::new()?,
        StaticProvider::new_minimal(access_key, secret_key),
        region,
    ))
}*/
