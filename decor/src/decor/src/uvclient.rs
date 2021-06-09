use crate::vault;
use crypto::{aead::*, chacha20poly1305::*};
use rusoto_core::request::HttpClient;
use rusoto_core::{ByteStream, Region};
use rusoto_credential::ProfileProvider;
use futures::executor;
use rusoto_s3::{
    GetObjectOutput,
    //CompleteMultipartUploadOutput, GetObjectRequest, PutObjectOutput,
    PutObjectRequest,
    S3Client,
    S3,
};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::str;

const BUCKET: &'static str = "arn:aws:s3:::edna-uservaults";

pub struct UVClient {
    s3client: S3Client,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct UVObject {
    pub key: String,
    pub body: Vec<u8>,
    pub tag: Vec<u8>,
}

impl UVClient {
    pub fn new() -> UVClient {
        UVClient {
            s3client: S3Client::new_with(
                HttpClient::new().unwrap(),
                ProfileProvider::with_default_configuration("./rootkey.csv"),
                Region::UsEast1,
            ),
        }
    }

    /*
     * Going to assume that ukey and nonce are constant for a disguise
     */
    pub fn insert_ve_for_user(
        &mut self,
        uid: u64,
        ukey: &[u8],
        nonce: &[u8],
        ve: &vault::VaultEntry,
    ) {
        // encrypt input with given user key and nonce
        let mut chacha = ChaCha20Poly1305::new(ukey, nonce, &vec![]);
        let plaintxt = vault::ve_to_bytes(ve);
        let mut encrypted = vec![];
        let mut tag = vec![];
        chacha.encrypt(&plaintxt, &mut encrypted, &mut tag);

        let uvobj = UVObject {
            key: format!("{}/{}", uid, ve.vault_id),
            body: encrypted,
            tag: tag,
        };
        let serialized = serde_json::to_string(&uvobj).unwrap();

        // insert into user's s3 bucket
        // key will look like UID/VEID
        let put_req = PutObjectRequest {
            acl: None,
            body: Some(ByteStream::from(serialized.as_bytes().to_vec())),
            bucket: BUCKET.to_string(),
            bucket_key_enabled: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_length: None,
            content_md5: None,
            content_type: None,
            expected_bucket_owner: None,
            expires: None,
            grant_full_control: None,
            grant_read: None,
            grant_read_acp: None,
            grant_write_acp: None,
            key: uvobj.key,
            metadata: None,
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_date: None,
            request_payer: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            ssekms_encryption_context: None,
            ssekms_key_id: None,
            server_side_encryption: None,
            storage_class: None,
            tagging: None,
            website_redirect_location: None,
        };
        match executor::block_on(self.s3client.put_object(put_req)) {
            Ok(_) => (),
            Err(e) => unimplemented!("Failed to add ve for {} to S3 bucket: {}", uid, e),
        }
    }

    pub fn get_ves_of_user(
        &mut self,
        uid: u64,
        ukey: &[u8],
        nonce: &[u8],
    ) -> Vec<vault::VaultEntry> {
        // read objects of user's s3 bucket
        let objs: Vec<GetObjectOutput> = vec![];

        let mut ves = vec![];
        let mut chacha = ChaCha20Poly1305::new(ukey, nonce, &vec![]);
        for obj in objs {
            let mut serialized = vec![];
            let mut body = obj.body.unwrap().into_blocking_read();
            body.read(&mut serialized).unwrap();
            let uvobj: UVObject =
                serde_json::from_str(&str::from_utf8(&serialized).unwrap()).unwrap();
            let mut plaintxt = vec![];
            chacha.decrypt(&uvobj.body, &mut plaintxt, &uvobj.tag);

            let ve: vault::VaultEntry =
                serde_json::from_str(&str::from_utf8(&plaintxt).unwrap()).unwrap();
            ves.push(ve);
        }
        ves
    }
}
