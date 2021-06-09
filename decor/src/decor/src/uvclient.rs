use crate::vault;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    //CompleteMultipartUploadOutput, GetObjectRequest, PutObjectOutput,
    //PutObjectRequest,  S3,
    S3Client, GetObjectOutput
};
use crypto::{ chacha20poly1305::*, aead::* };
use serde::{Deserialize, Serialize};
use std::str;
use std::io::Read;

pub struct UVClient {
    s3client: S3Client,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct UVObject {
    pub body: Vec<u8>, 
    pub tag: Vec<u8>, 
}

impl UVClient {
    pub fn new(region: Region, access_key: String, secret_key: String) -> UVClient {
        UVClient {
            s3client: S3Client::new_with(
                HttpClient::new().unwrap(),
                StaticProvider::new_minimal(access_key, secret_key),
                region,
            ),
        }
    }

    pub fn insert_ve_for_user(&mut self, uid: u64, ukey: &[u8], nonce: &[u8], ve: &vault::VaultEntry) {
        // encrypt input with given user key and nonce
        let chacha =  ChaCha20Poly1305::new(ukey, nonce, &vec![]);
        let plaintxt =  vault::ve_to_bytes(ve);
        let mut encrypted = vec![];
        let mut tag = vec![];
        chacha.encrypt(plaintxt, &mut encrypted, &mut tag);

        let uvobj = UVObject {
            body: encrypted,
            tag: tag,
        };
        let serialized = serde_json::to_string(&uvobj);

        // insert into user's s3 bucket

    }

    pub fn get_ves_of_user(&mut self, uid: u64, ukey: &[u8], nonce: &[u8]) -> Vec<vault::VaultEntry> {
        // read objects of user's s3 bucket
        let objs : Vec<GetObjectOutput> = vec![];
       
        let mut ves = vec![];
        let chacha =  ChaCha20Poly1305::new(ukey, nonce, &vec![]);
        for obj in objs {
            let mut serialized = vec![];
            let mut body = obj.body.unwrap().into_blocking_read();
            body.read(&mut serialized).unwrap();
            let uvobj :UVObject = serde_json::from_str(&str::from_utf8(&serialized).unwrap()).unwrap();
            let mut plaintxt = vec![];
            chacha.decrypt(&uvobj.body, &mut plaintxt, &uvobj.tag);
            
            let ve : vault::VaultEntry = serde_json::from_str(&str::from_utf8(&plaintxt).unwrap()).unwrap();
            ves.push(ve);
        }
        ves
    }
}
