use crate::{helpers::*, vaults::*};
use core::iter::repeat;
use crypto::{aead::*, chacha20poly1305::*};
use log::warn;
use rusoto_core::request::HttpClient;
use rusoto_core::{ByteStream, Region};
use rusoto_credential::ProfileProvider;
use rusoto_s3::{
    Delete, DeleteObjectsRequest, GetObjectRequest, ListObjectsV2Request, ObjectIdentifier,
    PutObjectRequest, S3Client, S3,
};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::str;
use tokio::runtime::Runtime;

pub struct UVClient {
    s3client: S3Client,
    bucket: String,
    max_uid: u64,
    max_did: u64,
    runtime: Runtime,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct UVObject {
    pub key: String,
    pub body: Vec<u8>,
    pub tag: Vec<u8>,
}

impl UVClient {
    pub fn new(b: &str, region: Region) -> UVClient {
        UVClient {
            s3client: S3Client::new_with(
                HttpClient::new().unwrap(),
                ProfileProvider::with_default_configuration("/home/tslilyai/.aws/credentials"),
                region,
            ),
            bucket: b.to_string(),
            max_uid: 0,
            max_did: 0,
            runtime: Runtime::new().unwrap(),
        }
    }

    /*
     * Going to assume that ukey and nonce are constant for a disguise
     */
    pub fn insert_user_ves(&mut self, ukey: &[u8], nonce: &[u8], ves: &Vec<VaultEntry>) {
        if ves.is_empty() {
            return;
        }
        let uid = ves[0].user_id;
        let did = ves[0].disguise_id;
        self.max_uid = max(uid, self.max_uid);
        self.max_did = max(uid, self.max_did);

        // encrypt input with given user key and nonce
        let mut chacha = ChaCha20Poly1305::new(ukey, nonce, &vec![]);

        let plaintxt = ves_to_bytes(ves);
        let mut encrypted: Vec<u8> = repeat(0u8).take(plaintxt.len()).collect();
        let mut tag: Vec<u8> = repeat(0u8).take(16).collect();
        chacha.encrypt(&plaintxt, &mut encrypted, &mut tag);

        // Key will look like UID/disguise
        let uvobj = UVObject {
            key: format!("{}/{}", uid, did),
            body: encrypted,
            tag: tag,
        };
        let serialized = serde_json::to_string(&uvobj).unwrap();
        warn!("Put serialized data {:?}", serialized);

        // insert into user's s3 bucket
        let put_req = PutObjectRequest {
            acl: None,
            body: Some(ByteStream::from(serialized.as_bytes().to_vec())),
            bucket: self.bucket.clone(),
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

        match self.runtime.block_on(self.s3client.put_object(put_req)) {
            Ok(_) => (),
            Err(e) => unimplemented!(
                "Failed to add ve for {} disguise {} to S3 bucket: {}",
                uid,
                did,
                e
            ),
        }
    }

    pub fn get_ves(
        &mut self,
        uid: u64,
        did: Option<u64>,
        ukey: &[u8],
        nonce: &[u8],
    ) -> Vec<VaultEntry> {
        // read objects of user's s3 bucket
        let key_prefix = match did {
            Some(d) => format!("{}/{}", uid, d),
            None => format!("{}/", uid),
        };

        let mut ves = vec![];
        let mut chacha = ChaCha20Poly1305::new(ukey, nonce, &vec![]);
        let mut ct = None;
        let mut started = false;
        while !ct.is_none() || !started {
            let list_req = ListObjectsV2Request {
                bucket: self.bucket.clone(),
                continuation_token: ct.clone(),
                delimiter: None,
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: None,
                prefix: Some(key_prefix.clone()),
                request_payer: None,
                start_after: None,
            };
            match self
                .runtime
                .block_on(self.s3client.list_objects_v2(list_req))
            {
                Ok(output) => {
                    ct = output.next_continuation_token;
                    match output.contents {
                        Some(metaobjs) => {
                            let mut get_req = GetObjectRequest {
                                bucket: self.bucket.clone(),
                                expected_bucket_owner: None,
                                if_match: None,
                                if_modified_since: None,
                                if_none_match: None,
                                if_unmodified_since: None,
                                key: String::new(),
                                part_number: None,
                                range: None,
                                request_payer: None,
                                response_cache_control: None,
                                response_content_disposition: None,
                                response_content_encoding: None,
                                response_content_language: None,
                                response_content_type: None,
                                response_expires: None,
                                sse_customer_algorithm: None,
                                sse_customer_key: None,
                                sse_customer_key_md5: None,
                                version_id: None,
                            };
                            for o in metaobjs {
                                get_req.key = o.key.unwrap();
                                warn!("Got object key {}", get_req.key);
                                match self
                                    .runtime
                                    .block_on(self.s3client.get_object(get_req.clone()))
                                {
                                    Ok(obj) => {
                                        let mut serialized = vec![];
                                        match obj.content_length {
                                            Some(n) => {
                                                warn!("Got data of len {:?}", n);
                                                let mut body =
                                                    obj.body.unwrap().into_blocking_read();
                                                body.read_to_end(&mut serialized).unwrap();
                                                warn!("Got serialized data {:?}", serialized);
                                                let uvobj: UVObject = serde_json::from_str(
                                                    &str::from_utf8(&serialized).unwrap(),
                                                )
                                                .unwrap();
                                                let mut plaintxt: Vec<u8> =
                                                    repeat(0u8).take(uvobj.body.len()).collect();
                                                chacha.decrypt(
                                                    &uvobj.body,
                                                    &mut plaintxt,
                                                    &uvobj.tag,
                                                );

                                                let mut tmpves: Vec<VaultEntry> =
                                                    serde_json::from_str(
                                                        &str::from_utf8(&plaintxt).unwrap(),
                                                    )
                                                    .unwrap();
                                                ves.append(&mut tmpves);
                                            }
                                            None => warn!("No data in object?"),
                                        }
                                    }
                                    Err(e) => unimplemented!(
                                        "Failed to get obj for {} to S3 bucket: {}",
                                        uid,
                                        e
                                    ),
                                }
                            }
                        }
                        None => (),
                    }
                }
                Err(e) => unimplemented!("Failed to add ve for {} to S3 bucket: {}", uid, e),
            }
            if !started {
                started = true;
            }
        }
        ves
    }

    fn clear_all_user_vaults(&mut self) {
        let mut objs = vec![];
        let mut ct = None;
        let mut started = false;
        while !ct.is_none() || !started {
            let list_req = ListObjectsV2Request {
                bucket: self.bucket.clone(),
                continuation_token: ct.clone(),
                delimiter: None,
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            };
            match self
                .runtime
                .block_on(self.s3client.list_objects_v2(list_req))
            {
                Ok(output) => {
                    ct = output.next_continuation_token;
                    match output.contents {
                        Some(metaobjs) => {
                            for o in metaobjs {
                                objs.push(ObjectIdentifier {
                                    key: o.key.unwrap(),
                                    version_id: None,
                                });
                            }
                        }
                        None => (),
                    }
                }
                Err(e) => {
                    unimplemented!("Failed to list objs for S3 bucket {}: {}", self.bucket, e)
                }
            }
            if !started {
                started = true;
            }
        }
        if !objs.is_empty() {
            let del_req = DeleteObjectsRequest {
                bucket: self.bucket.clone(),
                bypass_governance_retention: None,
                delete: Delete {
                    objects: objs,
                    quiet: None,
                },
                expected_bucket_owner: None,
                mfa: None,
                request_payer: None,
            };
            match self.runtime.block_on(self.s3client.delete_objects(del_req)) {
                Ok(_) => (),
                Err(e) => {
                    unimplemented!("Failed to delete objs for S3 bucket {}: {}", self.bucket, e)
                }
            }
        }
    }
}

fn create_dummy_ve(n: u64, reversed: bool) -> VaultEntry {
    let name = format!("guise{}", n);
    let val = format!("newval{}", n);
    VaultEntry {
        pred: String::new(),
        vault_id: n,
        disguise_id: n,
        user_id: n,
        guise_name: format!("guise{}", n),
        guise_id_cols: vec![name.clone()],
        guise_owner_cols: vec![name.clone()],
        guise_ids: vec![name.clone()],
        referencer_name: String::new(),
        fk_name: String::new(),
        fk_col: String::new(),
        update_type: n,
        modified_cols: vec![name.clone()],
        old_value: vec![RowVal {
            column: name.clone(),
            value: name.clone(),
        }],
        new_value: vec![RowVal {
            column: name.clone(),
            value: val,
        }],
        reversed: reversed,
        priority: 0,
    }
}

#[test]
fn test_insert_ve() {
    use rand::{OsRng, RngCore};

    let test_bucket: &'static str = "edna-uservaults-test";
    let test_region: Region = Region::UsEast1;

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .is_test(true)
        .try_init();

    // create client and reset for test
    let mut uvclient = UVClient::new(test_bucket, test_region);
    uvclient.clear_all_user_vaults();

    // insert vault entries for n users
    let n = 2;
    let mut gen = OsRng::new().expect("Failed to get OS random generator");
    let mut key: Vec<u8> = repeat(0u8).take(32).collect();
    gen.fill_bytes(&mut key[..]);
    let mut nonce: Vec<u8> = repeat(0u8).take(8).collect();
    gen.fill_bytes(&mut nonce[..]);
    for i in 0..n {
        let vault_entries = vec![create_dummy_ve(i, None)];
        uvclient.insert_user_ves(&key, &nonce, &vault_entries);
    }

    // get vault entries for n users
    for i in 0..n {
        let ves_user = uvclient.get_ves(i, None, &key, &nonce);
        let ves_user_disg = uvclient.get_ves(i, Some(i), &key, &nonce);
        warn!("Got user ves {:?}", ves_user);
        warn!("Got user disg ves {:?}", ves_user_disg);
        assert_eq!(ves_user.len(), 1);
        assert_eq!(ves_user_disg.len(), 1);

        let correct = create_dummy_ve(i, None);
        assert_eq!(ves_user[0], correct);
        assert_eq!(ves_user_disg[0], correct);
    }
}
