use crate::{DID, UID};
use rand::{thread_rng, Rng};
use rsa::{pkcs1::ToRsaPrivateKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrivkeyToken {
    pub token_id: u64,
    pub old_uid: UID,
    pub new_uid: UID,
    pub did: DID,
    pub priv_key: Vec<u8>,
}

pub fn privkey_token_from_bytes(bytes: &Vec<u8>) -> PrivkeyToken {
    serde_json::from_slice(bytes).unwrap()
}
pub fn privkey_tokens_from_bytes(bytes: &Vec<u8>) -> Vec<PrivkeyToken> {
    serde_json::from_slice(bytes).unwrap()
}
pub fn new_privkey_token(
    old_uid: UID,
    new_uid: UID,
    did: DID,
    priv_key: &RsaPrivateKey,
) -> PrivkeyToken {
    let mut token: PrivkeyToken = Default::default();
    token.token_id = thread_rng().gen();
    token.new_uid = new_uid;
    token.old_uid = old_uid;
    token.did = did;
    token.priv_key = priv_key.to_pkcs1_der().unwrap().as_der().to_vec();
    token
}
