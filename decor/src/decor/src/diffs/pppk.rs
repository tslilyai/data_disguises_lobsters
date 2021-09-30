use crate::{DID, UID};
use rsa::{pkcs1::ToRsaPrivateKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PPPrivKey {
    pub did: DID,
    pub uid: UID,
    pub new_uid: UID,
    pub priv_key: Vec<u8>,
}

pub fn ppprivkey_from_bytes(bytes: &Vec<u8>) -> PPPrivKey {
    serde_json::from_slice(bytes).unwrap()
}

pub fn new_ppprivkey(uid: UID, did: DID, new_uid: UID, priv_key: &RsaPrivateKey) -> PPPrivKey {
    let mut pppk: PPPrivKey = Default::default();
    pppk.uid = uid;
    pppk.did = did;
    pppk.priv_key = priv_key.to_pkcs1_der().unwrap().as_der().to_vec();
    pppk.new_uid = new_uid;
    pppk
}
