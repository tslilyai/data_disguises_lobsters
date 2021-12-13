use crate::UID;
use rsa::{pkcs1::ToRsaPrivateKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};
//use log::error;
use  std::mem::size_of_val;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrivkeyToken {
    pub new_uid: UID,
    pub priv_key: Vec<u8>,
}

pub fn privkey_token_from_bytes(bytes: &Vec<u8>) -> PrivkeyToken {
    bincode::deserialize(bytes).unwrap()
}
pub fn privkey_tokens_from_bytes(bytes: &Vec<u8>) -> Vec<PrivkeyToken> {
    bincode::deserialize(bytes).unwrap()
}
pub fn new_privkey_token(
    new_uid: UID,
    priv_key: &RsaPrivateKey,
) -> PrivkeyToken {
    let mut token: PrivkeyToken = Default::default();
    token.new_uid = new_uid;
    token.priv_key = priv_key.to_pkcs1_der().unwrap().as_der().to_vec();

    /*error!("PK DATA: new_uid {}, pk {}, all: {}", 
        size_of_val(&*token.new_uid),
        size_of_val(&*token.priv_key),
        size_of_val(&token),
    );*/
    token
}
