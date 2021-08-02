use crate::tokens::*;
//use log::warn;
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use rand::{rngs::OsRng, RngCore};
use rsa::{PaddingScheme, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::iter::repeat;
use crate::helpers::*;

const RSA_BITS: usize = 2048;

// create an alias for convenience
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

type DID = u64;
type UID = u64;

#[derive(Clone)]
pub struct TokenList {
    pub tail: u64,
    pub encrypted_symkey: Vec<u8>,
}

#[derive(Clone)]
pub struct PrincipalData {
    // each principal has two lists of encrypted tokens,
    // sharded by disguise ID
    cd_lists: HashMap<DID, TokenList>,
    privkey_lists: HashMap<DID, TokenList>,
    pubkey: RsaPublicKey,
    tmp_symkeys: HashMap<DID, Vec<u8>>,
}

#[derive(Clone)]
pub struct TokenCtrler {
    // principal tokens are stored indexed by some large random num
    pub principal_tokens: HashMap<UID, PrincipalData>,

    // a large array of encrypted tokens indexed by random number
    pub encrypted_tokens: HashMap<u64, Vec<u8>>,

    pub global_tokens: HashMap<DID, Vec<Token>>,

    pub rng: OsRng,
    pub hasher: Sha256,
}

impl TokenCtrler {
    pub fn new() -> TokenCtrler {
        TokenCtrler {
            principal_tokens: HashMap::new(),
            encrypted_tokens: HashMap::new(),
            global_tokens: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
        }
    }

    pub fn new_real_principal(&mut self, uid: u64, pubkey: RsaPublicKey) {
        self.principal_tokens.insert(
            uid,
            PrincipalData {
                cd_lists: HashMap::new(),
                privkey_lists: HashMap::new(),
                pubkey: pubkey,
                tmp_symkeys: HashMap::new(),
            },
        );
    }

    pub fn new_anon_principal(&mut self, uid: u64, real_principal: u64, did: u64) {
        let private_key =
            RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);

        // save the anon principal as a new principal with a public key 
        // and initially empty token vaults
        let anon_uid: u64 = self.rng.next_u64();
        self.principal_tokens.insert(
            anon_uid,
            PrincipalData {
                cd_lists: HashMap::new(),
                privkey_lists: HashMap::new(),
                pubkey: pub_key,
                tmp_symkeys: HashMap::new(),
            },
        );

        // insert new private key into private key list for original principal
        let p = self
            .principal_tokens
            .get_mut(&uid)
            .expect("no user with uid found?");

        let symkey = match p.tmp_symkeys.get(&did) {
            // if there's a symkey already, use it
            Some(sk) => sk.clone(),
            // otherwise generate it (and save it temporarily)
            None => {
                let mut key: Vec<u8> = repeat(0u8).take(128).collect();
                self.rng.fill_bytes(&mut key[..]);
                p.tmp_symkeys.insert(did, key.clone());
                key
            }
        };

        // encrypt the private key with the symmetric key
        let mut token: Token = Token::new_privkey_token(did, uid, private_key);
        let mut nonce: Vec<u8> = repeat(0u8).take(128).collect();
        self.rng.fill_bytes(&mut nonce[..]);
        token.nonce = nonce.to_vec();

        let cipher = Aes128Cbc::new_from_slices(&symkey, &nonce).unwrap();
        let plaintext = serialize_to_bytes(&token);
        let encrypted = cipher.encrypt_vec(&plaintext);
    }

    pub fn end_disguise(&mut self) {
        for (_pid, p) in &mut self.principal_tokens {
            p.tmp_symkeys.clear();
        }
    }

    fn insert_token(&mut self, token: Token) {
        let uid = token.user_id;
        let p = self
            .principal_tokens
            .get_mut(&uid)
            .expect("no user with uid found?");

        let symkey = match p.tmp_symkeys.get(&token.disguise_id) {
            // if there's a symkey already, use it
            Some(sk) => sk.clone(),
            // otherwise generate it (and save it temporarily)
            None => {
                let mut key: Vec<u8> = repeat(0u8).take(128).collect();
                self.rng.fill_bytes(&mut key[..]);
                p.tmp_symkeys.insert(token.disguise_id, key.clone());
                key
            }
        };

        /*let tailptr = match self.cd_lists.get(&token.disguise_id) {
            Some(ls) => ls.tail,
            None => {
                let pubkey = RsaPublicKey::from_pkcs1(&p.pubkey).expect("failed to parse key");
                // encrypt symkey with pubkey
                ls.encrypted_symkey = p
                    .pub_key
                    .encrypt(&mut rng, PaddingScheme::new_pkcs1v15(), &symkey)
                    .expect("failed to encrypt");
                -1
            }
        };

        // encrypt the token
        let nonce: Vec<u8> = repeat(0u8).take(128).collect();
        self.rng.fill_bytes(&mut nonce[..]);
        token.nonce = nonce;

        let cipher = Aes128Cbc::new_from_slices(&symkey, &nonce).unwrap();
        let plaintext = Token::token_to_bytes(&token);
        let encrypted = cipher.encrypt_vec(&plaintext);

        // insert encrypted token + next secret into the token map
        self.hasher.input(&key);
        self.tokens.insert(self.hasher.result_bytes(), encrypted);
        self.hasher.reset();

        // update the secrets map to store the new secret for the user
        self.secrets.insert(uid, key);*/
    }

    pub fn get_tokens(&mut self, uid: u64, pubkey: Vec<u8>) -> Vec<Token> {
        /*let mut tokens = vec![];
        let mut key = pubkey;
        let iv: Vec<u8> = repeat(0u8).take(128).collect();
        loop {
            self.hasher.input(&key);
            let hashed_key = self.result_bytes();
            self.hasher.reset();

            match self.tokens.get(&hashed_key) {
                Some(enc_token) => {
                    let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
                    let plaintext = cipher.decrypt_vec(&enc_token).unwrap();
                    let token = serialize_to_bytes(&plaintext);
                    key = token.last_tail;
                    tokens.push(token);
                }
                None => break,
            }
        }
        tokens*/
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_get_token_single() {}

    #[test]
    fn test_insert_get_token_multi() {
        assert_eq!(add(1, 2), 3);
    }
}
