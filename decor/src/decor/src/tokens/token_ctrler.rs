use crate::tokens::*;
//use log::warn;
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use rand::{rngs::OsRng, RngCore};
use rsa::{PaddingScheme, RsaPrivateKey, RsaPublicKey, PublicKey};
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

    pub fn insert_token(&mut self, token_type: TokenType, token: &mut Token) {
        let did = token.disguise_id;
        let uid = token.user_id;
        let p = self
            .principal_tokens
            .get_mut(&uid)
            .expect("no user with uid found?");

        // get the symmetric key being used for this disguise
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

        // encrypt the token with the symmetric key
        let mut nonce: Vec<u8> = repeat(0u8).take(128).collect();
        self.rng.fill_bytes(&mut nonce[..]);
        token.nonce = nonce.to_vec();
        let cipher = Aes128Cbc::new_from_slices(&symkey, &nonce).unwrap();
        
        // insert encrypted token into list for principal
        let next_token_ptr = self.rng.next_u64();
        let disguise_lists = match token_type {
            TokenType::Data => &mut p.cd_lists,
            TokenType::PrivKey => &mut p.privkey_lists,
        };
        match disguise_lists.get_mut(&did) {
            // if the list exists, just append and set the tail
            Some(tokenls) => {
                token.last_tail = tokenls.tail;
                tokenls.tail = next_token_ptr;
            },
            // if the list doesn't exist, also encrypt and set the symmetric key
            None => {
                // XXX the last tail could legit be 0, although this is so improbable
                token.last_tail = 0;
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let enc_symkey = p.pubkey.encrypt(&mut self.rng, padding, &symkey[..]).expect("failed to encrypt");
                let tokenls = TokenList {
                    tail: next_token_ptr,
                    encrypted_symkey: enc_symkey,
                };
                disguise_lists.insert(did, tokenls);
            }
        }

        // encrypt and add the token to the encrypted tokens array
        // ensure that no token existed at this pointer before
        let plaintext = serialize_to_bytes(&token);
        let encrypted = cipher.encrypt_vec(&plaintext);
        assert_eq!(self.encrypted_tokens.insert(next_token_ptr, encrypted), None);
    }

    pub fn end_disguise(&mut self) {
        for (_pid, p) in &mut self.principal_tokens {
            p.tmp_symkeys.clear();
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

    pub fn new_anon_principal(&mut self, uid: u64, real_principal: u64, did: u64) -> u64{
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
        
        let mut token: Token = Token::new_privkey_token(did, uid, private_key);
        self.insert_token(TokenType::PrivKey, &mut token);
        anon_uid
    }

    pub fn get_encrypted_capability(&self, uid: u64, did: u64) -> Option<Vec<u8>> {
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        if let Some(tokenls) = p.cd_lists.get(&did) {
            match self.encrypted_tokens.get(&tokenls.tail) {
                Some(enc_token) => return Some(enc_token.clone()),
                None => return None,
            } 
        }
        if let Some(tokenls) = p.privkey_lists.get(&did) {
            match self.encrypted_tokens.get(&tokenls.tail) {
                Some(enc_token) => return Some(enc_token.clone()),
                None => return None,
            } 
        }
        None
    }

    pub fn get_tokens(&mut self, uid: u64, did: u64, symkey: Vec<u8>, nonce: Vec<u8>) -> Vec<Token> {
        let mut tokens = vec![];
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        if let Some(tokenls) = p.cd_lists.get(&did) {
            // TODO check that client didn't forge symkey
            let mut tail_ptr = tokenls.tail;
            loop {
                match self.encrypted_tokens.get_mut(&tail_ptr) {
                    Some(mut enc_token) => {
                        // decrypt token with symkey provided by client
                        let cipher = Aes128Cbc::new_from_slices(&symkey, &nonce).unwrap();
                        let plaintext = cipher.decrypt_vec(&mut enc_token).unwrap();
                        let token = Token::token_from_bytes(plaintext);
                        
                        // add token to list
                        
                        // loop
                        tail_ptr = token.last_tail;
                        tokens.push(token.clone());
                    }
                    None => break,
                }
            }
        }
        tokens
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
