use crate::tokens::*;
use log::warn;
use crate::helpers::*;
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use rand::{rngs::OsRng, RngCore};
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use rsa::{pkcs1::ToRsaPrivateKey};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::iter::repeat;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedToken {
    token_data: Vec<u8>,
    iv: Vec<u8>,
}

#[derive(Clone)]
pub struct TokenCtrler {
    // principal tokens are stored indexed by some large random num
    pub principal_tokens: HashMap<UID, PrincipalData>,

    // a large array of encrypted tokens indexed by random number
    pub user_vaults_map: HashMap<u64, EncryptedToken>,

    pub global_vault: HashMap<(DID, UID), Vec<Token>>,

    pub rng: OsRng,
    pub hasher: Sha256,
}

impl TokenCtrler {
    pub fn new() -> TokenCtrler {
        TokenCtrler {
            principal_tokens: HashMap::new(),
            user_vaults_map: HashMap::new(),
            global_vault: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
        }
    }

    pub fn insert_global_token(&mut self, token_type: TokenType, token: &mut Token) {
        if let Some(user_disguise_tokens) = self.global_vault.get_mut(&(token.disguise_id, token.user_id)) {
            user_disguise_tokens.push(token.clone());
        } else {
            self.global_vault.insert((token.disguise_id, token.user_id), vec![token.clone()]);
        }
    }

    pub fn insert_user_token(&mut self, token_type: TokenType, token: &mut Token) {
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
                let mut key: Vec<u8> = repeat(0u8).take(16).collect();
                self.rng.fill_bytes(&mut key[..]);
                p.tmp_symkeys.insert(did, key.clone());
                key
            }
        };
        warn!("symkey insert user {} disguise {} token is {:?}", uid, did, symkey);

        // give the token a random nonce
        token.nonce = self.rng.next_u64();

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
            }
            // if the list doesn't exist, also encrypt and set the symmetric key
            None => {
                // XXX the last tail could legit be 0, although this is so improbable
                token.last_tail = 0;
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let enc_symkey = p
                    .pubkey
                    .encrypt(&mut self.rng, padding, &symkey[..])
                    .expect("failed to encrypt");
                let tokenls = TokenList {
                    tail: next_token_ptr,
                    encrypted_symkey: enc_symkey,
                };
                disguise_lists.insert(did, tokenls);
            }
        }

        // encrypt and add the token to the encrypted tokens array
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&symkey, &iv).unwrap();
        let plaintext = serialize_to_bytes(&token);
        let encrypted = cipher.encrypt_vec(&plaintext);
        // ensure that no token existed at this pointer before
        assert_eq!(
            self.user_vaults_map.insert(
                next_token_ptr,
                EncryptedToken {
                    token_data: encrypted,
                    iv: iv,
                }
            ),
            None
        );
    }

    // XXX note this doesn't allow for concurrent disguising right now
    pub fn end_disguise(&mut self) {
        for (_pid, p) in &mut self.principal_tokens {
            p.tmp_symkeys.clear();
        }
    }

    pub fn new_real_principal(&mut self, uid: u64, pubkey: &RsaPublicKey) {
        self.principal_tokens.insert(
            uid,
            PrincipalData {
                cd_lists: HashMap::new(),
                privkey_lists: HashMap::new(),
                pubkey: pubkey.clone(),
                tmp_symkeys: HashMap::new(),
            },
        );
    }

    pub fn new_anon_principal(&mut self, uid: u64, real_principal: u64, did: u64) -> u64 {
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

        let mut token: Token = Token::new_privkey_token(did, uid, &private_key);
        self.insert_user_token(TokenType::PrivKey, &mut token);
        self.insert_global_token(TokenType::Data, &mut token);
        anon_uid
    }

    pub fn get_encrypted_symkey(&self, uid: u64, did: u64) -> Option<Vec<u8>> {
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        if let Some(tokenls) = p.cd_lists.get(&did) {
            return Some(tokenls.encrypted_symkey.clone());
        }
        if let Some(tokenls) = p.privkey_lists.get(&did) {
            return Some(tokenls.encrypted_symkey.clone());
        }
        None
    }

    pub fn get_global_tokens(&self, uid: u64, did: u64) -> Vec<Token> {
        if let Some(global_tokens) = self.global_vault.get(&(did, uid)) {
            return global_tokens.to_vec();
        }
        vec![]
    }

    pub fn get_tokens(&mut self, uid: u64, did: u64, symkey: Vec<u8>) -> (Vec<Token>, Vec<Token>) {
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        
        // XXX should we check that client didn't forge symkey?
        // we would need to remember padding scheme
        /*assert_eq!(
            p.pubkey
                .encrypt(&mut self.rng, padding, &symkey[..])
                .expect("failed to encrypt"),
            symkey
        );*/

        let mut cd_tokens = vec![];
        let mut privkey_tokens = vec![];

        // get all of this user's globally accessible tokens
        cd_tokens.append(&mut self.get_global_tokens(uid, did));

        // get all of this user's encrypted correlation/datatokens
        if let Some(tokenls) = p.cd_lists.get(&did) {
            let mut tail_ptr = tokenls.tail;
            loop {
                match self.user_vaults_map.get_mut(&tail_ptr) {
                    Some(enc_token) => {
                        // decrypt token with symkey provided by client
                        let cipher = Aes128Cbc::new_from_slices(&symkey, &enc_token.iv).unwrap();
                        let plaintext = cipher.decrypt_vec(&mut enc_token.token_data).unwrap();
                        let token = Token::token_from_bytes(plaintext);

                        // add token to list
                        cd_tokens.push(token.clone());

                        // go to next encrypted token in list
                        tail_ptr = token.last_tail;
                    }
                    None => break,
                }
            }
        }

        // get all privkey tokens, even from other disguises
        for (_, tokenls) in p.privkey_lists.iter() {
            let mut tail_ptr = tokenls.tail;
            loop {
                match self.user_vaults_map.get_mut(&tail_ptr) {
                    Some(enc_token) => {
                        // decrypt token with symkey provided by client
                        let cipher = Aes128Cbc::new_from_slices(&symkey, &enc_token.iv).unwrap();
                        let plaintext = cipher.decrypt_vec(&mut enc_token.token_data).unwrap();
                        let token = Token::token_from_bytes(plaintext);

                        // add token to list
                        privkey_tokens.push(token.clone());

                        // go to next encrypted token in list
                        tail_ptr = token.last_tail;
                    }
                    None => break,
                }
            }
        }

        (cd_tokens, privkey_tokens)
    }
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_global_token_single () {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let did = 1;
        let uid = 11;
        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let referenced_name = "referenced".to_string();
        let old_fk_value = 5;
        let new_fk_value = 55;
        let fk_col = "fk_col".to_string();
        
        let mut rng = OsRng;
        let private_key =
            RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.new_real_principal(uid, &pub_key);

        let mut decor_token = Token::new_decor_token(
            did,
            uid,
            guise_name,
            guise_ids,
            referenced_name,
            old_fk_value,
            new_fk_value,
            fk_col,
        );
        ctrler.insert_global_token(TokenType::Data, &mut decor_token);
        assert_eq!(ctrler.global_vault.len(), 1);
        let tokens = ctrler.get_global_tokens(uid, did);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], decor_token);
    }

    #[test]
    fn test_insert_user_token_single() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let did = 1;
        let uid = 11;
        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let referenced_name = "referenced".to_string();
        let old_fk_value = 5;
        let new_fk_value = 55;
        let fk_col = "fk_col".to_string()       ;

        let mut rng = OsRng;
        let private_key =
            RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.new_real_principal(uid, &pub_key);

        let mut decor_token = Token::new_decor_token(
            did,
            uid,
            guise_name,
            guise_ids,
            referenced_name,
            old_fk_value,
            new_fk_value,
            fk_col,
        );
        let mut privkey_token = Token::new_privkey_token(
            did,
            uid,
            &private_key,
        );
        ctrler.insert_user_token(TokenType::Data, &mut decor_token);
        ctrler.insert_user_token(TokenType::PrivKey, &mut privkey_token);
        ctrler.end_disguise();

        // check principal data
        let p = ctrler.principal_tokens.get(&uid).expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert_eq!(p.cd_lists.len(), 1);
        assert_eq!(p.privkey_lists.len(), 1);
        assert!(p.tmp_symkeys.is_empty());
        
        // check symkey stored for principal lists
        let cd_ls = p.cd_lists.get(&did).expect("failed to get disguise?");
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey1 = private_key.decrypt(padding, &cd_ls.encrypted_symkey).expect("failed to decrypt");
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        warn!("symkey1 is {:?}", symkey1);

        let privkey_ls = p.privkey_lists.get(&did).expect("failed to get disguise?");
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey2 = private_key.decrypt(padding, &privkey_ls.encrypted_symkey).expect("failed to decrypt");
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        warn!("symkey2 is {:?}", symkey1);

        assert_eq!(symkey1, symkey2);

        // get tokens
        let (cdtokens, privkeytokens) = ctrler.get_tokens(uid, did, symkey1);
        assert_eq!(cdtokens.len(), 1);
        assert_eq!(privkeytokens.len(), 1);
        assert_eq!(cdtokens[0], decor_token);
        assert_eq!(privkeytokens[0], privkey_token);
    }

    #[test]
    fn test_insert_user_token_multi() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let did = 1;
        let uid = 11;
        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let referenced_name = "referenced".to_string();
        let old_fk_value = 5;
        let new_fk_value = 55;
        let fk_col = "fk_col".to_string()       ;

        let mut rng = OsRng;
        let mut priv_keys = vec![];
        let mut pub_keys = vec![];
        
        for u in 0..5 {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.new_real_principal(uid+u, &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key.clone());

            for d in 0..5 {
                for i in 0..5 {
                    let mut decor_token = Token::new_decor_token(
                        did+d,
                        uid+u,
                        guise_name.clone(),
                        guise_ids.clone(),
                        referenced_name.clone(),
                        old_fk_value+i,
                        new_fk_value+i,
                        fk_col.clone(),
                    );
                    ctrler.insert_user_token(TokenType::Data, &mut decor_token);
                }
                let mut privkey_token = Token::new_privkey_token(
                    did+d,
                    uid+u,
                    &private_key,
                );
                ctrler.insert_user_token(TokenType::PrivKey, &mut privkey_token);
                ctrler.end_disguise();
            }
        }

        for u in 0..5 {
            // check principal data
            let p = ctrler.principal_tokens.get(&(uid+u)).expect("failed to get user?").clone();
            assert_eq!(pub_keys[u as usize], p.pubkey);
            assert_eq!(p.cd_lists.len(), 5);
            assert_eq!(p.privkey_lists.len(), 5);
            assert!(p.tmp_symkeys.is_empty());

            for d in 0..5 {
                // check symkey stored for principal lists
                let cd_ls = p.cd_lists.get(&(did+d)).expect("failed to get disguise?");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let symkey1 = priv_keys[u as usize].decrypt(padding, &cd_ls.encrypted_symkey).expect("failed to decrypt");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                warn!("symkey1 is {:?}", symkey1);

                let privkey_ls = p.privkey_lists.get(&(did+d)).expect("failed to get disguise?");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let symkey2 = priv_keys[u as usize].decrypt(padding, &privkey_ls.encrypted_symkey).expect("failed to decrypt");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                warn!("symkey2 is {:?}", symkey1);

                assert_eq!(symkey1, symkey2);

                // get tokens
                let (cdtokens, privkeytokens) = ctrler.get_tokens(uid, did, symkey1);
                assert_eq!(cdtokens.len(), 5);
                assert_eq!(privkeytokens.len(), 1);
                assert_eq!(privkeytokens[d as usize].priv_key, priv_keys[u as usize].to_pkcs1_der().unwrap().as_der().to_vec());
                for i in 0..5 {
                    assert_eq!(cdtokens[i as usize].old_fk_value, old_fk_value+i);
                    assert_eq!(cdtokens[i as usize].new_fk_value, new_fk_value+i);
                }
            }
        }
    }
}
