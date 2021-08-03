use crate::helpers::*;
use crate::tokens::*;
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use log::warn;
use rand::{rngs::OsRng, RngCore};
use rsa::pkcs1::FromRsaPrivateKey;
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::iter::repeat;

const RSA_BITS: usize = 2048;

// create an alias for convenience
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

type DID = u64;
type UID = u64;

#[derive(Clone)]
pub struct EncListTail {
    pub tail: u64,
    pub list_enc_symkey: EncListSymKey,
}

#[derive(Clone)]
pub struct EncListSymKey {
    pub enc_symkey: Vec<u8>,
    pub uid: u64,
    pub did: u64,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ListSymKey {
    pub symkey: Vec<u8>,
    pub uid: u64,
    pub did: u64,
}

#[derive(Clone)]
pub struct PrincipalData {
    // each principal has two lists of encrypted tokens,
    // sharded by disguise ID
    cd_lists: HashMap<DID, EncListTail>,
    privkey_lists: HashMap<DID, EncListTail>,
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

    pub fn insert_global_token(&mut self, token: &mut Token) {
        if let Some(user_disguise_tokens) = self
            .global_vault
            .get_mut(&(token.disguise_id, token.user_id))
        {
            user_disguise_tokens.push(token.clone());
        } else {
            self.global_vault
                .insert((token.disguise_id, token.user_id), vec![token.clone()]);
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
                let tokenls = EncListTail {
                    tail: next_token_ptr,
                    list_enc_symkey: EncListSymKey {
                        enc_symkey: enc_symkey,
                        uid: uid,
                        did: did,
                    },
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
        assert_eq!(encrypted.len() % 16, 0);
        warn!(
            "Encrypted token data of len {} with symkey {:?}-{:?}",
            encrypted.len(),
            symkey,
            iv
        );
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

    pub fn register_principal(&mut self, uid: u64, pubkey: &RsaPublicKey) {
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

    pub fn create_anon_principal(&mut self, uid: u64, did: u64) -> u64 {
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

        let mut token: Token = Token::new_privkey_token(did, uid, anon_uid, &private_key);
        self.insert_user_token(TokenType::PrivKey, &mut token);
        self.insert_global_token(&mut token);
        anon_uid
    }

    pub fn get_encrypted_symkey(
        &self,
        uid: u64,
        did: u64,
        token_type: TokenType,
    ) -> Option<EncListSymKey> {
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        let disguise_lists = match token_type {
            TokenType::Data => &p.cd_lists,
            TokenType::PrivKey => &p.privkey_lists,
        };
        if let Some(tokenls) = disguise_lists.get(&did) {
            return Some(tokenls.list_enc_symkey.clone());
        }
        None
    }

    pub fn get_global_tokens(&self, uid: u64, did: u64) -> Vec<Token> {
        if let Some(global_tokens) = self.global_vault.get(&(did, uid)) {
            return global_tokens.to_vec();
        }
        vec![]
    }

    fn get_all_principal_symkeys(&self, uid: u64, priv_key: RsaPrivateKey) -> HashSet<ListSymKey> {
        let mut symkeys = HashSet::new();
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        for (_, tokenls) in &p.cd_lists {
            let padding = PaddingScheme::new_pkcs1v15_encrypt();
            let symkey = priv_key
                .decrypt(padding, &tokenls.list_enc_symkey.enc_symkey)
                .expect("failed to decrypt");
            symkeys.insert(ListSymKey {
                uid: uid,
                did: tokenls.list_enc_symkey.did,
                symkey: symkey,
            });
        }
        for (_, tokenls) in &p.privkey_lists {
            let padding = PaddingScheme::new_pkcs1v15_encrypt();
            let symkey = priv_key
                .decrypt(padding, &tokenls.list_enc_symkey.enc_symkey)
                .expect("failed to decrypt");
            symkeys.insert(ListSymKey {
                uid: uid,
                did: tokenls.list_enc_symkey.did,
                symkey: symkey,
            });
        }
        symkeys
    }

    pub fn get_tokens(&mut self, symkeys: &HashSet<ListSymKey>) -> Vec<Token> {
        let mut cd_tokens = vec![];
        for symkey in symkeys {
            let p = self
                .principal_tokens
                .get(&symkey.uid)
                .expect("no user with uid found?");

            // XXX should we check that client didn't forge symkey?
            // we would need to remember padding scheme

            // get all of this user's globally accessible tokens
            cd_tokens.append(&mut self.get_global_tokens(symkey.uid, symkey.did));
            warn!("cd tokens global pushed to len {}", cd_tokens.len());

            // get all of this user's encrypted correlation/datatokens
            if let Some(tokenls) = p.cd_lists.get(&symkey.did) {
                let mut tail_ptr = tokenls.tail;
                loop {
                    match self.user_vaults_map.get_mut(&tail_ptr) {
                        Some(enc_token) => {
                            // decrypt token with symkey provided by client
                            warn!(
                                "Got cd data of len {} with symkey {:?}-{:?}",
                                enc_token.token_data.len(),
                                symkey.symkey,
                                enc_token.iv
                            );
                            let cipher =
                                Aes128Cbc::new_from_slices(&symkey.symkey, &enc_token.iv).unwrap();
                            let plaintext = cipher.decrypt_vec(&mut enc_token.token_data).unwrap();
                            let token = Token::token_from_bytes(plaintext);

                            // add token to list
                            cd_tokens.push(token.clone());
                            warn!("cd tokens uid {} disguise {} pushed to len {}", token.user_id, token.disguise_id, cd_tokens.len());

                            // go to next encrypted token in list
                            tail_ptr = token.last_tail;
                        }
                        None => break,
                    }
                }
            }

            // get all privkey tokens, even from other disguises
            let mut privkey_tokens = vec![];
            if let Some(tokenls) = p.privkey_lists.get(&symkey.did) {
                let mut tail_ptr = tokenls.tail;
                loop {
                    match self.user_vaults_map.get_mut(&tail_ptr) {
                        Some(enc_token) => {
                            // decrypt token with symkey provided by client
                            warn!(
                                "Got privkey data of len {} with symkey {:?}-{:?}",
                                enc_token.token_data.len(),
                                symkey.symkey,
                                enc_token.iv
                            );
                            let cipher =
                                Aes128Cbc::new_from_slices(&symkey.symkey, &enc_token.iv).unwrap();
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

            // use privkey tokens to decrypt symkeys of anon principles, and recursively get all of their cd_tokens
            let mut new_symkeys = HashSet::new();
            for pk_token in &privkey_tokens {
                let priv_key = RsaPrivateKey::from_pkcs1_der(&pk_token.priv_key).unwrap();
                new_symkeys.extend(self.get_all_principal_symkeys(pk_token.new_user_id, priv_key));
            }
            cd_tokens.extend(self.get_tokens(&new_symkeys));
            warn!("cd tokens extended to len {}", cd_tokens.len());
        }
        cd_tokens
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
    fn test_insert_global_token_single() {
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
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.register_principal(uid, &pub_key);

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
        ctrler.insert_global_token(&mut decor_token);
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
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.register_principal(uid, &pub_key);

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
        ctrler.insert_user_token(TokenType::Data, &mut decor_token);
        ctrler.end_disguise();

        // check principal data
        let p = ctrler
            .principal_tokens
            .get(&uid)
            .expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert_eq!(p.cd_lists.len(), 1);
        assert_eq!(p.privkey_lists.len(), 0);
        assert!(p.tmp_symkeys.is_empty());

        // check symkey stored for principal lists
        let cd_ls = p.cd_lists.get(&did).expect("failed to get disguise?");
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey = private_key
            .decrypt(padding, &cd_ls.list_enc_symkey.enc_symkey)
            .expect("failed to decrypt");
        let mut hs = HashSet::new();
        hs.insert(ListSymKey {
            uid: cd_ls.list_enc_symkey.uid,
            did: cd_ls.list_enc_symkey.did,
            symkey: symkey,
        });

        // get tokens
        let cdtokens = ctrler.get_tokens(&hs);
        assert_eq!(cdtokens.len(), 1);
        assert_eq!(cdtokens[0], decor_token);
    }

    #[test]
    fn test_insert_user_token_multi() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let referenced_name = "referenced".to_string();
        let old_fk_value = 5;
        let new_fk_value = 55;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let mut priv_keys = vec![];
        let mut pub_keys = vec![];

        let iters = 2;
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key.clone());

            for d in 1..iters {
                for i in 1..iters {
                    let mut decor_token = Token::new_decor_token(
                        d,
                        u,
                        guise_name.clone(),
                        guise_ids.clone(),
                        referenced_name.clone(),
                        old_fk_value + i,
                        new_fk_value + i,
                        fk_col.clone(),
                    );
                    ctrler.insert_user_token(TokenType::Data, &mut decor_token);
                }
                ctrler.end_disguise();
            }
        }

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_tokens
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert_eq!(p.cd_lists.len(), iters as usize - 1);
            assert_eq!(p.privkey_lists.len(), 0);
            assert!(p.tmp_symkeys.is_empty());

            for d in 1..iters {
                // check symkey stored for principal lists
                let cd_ls = p.cd_lists.get(&d).expect("failed to get disguise?");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let symkey = priv_keys[u as usize - 1]
                    .decrypt(padding, &cd_ls.list_enc_symkey.enc_symkey)
                    .expect("failed to decrypt");
                let mut hs = HashSet::new();
                hs.insert(ListSymKey {
                    uid: cd_ls.list_enc_symkey.uid,
                    did: cd_ls.list_enc_symkey.did,
                    symkey: symkey,
                });

                // get tokens
                let cdtokens = ctrler.get_tokens(&hs);
                assert_eq!(cdtokens.len(), 1);
                for i in 1..iters {
                    assert_eq!(cdtokens[i as usize - 1].old_fk_value, old_fk_value + i);
                    assert_eq!(cdtokens[i as usize - 1].new_fk_value, new_fk_value + i);
                }
            }
        }
    }

    #[test]
    fn test_insert_user_token_multi_privkey() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let referenced_name = "referenced".to_string();
        let old_fk_value = 5;
        let new_fk_value = 55;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let mut priv_keys = vec![];
        let mut pub_keys = vec![];

        let iters = 5;
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key.clone());

            for d in 1..iters {
                let mut decor_token = Token::new_decor_token(
                    d,
                    u,
                    guise_name.clone(),
                    guise_ids.clone(),
                    referenced_name.clone(),
                    old_fk_value + d,
                    new_fk_value + d,
                    fk_col.clone(),
                );
                ctrler.insert_user_token(TokenType::Data, &mut decor_token);
                
                // create an anonymous user
                // and insert some token for the anon user
                //pub fn new_insert_token(
                let anon_uid = ctrler.create_anon_principal(u, d);
                let mut insert_token = Token::new_insert_token(
                    d,
                    anon_uid,
                    guise_name.clone(),
                    guise_ids.clone(),
                    format!("{}", d),
                    vec![]
                );
                ctrler.insert_user_token(TokenType::Data, &mut insert_token);
                ctrler.end_disguise();
            }
        }

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_tokens
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert_eq!(p.cd_lists.len(), iters as usize - 1);
            assert_eq!(p.privkey_lists.len(), iters as usize - 1);
            assert!(p.tmp_symkeys.is_empty());

            for d in 1..iters {
                // check symkey stored for principal lists
                let cd_ls = p.cd_lists.get(&d).expect("failed to get disguise?");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let symkey = priv_keys[u as usize - 1]
                    .decrypt(padding, &cd_ls.list_enc_symkey.enc_symkey)
                    .expect("failed to decrypt");
                let mut hs = HashSet::new();
                hs.insert(ListSymKey {
                    uid: cd_ls.list_enc_symkey.uid,
                    did: cd_ls.list_enc_symkey.did,
                    symkey: symkey,
                });

                // get tokens
                let cdtokens = ctrler.get_tokens(&hs);
                assert_eq!(cdtokens.len(), 2);
                assert_eq!(cdtokens[0].old_fk_value, old_fk_value + d);
                assert_eq!(cdtokens[0].new_fk_value, new_fk_value + d);
                assert!(cdtokens[1].user_id != u);
                assert_eq!(cdtokens[1].referencer_name, format!("{}", d));
            }
        }
    }
}
