use crate::helpers::*;
use crate::tokens::*;
use crate::{DID, UID};
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use log::warn;
use rand::{rngs::OsRng, RngCore};
use rsa::pkcs1::FromRsaPrivateKey;
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::iter::repeat;
use std::sync::{Arc, RwLock};

const RSA_BITS: usize = 2048;
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

#[derive(Clone)]
pub struct EncListTail {
    pub tail: u64,
    pub list_enc_symkey: EncListSymKey,
}

#[derive(Clone)]
pub struct EncListSymKey {
    pub enc_symkey: Vec<u8>,
    pub uid: UID,
    pub did: DID,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ListSymKey {
    pub uid: UID,
    pub did: DID,
    pub symkey: Vec<u8>,
}

impl Hash for ListSymKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uid.hash(state);
        self.did.hash(state);
    }
}

#[derive(Clone)]
pub struct PrincipalData {
    // each principal has two lists of encrypted tokens,
    // sharded by disguise ID
    cd_lists: HashMap<DID, EncListTail>,
    privkey_lists: HashMap<DID, EncListTail>,
    pubkey: RsaPublicKey,
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

    pub global_vault: HashMap<(DID, UID), Arc<RwLock<HashSet<Token>>>>,

    pub rng: OsRng,
    pub hasher: Sha256,

    pub tmp_symkeys: HashMap<(UID, DID), ListSymKey>,
}

impl TokenCtrler {
    pub fn new() -> TokenCtrler {
        TokenCtrler {
            principal_tokens: HashMap::new(),
            user_vaults_map: HashMap::new(),
            global_vault: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_symkeys: HashMap::new(),
        }
    }

    pub fn insert_global_token(&mut self, token: &mut Token) {
        token.is_global = true;
        token.token_id = self.rng.next_u64();
        warn!(
            "Inserting global token disguise {} user {}",
            token.did, token.uid
        );
        if let Some(user_disguise_tokens) = self.global_vault.get_mut(&(token.did, token.uid)) {
            let mut tokens = user_disguise_tokens.write().unwrap();
            tokens.insert(token.clone());
        } else {
            let mut hs = HashSet::new();
            hs.insert(token.clone());
            self.global_vault
                .insert((token.did, token.uid), Arc::new(RwLock::new(hs)));
        }
    }

    pub fn insert_user_token(&mut self, token_type: TokenType, token: &mut Token) {
        assert!(token.uid != 0);
        token.is_global = false;
        let did = token.did;
        let uid = token.uid;
        warn!("inserting user token {:?} with uid {} did {}", token, uid, did);
        let p = self
            .principal_tokens
            .get_mut(&uid)
            .expect("no user with uid found?");

        // get the symmetric key being used for this disguise
        let symkey = match self.tmp_symkeys.get(&(uid, did)) {
            // if there's a symkey already, use it
            Some(sk) => sk.symkey.clone(),
            // otherwise generate it (and save it temporarily)
            None => {
                let mut key: Vec<u8> = repeat(0u8).take(16).collect();
                self.rng.fill_bytes(&mut key[..]);
                self.tmp_symkeys.insert(
                    (uid, did),
                    ListSymKey {
                        symkey: key.clone(),
                        uid: uid,
                        did: did,
                    },
                );
                key
            }
        };

        // give the token a random nonce
        token.nonce = self.rng.next_u64();

        // insert encrypted token into list for principal
        let next_token_ptr = self.rng.next_u64();
        token.token_id = next_token_ptr;

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
    pub fn clear_symkeys(&mut self) {
        self.tmp_symkeys.clear();
    }

    pub fn register_principal(&mut self, uid: u64, pubkey: &RsaPublicKey) {
        warn!("Registering principal {}", uid);
        self.principal_tokens.insert(
            uid,
            PrincipalData {
                cd_lists: HashMap::new(),
                privkey_lists: HashMap::new(),
                pubkey: pubkey.clone(),
            },
        );
    }

    pub fn register_anon_principal(&mut self, uid: UID, anon_uid: UID, did: DID) -> u64 {
        let private_key =
            RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);

        // save the anon principal as a new principal with a public key
        // and initially empty token vaults
        self.register_principal(anon_uid, &pub_key);
        let mut token: Token = Token::new_privkey_token(uid, did, anon_uid, &private_key);
        self.insert_user_token(TokenType::PrivKey, &mut token);
        anon_uid
    }

    pub fn remove_anon_principal(&mut self, anon_uid: UID) {
        self.principal_tokens.remove(&anon_uid);
    }
    
    pub fn check_global_token_for_match(&mut self, token: &Token) -> (bool, bool) {
        if let Some(global_tokens) = self.global_vault.get(&(token.did, token.uid)) {
            let tokens = global_tokens.read().unwrap();
            for t in tokens.iter() {
                if t.token_id == token.token_id {
                    // XXX todo this is a bit inefficient
                    let mut mytok = token.clone();
                    mytok.revealed = t.revealed;
                    let eq = mytok == *t;
                    if t.revealed {
                        return (true, eq);
                    }
                    return (false, eq);
                }
            }
        }
        return (false, false);
    }
 
    pub fn remove_global_token(&mut self, uid: UID, did: DID, token: &Token) -> bool {
        assert!(token.is_global);
        assert!(uid != 0);
        let mut found = false;
        
        // delete token 
        if let Some(global_tokens) = self.global_vault.get(&(token.did, token.uid)) {
            let mut tokens = global_tokens.write().unwrap();
            // just insert the token to replace the old one
            tokens.remove(&token);
            found = true;
        }
        // log token for disguise that marks removal
        self.insert_user_token(
            TokenType::Data,
            &mut Token::new_token_remove(uid, did, token),
        );
        return found;
    }

    pub fn update_global_token_from_old_to (
        &mut self,
        old_token: &Token,
        new_token: &Token,
        record_token_for_disguise: Option<(UID,DID)> 
    ) -> bool {
        assert!(new_token.is_global);
        let mut found = false;
        if let Some(global_tokens) = self.global_vault.get(&(new_token.did, new_token.uid)) {
            let mut tokens = global_tokens.write().unwrap();
            // just insert the token to replace the old one
            tokens.insert(new_token.clone());
            found = true;
        }
        if let Some((uid, did)) = record_token_for_disguise {
            self.insert_user_token(
                TokenType::Data,
                &mut Token::new_token_modify(uid, did, old_token, new_token),
            );
        }
        found
    }

    pub fn mark_token_revealed(
        &mut self,
        token: &Token,
    ) -> bool {
        let mut found = false;
        if token.is_global {
            if let Some(global_tokens) = self.global_vault.get(&(token.did, token.uid)) {
                let mut tokens = global_tokens.write().unwrap();
                // just insert the token to replace the old one
                let mut t = token.clone();
                t.revealed = true;
                tokens.insert(t);
                found = true;
            }
            return found;
        }

        // otherwise search the user list
        let symkey = match self.tmp_symkeys.get(&(token.uid, token.did)) {
            Some(sk) => sk,
            None => unimplemented!("Token to update inaccessible!"),
        };

        let p = self
            .principal_tokens
            .get(&symkey.uid)
            .expect("no user with uid found?");

        // iterate through user's encrypted correlation/datatokens
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
                        let mut t = Token::token_from_bytes(plaintext);
                        if t.token_id == token.token_id {
                            t.revealed = true;
                            // XXX do we need a new IV?
                            let cipher =
                                Aes128Cbc::new_from_slices(&symkey.symkey, &enc_token.iv).unwrap();
                            let plaintext = serialize_to_bytes(&t);
                            let encrypted = cipher.encrypt_vec(&plaintext);
                            let iv = enc_token.iv.clone();
                            assert_eq!(encrypted.len() % 16, 0);
                            self.user_vaults_map.insert(
                                tail_ptr,
                                EncryptedToken {
                                    token_data: encrypted,
                                    iv: iv,
                                },
                            );
                            warn!(
                                "token uid {} disguise {} revealed token {}",
                                token.uid, token.did, token.token_id,
                            );
                            found = true;
                            break;
                        }

                        // update which encrypted token is to be next in list
                        tail_ptr = token.last_tail;
                    }
                    None => break,
                }
            }
        }
        found
    }

    pub fn get_global_tokens(&self, uid: UID, did: DID) -> Vec<Token> {
        if let Some(global_tokens) = self.global_vault.get(&(did, uid)) {
            let tokens = global_tokens.read().unwrap();
            return tokens.clone().into_iter().filter(|t| !t.revealed).collect();
        }
        vec![]
    }

    pub fn get_tokens(&mut self, symkeys: &HashSet<ListSymKey>, save_keys: bool) -> Vec<Token> {
        let mut cd_tokens = vec![];

        for symkey in symkeys {
            if save_keys {
                // save symkeys for later in the disguise
                self.tmp_symkeys
                    .insert((symkey.uid, symkey.did), symkey.clone());
            }

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

                            // add token to list only if it hasn't be revealed before
                            if !token.revealed {
                                cd_tokens.push(token.clone());
                            }
                            warn!(
                                "cd tokens uid {} disguise {} pushed to len {}",
                                token.uid,
                                token.did,
                                cd_tokens.len()
                            );

                            // update which encrypted token is to be next in list
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
                new_symkeys.extend(self.get_all_principal_symkeys(pk_token.new_uid, priv_key));
            }
            cd_tokens.extend(self.get_tokens(&new_symkeys, save_keys));
            warn!("cd tokens extended to len {}", cd_tokens.len());
        }
        cd_tokens
    }

    pub fn get_encrypted_symkey(&self, uid: UID, did: DID) -> Option<EncListSymKey> {
        let p = self
            .principal_tokens
            .get(&uid)
            .expect("no user with uid found?");
        if let Some(tokenls) = p.cd_lists.get(&did) {
            return Some(tokenls.list_enc_symkey.clone());
        }
        None
    }

    fn get_all_principal_symkeys(&self, uid: UID, priv_key: RsaPrivateKey) -> HashSet<ListSymKey> {
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
            guise_name,
            guise_ids,
            referenced_name,
            fk_col.clone(),
            vec![RowVal {
                column: fk_col.clone(),
                value: old_fk_value.to_string(),
            }],
            vec![RowVal {
                column: fk_col.clone(),
                value: new_fk_value.to_string(),
            }],
        );
        decor_token.uid = uid;
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
            guise_name,
            guise_ids,
            referenced_name,
            fk_col.clone(),
            vec![RowVal {
                column: fk_col.clone(),
                value: old_fk_value.to_string(),
            }],
            vec![RowVal {
                column: fk_col.clone(),
                value: new_fk_value.to_string(),
            }],
        );
        decor_token.uid = uid;
        ctrler.insert_user_token(TokenType::Data, &mut decor_token);
        ctrler.clear_symkeys();
        assert_eq!(ctrler.global_vault.len(), 0);

        // check principal data
        let p = ctrler
            .principal_tokens
            .get(&uid)
            .expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert_eq!(p.cd_lists.len(), 1);
        assert_eq!(p.privkey_lists.len(), 0);
        assert!(ctrler.tmp_symkeys.is_empty());

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
        let cdtokens = ctrler.get_tokens(&hs, true);
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

        let iters = 5;
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key.clone());

            for d in 1..iters {
                for i in 0..iters {
                    let mut decor_token = Token::new_decor_token(
                        d,
                        guise_name.clone(),
                        guise_ids.clone(),
                        referenced_name.clone(),
                        fk_col.clone(),
                        vec![RowVal {
                            column: fk_col.clone(),
                            value: (old_fk_value + i).to_string(),
                        }],
                        vec![RowVal {
                            column: fk_col.clone(),
                            value: (new_fk_value + i).to_string(),
                        }],
                    );
                    decor_token.uid = u;
                    ctrler.insert_user_token(TokenType::Data, &mut decor_token);
                }
            }
        }
        assert_eq!(ctrler.global_vault.len(), 0);
        ctrler.clear_symkeys();
        assert!(ctrler.tmp_symkeys.is_empty());

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
                let cdtokens = ctrler.get_tokens(&hs, true);
                assert_eq!(cdtokens.len(), (iters as usize));
                for i in 0..iters {
                    assert_eq!(
                        cdtokens[i as usize].old_value[0].value,
                        (old_fk_value + (iters - i - 1) as u64).to_string()
                    );
                    assert_eq!(
                        cdtokens[i as usize].new_value[0].value,
                        (new_fk_value + (iters - i - 1) as u64).to_string()
                    );
                }
            }
        }
    }

    #[test]
    fn test_insert_user_token_privkey() {
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
                    guise_name.clone(),
                    guise_ids.clone(),
                    referenced_name.clone(),
                    fk_col.clone(),
                    vec![RowVal {
                        column: fk_col.clone(),
                        value: (old_fk_value + d).to_string(),
                    }],
                    vec![RowVal {
                        column: fk_col.clone(),
                        value: (new_fk_value + d).to_string(),
                    }],
                );
                decor_token.uid = u;
                ctrler.insert_user_token(TokenType::Data, &mut decor_token);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some token for the anon user
                ctrler.register_anon_principal(u, anon_uid, d);
            }
        }
        assert_eq!(ctrler.global_vault.len(), 0);
        ctrler.clear_symkeys();
        assert!(ctrler.tmp_symkeys.is_empty());

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
                let cdtokens = ctrler.get_tokens(&hs, true);
                assert_eq!(cdtokens.len(), 1);
                assert_eq!(
                    cdtokens[0].old_value[0].value,
                    (old_fk_value + d).to_string()
                );
                assert_eq!(
                    cdtokens[0].new_value[0].value,
                    (new_fk_value + d).to_string()
                );
            }
        }
    }
}
