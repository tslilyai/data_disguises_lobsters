use crate::helpers::*;
use crate::tokens::*;
use crate::{DID, UID};
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use log::warn;
use rand::{rngs::OsRng, RngCore};
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::iter::repeat;
use std::sync::{Arc, RwLock};

pub type Capability = u64;
const RSA_BITS: usize = 2048;
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EncSymKey {
    pub enc_symkey: Vec<u8>,
    pub uid: UID,
    pub did: DID,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SymKey {
    pub symkey: Vec<u8>,
    pub uid: UID,
    pub did: DID,
}

impl Hash for SymKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uid.hash(state);
        self.did.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncToken {
    pub token_data: Vec<u8>,
    pub iv: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncPrivKeyToken {
    pub enc_key: Vec<u8>,
    pub enc_token: EncToken,
}

#[derive(Clone)]
pub struct PrincipalData {
    enc_privkey_tokens: Vec<EncPrivKeyToken>,
    pubkey: RsaPublicKey,
    email: String,
    // only for pseudoprincipals!
    capabilities: Vec<Capability>,
}

#[derive(Clone)]
pub struct TokenCtrler {
    // principal tokens are stored indexed by some large random num
    pub principal_data: HashMap<UID, PrincipalData>,

    // (p,d) capability -> set of token ciphertext for principal+disguise
    pub data_tokens_map: HashMap<Capability, Vec<EncToken>>,

    // (p,d) capability -> encrypted symkey for principal+disguise
    pub enc_token_symkeys_map: HashMap<Capability, EncSymKey>,

    pub global_tokens: HashMap<DID, HashMap<UID, Arc<RwLock<HashSet<Token>>>>>,

    // used for randomness stuff
    pub rng: OsRng,
    pub hasher: Sha256,

    // used to temporarily store keys used during disguises
    pub tmp_symkeys: HashMap<(UID, DID), SymKey>,
    pub tmp_capabilities: HashMap<(UID, DID), Capability>,
    
    // XXX get rid of this, just for testing
    pub capabilities: HashMap<(UID, DID), Capability>,
}

impl TokenCtrler {
    pub fn new() -> TokenCtrler {
        TokenCtrler {
            principal_data: HashMap::new(),
            data_tokens_map: HashMap::new(),
            enc_token_symkeys_map: HashMap::new(),
            global_tokens: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_symkeys: HashMap::new(),
            tmp_capabilities: HashMap::new(),

            // XXX get rid of this
            capabilities: HashMap::new(),
        }
    }

    /*
     * TEMP STORAGE AND CLEARING
     */
    pub fn get_tmp_capability(&self, uid: UID, did: DID) -> Option<&Capability> {
        self.tmp_capabilities.get(&(uid, did))
    }

    pub fn save_capabilities(&mut self) {
        for ((uid, did), c) in self.tmp_capabilities.iter() {
            let p = self.principal_data.get_mut(&uid).unwrap();
            // save to principal data if no email (pseudoprincipal)
            if p.email.is_empty() {
                p.capabilities.push(*c);
            } else {
                // TODO email capability to user if user has email
                self.capabilities.insert((*uid, *did), *c);
            }
        }
    }

    // XXX note this doesn't allow for concurrent disguising right now
    pub fn clear_tmp(&mut self) {
        self.tmp_symkeys.clear();
        self.tmp_capabilities.clear();
    }

    /*
     * REGISTRATION
     */
    pub fn register_principal(&mut self, uid: UID, email: String, pubkey: &RsaPublicKey) {
        warn!("Registering principal {}", uid);
        self.principal_data.insert(
            uid,
            PrincipalData {
                enc_privkey_tokens: vec![],
                pubkey: pubkey.clone(),
                email: email,
                capabilities: vec![],
            },
        );
    }
    pub fn register_anon_principal(&mut self, uid: UID, anon_uid: UID, did: DID) -> UID {
        let private_key =
            RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);

        // save the anon principal as a new principal with a public key
        // and initially empty token vaults
        self.register_principal(anon_uid, String::new(), &pub_key);
        let mut token: PrivKeyToken = Token::new_privkey_token(uid, did, anon_uid, &private_key);
        self.insert_privkey_token(&mut token);
        anon_uid
    }

    pub fn get_enc_privkeys_of_user(&self ,uid:UID) -> Vec<EncPrivKeyToken> {
        match self.principal_data.get(&uid) {
            Some(p) => p.enc_privkey_tokens.clone(),
            None => vec![],
        }
    }

    pub fn remove_anon_principal(&mut self, anon_uid: UID) {
        self.principal_data.remove(&anon_uid);
    }

    /*
     * TOKEN INSERT
     */
    pub fn insert_global_token(&mut self, token: &mut Token) {
        token.is_global = true;
        token.token_id = self.rng.next_u64();
        warn!(
            "Inserting global token disguise {} user {}",
            token.did, token.uid
        );
        if let Some(hm) = self.global_tokens.get_mut(&token.did) {
            if let Some(user_disguise_tokens) = hm.get_mut(&token.uid) {
                let mut tokens = user_disguise_tokens.write().unwrap();
                tokens.insert(token.clone());
            } else {
                let mut hs = HashSet::new();
                hs.insert(token.clone());
               hm.insert(token.uid), Arc::new(RwLock::new(hs)));
            }
        }
    }

    pub fn insert_privkey_token(&mut self, token: &mut PrivKeyToken) {
        assert!(token.uid != 0);

        let p = self
            .principal_data
            .get_mut(&token.uid)
            .expect("no user with uid found?");

        // generate key
        let mut key: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut key[..]);

        // encrypt token with key
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
        let plaintext = serialize_to_bytes(&token);
        let encrypted = cipher.encrypt_vec(&plaintext);
        let enctok = EncToken {
            token_data: encrypted,
            iv: iv,
        };

        // encrypt key with pubkey
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_symkey = p
            .pubkey
            .encrypt(&mut self.rng, padding, &key[..])
            .expect("failed to encrypt");

        // save
        p.enc_privkey_tokens.push(EncPrivKeyToken {
            enc_key: enc_symkey,
            enc_token: enctok,
        });
    }

    pub fn insert_user_data_token(&mut self, token: &mut Token) {
        assert!(token.uid != 0);
        token.is_global = false;
        let did = token.did;
        let uid = token.uid;
        warn!(
            "inserting user token {:?} with uid {} did {}",
            token, uid, did
        );
        let p = self
            .principal_data
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

                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let enc_symkey = p
                    .pubkey
                    .encrypt(&mut self.rng, padding, &key[..])
                    .expect("failed to encrypt");

                // insert key into enc symkeys map
                let cap = self.rng.next_u64();
                assert_eq!(
                    self.enc_token_symkeys_map.insert(
                        cap,
                        EncSymKey {
                            enc_symkey: enc_symkey,
                            uid: uid,
                            did: did,
                        }
                    ),
                    None
                );

                // temporarily save symkey for future use
                assert_eq!(
                    self.tmp_symkeys.insert(
                        (uid, did),
                        SymKey {
                            symkey: key.clone(),
                            uid: uid,
                            did: did,
                        }
                    ),
                    None
                );

                // temporarily save cap for future use
                assert_eq!(self.tmp_capabilities.insert((uid, did), cap), None);
                key
            }
        };

        let cap = match self.tmp_capabilities.get(&(uid, did)) {
            Some(c) => c,
            // otherwise generate it (and save it temporarily)
            None => unimplemented!("Capability should have been generated with symkey?"),
        };

        // give the token a random nonce and some id
        token.nonce = self.rng.next_u64();
        token.token_id = self.rng.next_u64();

        // encrypt and add the token to the map of encrypted tokens corresponding to this
        // capability
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
        let enctok = EncToken {
            token_data: encrypted,
            iv: iv,
        };
        match self.data_tokens_map.get_mut(&cap) {
            Some(ts) => {
                ts.push(enctok);
            }
            None => {
                self.data_tokens_map.insert(*cap, vec![enctok]);
            }
        }
    }

    /*
     * GLOBAL TOKEN FUNCTIONS
     */
    pub fn check_global_token_for_match(&mut self, token: &Token) -> (bool, bool) {
        if let Some(global_tokens) = self.global_tokens.get(&(token.did, token.uid)) {
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
        if let Some(global_tokens) = self.global_tokens.get(&(token.did, token.uid)) {
            let mut tokens = global_tokens.write().unwrap();
            // just insert the token to replace the old one
            tokens.remove(&token);
            found = true;
        }
        // log token for disguise that marks removal
        self.insert_user_data_token(&mut Token::new_token_remove(uid, did, token));
        return found;
    }

    pub fn update_global_token_from_old_to(
        &mut self,
        old_token: &Token,
        new_token: &Token,
        record_token_for_disguise: Option<(UID, DID)>,
    ) -> bool {
        assert!(new_token.is_global);
        let mut found = false;
        if let Some(global_tokens) = self.global_tokens.get(&(new_token.did, new_token.uid)) {
            let mut tokens = global_tokens.write().unwrap();
            // just insert the token to replace the old one
            tokens.insert(new_token.clone());
            found = true;
        }
        if let Some((uid, did)) = record_token_for_disguise {
            self.insert_user_data_token(&mut Token::new_token_modify(
                uid, did, old_token, new_token,
            ));
        }
        found
    }

    /*
     * UPDATE TOKEN FUNCTIONS
     */
    pub fn mark_token_revealed(&mut self, token: &Token) -> bool {
        let mut found = false;
        if token.is_global {
            if let Some(global_tokens) = self.global_tokens.get(&(token.did, token.uid)) {
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
        let cap = match self.tmp_capabilities.get(&(token.uid, token.did)) {
            Some(c) => c,
            None => unimplemented!("Token to update inaccessible!"),
        };

        // iterate through user's encrypted datatokens
        if let Some(tokenls) = self.data_tokens_map.get_mut(&cap) {
            for (i, enc_token) in tokenls.iter_mut().enumerate() {
                // decrypt token with symkey provided by client
                warn!(
                    "Got cd data of len {} with symkey {:?}-{:?}",
                    enc_token.token_data.len(),
                    symkey.symkey,
                    enc_token.iv
                );
                let cipher = Aes128Cbc::new_from_slices(&symkey.symkey, &enc_token.iv).unwrap();
                let plaintext = cipher.decrypt_vec(&mut enc_token.token_data).unwrap();
                let mut t = token_from_bytes(plaintext);
                if t.token_id == token.token_id {
                    t.revealed = true;
                    // XXX do we need a new IV?
                    let cipher = Aes128Cbc::new_from_slices(&symkey.symkey, &enc_token.iv).unwrap();
                    let plaintext = serialize_to_bytes(&t);
                    let encrypted = cipher.encrypt_vec(&plaintext);
                    let iv = enc_token.iv.clone();
                    assert_eq!(encrypted.len() % 16, 0);
                    // replace token with updated token
                    tokenls[i] = EncToken {
                        token_data: encrypted,
                        iv: iv,
                    };
                    warn!(
                        "token uid {} disguise {} revealed token {}",
                        token.uid, token.did, token.token_id,
                    );
                    found = true;
                    break;
                }
            }
        }
        found
    }

    /*
     * GET TOKEN FUNCTIONS
     */
    pub fn get_global_tokens(&self, uid: UID, did: DID) -> Vec<Token> {
        if let Some(global_tokens) = self.global_tokens.get(&(did, uid)) {
            let tokens = global_tokens.read().unwrap();
            return tokens.clone().into_iter().filter(|t| !t.revealed).collect();
        }
        vec![]
    }

    pub fn get_tokens(
        &mut self,
        symkeys: &Vec<(SymKey, Capability)>,
        for_disguise: bool,
    ) -> Vec<Token> {
        let mut tokens = vec![];

        for (symkey, cap) in symkeys {
            if for_disguise {
                // save symkeys for later in the disguise
                self.tmp_symkeys
                    .insert((symkey.uid, symkey.did), symkey.clone());
                self.tmp_capabilities.insert((symkey.uid, symkey.did), *cap);
            }

            // get all of this user's globally accessible tokens
            tokens.append(&mut self.get_global_tokens(symkey.uid, symkey.did));
            warn!("cd tokens global pushed to len {}", tokens.len());

            // get all of this user's encrypted correlation/datatokens
            if let Some(tokenls) = self.data_tokens_map.get_mut(&cap) {
                for enc_token in tokenls {
                    // decrypt token with symkey provided by client
                    warn!(
                        "Got cd data of len {} with symkey {:?}-{:?}",
                        enc_token.token_data.len(),
                        symkey.symkey,
                        enc_token.iv
                    );
                    let cipher = Aes128Cbc::new_from_slices(&symkey.symkey, &enc_token.iv).unwrap();
                    let plaintext = cipher.decrypt_vec(&mut enc_token.token_data).unwrap();
                    let token = token_from_bytes(plaintext);

                    // add token to list only if it hasn't be revealed before
                    if !token.revealed {
                        tokens.push(token.clone());
                    }
                    warn!(
                        "tokens uid {} disguise {} pushed to len {}",
                        token.uid,
                        token.did,
                        tokens.len()
                    );
                }
            }
        }
        tokens
    }

    /*
     * GET ENC SYMKEYS FUNCTIONS
     */
    pub fn get_pseudouid_enc_token_symkeys(&self, puid: UID) -> Vec<(EncSymKey, Capability)> {
        let p = self
            .principal_data
            .get(&puid)
            .expect("no user with uid found?");
        assert!(p.email.is_empty());

        let mut esks = vec![];
        for c in &p.capabilities {
            match self.enc_token_symkeys_map.get(&c) {
                Some(esk) => esks.push((esk.clone(), *c)),
                None => (),
            }
        }
        esks
    }

    pub fn get_enc_symkey(&self, cap: Capability) -> Option<EncSymKey> {
        if let Some(esk) = self.enc_token_symkeys_map.get(&cap) {
            return Some(esk.clone());
        }
        None
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
        ctrler.register_principal(uid, "email@mail.com".to_string(), &pub_key);

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
        assert_eq!(ctrler.global_tokens.len(), 1);
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
        ctrler.register_principal(uid, "email@mail.com".to_string(), &pub_key);

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
        ctrler.insert_user_data_token(&mut decor_token);
        let c = ctrler.get_tmp_capability(uid, did).unwrap().clone();
        ctrler.clear_tmp();
        assert_eq!(ctrler.global_tokens.len(), 0);

        // check principal data
        let p = ctrler
            .principal_data
            .get(&uid)
            .expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert!(p.enc_privkey_tokens.is_empty());
        assert!(p.capabilities.is_empty());
        assert!(ctrler.tmp_symkeys.is_empty());
        assert!(ctrler.tmp_capabilities.is_empty());

        // check symkey stored for principal lists
        let encsymkey = ctrler
            .enc_token_symkeys_map
            .get(&c)
            .expect("failed to get disguise?");
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey = private_key
            .decrypt(padding, &encsymkey.enc_symkey)
            .expect("failed to decrypt");
        let keys = vec![(
            SymKey {
                uid: encsymkey.uid,
                did: encsymkey.did,
                symkey: symkey,
            },
            c,
        )];

        // get tokens
        let tokens = ctrler.get_tokens(&keys, true);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], decor_token);
    }

    #[test]
    fn test_insert_user_data_token_multi() {
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
        let mut caps = HashMap::new();
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
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
                    ctrler.insert_user_data_token(&mut decor_token);
                }
                let c = ctrler.get_tmp_capability(u, d).unwrap().clone();
                caps.insert((u, d), c);
            }
        }
        assert_eq!(ctrler.global_tokens.len(), 0);
        ctrler.clear_tmp();
        assert!(ctrler.tmp_symkeys.is_empty());
        assert!(ctrler.tmp_capabilities.is_empty());

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert!(p.enc_privkey_tokens.is_empty());
            assert!(p.capabilities.is_empty());

            for d in 1..iters {
                let c = caps.get(&(u, d)).unwrap().clone();
                let encsymkey = ctrler
                    .enc_token_symkeys_map
                    .get(&c)
                    .expect("failed to get disguise?");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let symkey = priv_keys[u as usize - 1]
                    .decrypt(padding, &encsymkey.enc_symkey)
                    .expect("failed to decrypt");
                let keys = vec![(
                    SymKey {
                        uid: encsymkey.uid,
                        did: encsymkey.did,
                        symkey: symkey,
                    },
                    c,
                )];
                // get tokens
                let tokens = ctrler.get_tokens(&keys, true);
                assert_eq!(tokens.len(), (iters as usize));
                for i in 0..iters {
                    assert_eq!(
                        tokens[i as usize].old_value[0].value,
                        (old_fk_value + i as u64).to_string()
                    );
                    assert_eq!(
                        tokens[i as usize].new_value[0].value,
                        (new_fk_value + i as u64).to_string()
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
        let mut caps = HashMap::new();
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
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
                ctrler.insert_user_data_token(&mut decor_token);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some token for the anon user
                ctrler.register_anon_principal(u, anon_uid, d);
                let c = ctrler.get_tmp_capability(u, d).unwrap().clone();
                caps.insert((u, d), c);
            }
        }
        assert_eq!(ctrler.global_tokens.len(), 0);
        ctrler.clear_tmp();

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert_eq!(p.enc_privkey_tokens.len() as u64, iters - 1);

            for d in 1..iters {
                let c = caps.get(&(u, d)).unwrap().clone();
                let encsymkey = ctrler
                    .enc_token_symkeys_map
                    .get(&c)
                    .expect("failed to get disguise?");
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let symkey = priv_keys[u as usize - 1]
                    .decrypt(padding, &encsymkey.enc_symkey)
                    .expect("failed to decrypt");
                let keys = vec![(
                    SymKey {
                        uid: encsymkey.uid,
                        did: encsymkey.did,
                        symkey: symkey,
                    },
                    c,
                )];
                // get tokens
                let tokens = ctrler.get_tokens(&keys, true);
                assert_eq!(tokens.len(), 1);
                assert_eq!(
                    tokens[0].old_value[0].value,
                    (old_fk_value + d).to_string()
                );
                assert_eq!(
                    tokens[0].new_value[0].value,
                    (new_fk_value + d).to_string()
                );
            }
        }
    }
}
