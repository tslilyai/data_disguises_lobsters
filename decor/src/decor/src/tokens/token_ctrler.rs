use crate::helpers::*;
use crate::tokens::*;
use crate::{DID, UID};
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use log::warn;
use rand::{rngs::OsRng, RngCore};
use rsa::pkcs1::{FromRsaPrivateKey, ToRsaPrivateKey};
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::iter::repeat;
use std::sync::{Arc, RwLock};

pub type LocCap = u64;
pub type DataCap = Vec<u8>; // private key
const RSA_BITS: usize = 2048;
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncData {
    pub enc_key: Vec<u8>,
    pub enc_data: Vec<u8>,
    pub iv: Vec<u8>,
}

impl EncData {
    pub fn decrypt_encdata(&self, data_cap: &DataCap) -> (Vec<u8>, Vec<u8>) {
        if data_cap.is_empty() {
            return (vec![], vec![]);
        }

        let priv_key = RsaPrivateKey::from_pkcs1_der(data_cap).unwrap();
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let key = priv_key
            .decrypt(padding, &self.enc_key)
            .expect("failed to decrypt");
        let cipher = Aes128Cbc::new_from_slices(&key, &self.iv).unwrap();
        let mut edata = self.enc_data.clone();
        let plaintext = cipher.decrypt_vec(&mut edata).unwrap();
        (key.to_vec(), plaintext)
    }
}

#[derive(Clone)]
pub struct PrincipalData {
    pubkey: RsaPublicKey,
    email: String,

    // only nonempty for pseudoprincipals!
    ownership_loc_caps: Vec<LocCap>,
    diff_loc_caps: Vec<LocCap>,
}

#[derive(Clone)]
pub struct TokenCtrler {
    // principal tokens are stored indexed by some large random num
    pub principal_data: HashMap<UID, PrincipalData>,

    // (p,d) capability -> set of token ciphertext for principal+disguise
    pub enc_diffs_map: HashMap<LocCap, Vec<EncData>>,

    // (p,d) capability -> set of token ciphertext for principal+disguise
    pub enc_ownership_map: HashMap<LocCap, Vec<EncData>>,

    pub global_diff_tokens: HashMap<DID, HashMap<UID, Arc<RwLock<HashSet<DiffToken>>>>>,

    // used for randomness stuff
    pub rng: OsRng,
    pub hasher: Sha256,

    // used to temporarily store keys used during disguises
    // TODO have separate caps for pseudoprincipals?
    pub tmp_ownership_loc_caps: HashMap<(UID, DID), LocCap>,

    pub tmp_diff_loc_caps: HashMap<(UID, DID), LocCap>,
}

impl TokenCtrler {
    pub fn new() -> TokenCtrler {
        TokenCtrler {
            principal_data: HashMap::new(),
            enc_diffs_map: HashMap::new(),
            enc_ownership_map: HashMap::new(),
            global_diff_tokens: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_ownership_loc_caps: HashMap::new(),
            tmp_diff_loc_caps: HashMap::new(),
        }
    }

    /*
     * LOCATING CAPABILITIES
     */
    pub fn get_tmp_reveal_capability(&self, uid: UID, did: DID) -> Option<&LocCap> {
        self.tmp_diff_loc_caps.get(&(uid, did))
    }

    pub fn get_tmp_ownership_capability(&self, uid: UID, did: DID) -> Option<&LocCap> {
        self.tmp_diff_loc_caps.get(&(uid, did))
    }

    pub fn save_and_clear_loc_caps(&mut self) -> (HashMap<(UID, DID), LocCap>, HashMap<(UID, DID), LocCap>) {
        let dlcs = self.tmp_diff_loc_caps.clone();
        let olcs = self.tmp_ownership_loc_caps.clone();
        for ((uid, _), c) in dlcs.iter() {
            let p = self.principal_data.get_mut(&uid).unwrap();
            // save to principal data if no email (pseudoprincipal)
            if p.email.is_empty() {
                p.diff_loc_caps.push(*c);
            } else {
                // TODO email capability to user if user has email
                //self.loc_caps.insert((*uid, *did), *c);
            }
        }
        for ((uid, _), c) in olcs.iter() {
            let p = self.principal_data.get_mut(&uid).unwrap();
            // save to principal data if no email (pseudoprincipal)
            if p.email.is_empty() {
                p.ownership_loc_caps.push(*c);
            } else {
                // TODO email capability to user if user has email
                //self.loc_caps.insert((*uid, *did), *c);
            }
        }
        self.clear_tmp();
        (dlcs, olcs)
    }

    // XXX note this doesn't allow for concurrent disguising right now
    pub fn clear_tmp(&mut self) {
        self.tmp_diff_loc_caps.clear();
        self.tmp_ownership_loc_caps.clear();
    }

    fn get_ownership_loc_cap(&mut self, uid: u64, did: u64) -> LocCap {
        // get the location capability being used for this disguise
        match self.tmp_ownership_loc_caps.get(&(uid, did)) {
            // if there's a loccap already, use it
            Some(lc) => return *lc,
            // otherwise generate it (and save it temporarily)
            None => {
                let cap = self.rng.next_u64();
                // temporarily save cap for future use
                assert_eq!(self.tmp_ownership_loc_caps.insert((uid, did), cap), None);
                return cap;
            }
        }
    }

    fn get_diff_loc_cap(&mut self, uid: u64, did: u64) -> LocCap {
        // get the location capability being used for this disguise
        match self.tmp_diff_loc_caps.get(&(uid, did)) {
            // if there's a loccap already, use it
            Some(lc) => return *lc,
            // otherwise generate it (and save it temporarily)
            None => {
                let cap = self.rng.next_u64();
                // temporarily save cap for future use
                assert_eq!(self.tmp_diff_loc_caps.insert((uid, did), cap), None);
                return cap;
            }
        }
    }

    /*
     * REGISTRATION
     */
    pub fn register_principal(&mut self, uid: UID, email: String, pubkey: &RsaPublicKey) {
        warn!("Registering principal {}", uid);
        self.principal_data.insert(
            uid,
            PrincipalData {
                pubkey: pubkey.clone(),
                email: email,
                ownership_loc_caps: vec![],
                diff_loc_caps: vec![],
            },
        );
    }

    pub fn register_anon_principal(
        &mut self,
        uid: UID,
        anon_uid: UID,
        did: DID,
        child_name: String,
        child_ids: Vec<RowVal>,
        pprincipal_name: String,
        pprincipal_id_col: String,
        fk_col: String,
    ) -> UID {
        let private_key =
            RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);

        // save the anon principal as a new principal with a public key
        // and initially empty token vaults
        self.register_principal(anon_uid, String::new(), &pub_key);
        let mut pppk: OwnershipToken = new_ownership_token(
            did,
            child_name,
            child_ids,
            pprincipal_name,
            pprincipal_id_col,
            fk_col,
            uid,
            anon_uid,
            &private_key,
        );
        self.insert_ownership_token(&mut pppk);
        anon_uid
    }

    pub fn remove_anon_principal(&mut self, anon_uid: UID) {
        warn!("Removing principal {}\n", anon_uid);
        self.principal_data.remove(&anon_uid);
    }

    /*
     * PRINCIPAL TOKEN INSERT
     */
    fn insert_ownership_token(&mut self, pppk: &mut OwnershipToken) {
        assert!(pppk.uid != 0);

        // give token a unique id
        pppk.token_id = self.rng.next_u64();

        let p = self
            .principal_data
            .get_mut(&pppk.uid)
            .expect("no user with uid found?");

        // generate key
        let mut key: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut key[..]);

        // encrypt key with pubkey
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_key = p
            .pubkey
            .encrypt(&mut self.rng, padding, &key[..])
            .expect("failed to encrypt");

        // encrypt pppk with key
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
        let plaintext = serialize_to_bytes(&pppk);
        let encrypted = cipher.encrypt_vec(&plaintext);
        let enc_pppk = EncData {
            enc_key: enc_key,
            enc_data: encrypted,
            iv: iv,
        };

        // insert the encrypted pppk into locating capability
        let lc = self.get_ownership_loc_cap(pppk.uid, pppk.did);
        match self.enc_ownership_map.get_mut(&lc) {
            Some(ts) => {
                ts.push(enc_pppk);
            }
            None => {
                self.enc_ownership_map.insert(lc, vec![enc_pppk]);
            }
        }
    }

    pub fn insert_user_diff_token(&mut self, token: &mut DiffToken) {
        assert!(token.uid != 0);
        token.is_global = false;
        let did = token.did;
        let uid = token.uid;
        warn!(
            "inserting user token {:?} with uid {} did {}",
            token, uid, did
        );

        let cap = self.get_diff_loc_cap(uid, did);

        let p = self
            .principal_data
            .get_mut(&uid)
            .expect("no user with uid found?");

        // give the token a random nonce and some id
        token.nonce = self.rng.next_u64();
        token.token_id = self.rng.next_u64();

        // generate key
        let mut key: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut key[..]);

        // encrypt key with pubkey
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_key = p
            .pubkey
            .encrypt(&mut self.rng, padding, &key[..])
            .expect("failed to encrypt");

        // encrypt and add the token to the map of encrypted tokens
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
        let plaintext = serialize_to_bytes(&token);
        let encrypted = cipher.encrypt_vec(&plaintext);
        assert_eq!(encrypted.len() % 16, 0);
        let enctoken = EncData {
            enc_key: enc_key,
            enc_data: encrypted,
            iv: iv,
        };
        match self.enc_diffs_map.get_mut(&cap) {
            Some(ts) => {
                ts.push(enctoken);
            }
            None => {
                self.enc_diffs_map.insert(cap, vec![enctoken]);
            }
        }
    }

    /*
     * GLOBAL TOKEN FUNCTIONS
     */
    pub fn insert_global_diff_token(&mut self, token: &mut DiffToken) {
        token.is_global = true;
        token.token_id = self.rng.next_u64();
        warn!(
            "Inserting global token disguise {} user {}",
            token.did, token.uid
        );
        if let Some(hm) = self.global_diff_tokens.get_mut(&token.did) {
            if let Some(user_disguise_tokens) = hm.get_mut(&token.uid) {
                let mut tokens = user_disguise_tokens.write().unwrap();
                tokens.insert(token.clone());
            } else {
                let mut hs = HashSet::new();
                hs.insert(token.clone());
                hm.insert(token.uid, Arc::new(RwLock::new(hs)));
            }
        } else {
            let mut user_hm = HashMap::new();
            let mut hs = HashSet::new();
            hs.insert(token.clone());
            user_hm.insert(token.uid, Arc::new(RwLock::new(hs)));
            self.global_diff_tokens.insert(token.did, user_hm);
        }
    }

    pub fn check_global_diff_token_for_match(&mut self, token: &DiffToken) -> (bool, bool) {
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&token.did) {
            if let Some(user_tokens) = global_diff_tokens.get(&token.uid) {
                let tokens = user_tokens.read().unwrap();
                for t in tokens.iter() {
                    if t.token_id == token.token_id {
                        // XXX todo this is a bit inefficient
                        let mut mytoken = token.clone();
                        mytoken.revealed = t.revealed;
                        let eq = mytoken == *t;
                        if t.revealed {
                            return (true, eq);
                        }
                        return (false, eq);
                    }
                }
            }
        }
        return (false, false);
    }

    pub fn remove_global_diff_token(&mut self, uid: UID, did: DID, token: &DiffToken) -> bool {
        assert!(token.is_global);
        assert!(uid != 0);
        let mut found = false;

        // delete token
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&token.did) {
            if let Some(user_tokens) = global_diff_tokens.get(&token.uid) {
                let mut tokens = user_tokens.write().unwrap();
                tokens.remove(&token);
                found = true;
            }
        }
        // log token for disguise that marks removal
        self.insert_user_diff_token(&mut DiffToken::new_token_remove(uid, did, token));
        return found;
    }

    pub fn update_global_diff_token_from_old_to(
        &mut self,
        old_token: &DiffToken,
        new_token: &DiffToken,
        record_token_for_disguise: Option<(UID, DID)>,
    ) -> bool {
        assert!(new_token.is_global);
        let mut found = false;
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&old_token.did) {
            if let Some(user_tokens) = global_diff_tokens.get(&old_token.uid) {
                let mut tokens = user_tokens.write().unwrap();
                // just insert the token to replace the old one
                tokens.insert(new_token.clone());
                found = true;
            }
        }
        if let Some((uid, did)) = record_token_for_disguise {
            self.insert_user_diff_token(&mut DiffToken::new_token_modify(
                uid, did, old_token, new_token,
            ));
        }
        found
    }

    /*
     * UPDATE TOKEN FUNCTIONS
     */
    pub fn mark_diff_token_revealed(
        &mut self,
        did: DID,
        token: &DiffToken,
        data_cap: &DataCap,
        diff_loc_caps: &Vec<LocCap>,
        ownership_loc_caps: &Vec<LocCap>,
    ) -> bool {
        let mut found = false;
        if token.is_global {
            if let Some(global_diff_tokens) = self.global_diff_tokens.get(&token.did) {
                if let Some(user_tokens) = global_diff_tokens.get(&token.uid) {
                    let mut tokens = user_tokens.write().unwrap();
                    let mut t = token.clone();
                    t.revealed = true;
                    tokens.insert(t);
                    found = true;
                }
                // just return if the disguise was global
            }
            return found;
        }

        // return if no tokens accessible
        if data_cap.is_empty() {
            return false;
        }

        // iterate through user's encrypted datatokens
        'lcloop: for lc in diff_loc_caps {
            if let Some(tokenls) = self.enc_diffs_map.get_mut(&lc) {
                for (i, enc_token) in tokenls.iter_mut().enumerate() {
                    // decrypt data and compare
                    let (key, tokenplaintext) = enc_token.decrypt_encdata(data_cap);
                    let mut curtoken = diff_token_from_bytes(&tokenplaintext);

                    if curtoken.did != did {
                        continue 'lcloop;
                    }

                    if curtoken.token_id == token.token_id {
                        curtoken.revealed = true;
                        let cipher = Aes128Cbc::new_from_slices(&key, &enc_token.iv).unwrap();
                        let plaintext = serialize_to_bytes(&curtoken);
                        let encrypted = cipher.encrypt_vec(&plaintext);
                        let iv = enc_token.iv.clone();
                        assert_eq!(encrypted.len() % 16, 0);
                        // replace token with updated token
                        tokenls[i] = EncData {
                            enc_key: enc_token.enc_key.clone(),
                            enc_data: encrypted,
                            iv: iv,
                        };
                        warn!(
                            "token uid {} disguise {} revealed token {}",
                            token.uid, token.did, token.token_id,
                        );
                        return true;
                    }
                }
            }
        }

        // iterate through allowed pseudoprincipals' tokens as well
        for lc in ownership_loc_caps {
            if let Some(pks) = self.enc_ownership_map.get(&lc) {
                for enc_pk in &pks.clone() {
                    // decrypt with data_cap provided by client
                    let (_, plaintext) = enc_pk.decrypt_encdata(data_cap);
                    let pk = ownership_token_from_bytes(&plaintext);

                    // get all tokens of pseudoprincipal
                    if let Some(pp) = self.principal_data.get(&pk.new_uid) {
                        let pp = pp.clone();
                        if self.mark_diff_token_revealed(
                            did,
                            token,
                            &pk.priv_key,
                            &pp.diff_loc_caps,
                            &pp.ownership_loc_caps,
                        ) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    pub fn mark_ownership_token_revealed(
        &mut self,
        did: DID,
        token: &OwnershipToken,
        data_cap: &DataCap,
        ownership_loc_caps: &Vec<LocCap>,
    ) -> bool {
        // return if no tokens accessible
        if data_cap.is_empty() {
            return false;
        }

        // iterate through my ownership tokens
        for lc in ownership_loc_caps {
            if let Some(pks) = self.enc_ownership_map.get_mut(&lc) {
                for (i, enc_token) in pks.iter_mut().enumerate() {
                    // decrypt with data_cap provided by client
                    let (key, plaintext) = enc_token.decrypt_encdata(data_cap);
                    let mut curtoken = ownership_token_from_bytes(&plaintext);
                    if curtoken.token_id == token.token_id {
                        curtoken.revealed = true;
                        let cipher = Aes128Cbc::new_from_slices(&key, &enc_token.iv).unwrap();
                        let plaintext = serialize_to_bytes(&curtoken);
                        let encrypted = cipher.encrypt_vec(&plaintext);
                        let iv = enc_token.iv.clone();
                        assert_eq!(encrypted.len() % 16, 0);
                        // replace token with updated token
                        pks[i] = EncData {
                            enc_key: enc_token.enc_key.clone(),
                            enc_data: encrypted,
                            iv: iv,
                        };
                        warn!(
                            "token uid {} disguise {} revealed token {}",
                            token.uid, token.did, token.token_id,
                        );
                        return true;
                    }
                }
            }
        }

        // iterate through allowed pseudoprincipals' tokens as well
        for lc in ownership_loc_caps {
            if let Some(pks) = self.enc_ownership_map.get(&lc) {
                for enc_pk in &pks.clone() {
                    // decrypt with data_cap provided by client
                    let (_, plaintext) = enc_pk.decrypt_encdata(data_cap);
                    let pk = ownership_token_from_bytes(&plaintext);

                    // get all tokens of pseudoprincipal
                    if let Some(pp) = self.principal_data.get(&pk.new_uid) {
                        let pp = pp.clone();
                        if self.mark_ownership_token_revealed(
                            did,
                            token,
                            &pk.priv_key,
                            &pp.ownership_loc_caps,
                        ) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /*
     * GET TOKEN FUNCTIONS
     */
    pub fn get_all_global_diff_tokens(&self) -> Vec<DiffToken> {
        let mut tokens = vec![];
        for (_, global_diff_tokens) in self.global_diff_tokens.iter() {
            for (_, user_tokens) in global_diff_tokens.iter() {
                let utokens = user_tokens.read().unwrap();
                let nonrev_tokens: Vec<DiffToken> = utokens
                    .clone()
                    .into_iter()
                    .filter(|t| !t.revealed)
                    .collect();
                for d in nonrev_tokens {
                    tokens.push(d.clone());
                }
            }
        }
        tokens
    }

    pub fn get_global_diff_tokens_of_disguise(&self, did: DID) -> Vec<DiffToken> {
        let mut tokens = vec![];
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&did) {
            for (_, user_tokens) in global_diff_tokens.iter() {
                let utokens = user_tokens.read().unwrap();
                let nonrev_tokens: Vec<DiffToken> = utokens
                    .clone()
                    .into_iter()
                    .filter(|t| !t.revealed)
                    .collect();
                for d in nonrev_tokens {
                    tokens.push(d.clone());
                }
            }
        }
        warn!("Got {} global diff tokens for disguise {}\n", tokens.len(), did);
        tokens
    }

    pub fn get_global_diff_tokens(&self, uid: UID, did: DID) -> Vec<DiffToken> {
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&did) {
            if let Some(user_tokens) = global_diff_tokens.get(&uid) {
                let tokens = user_tokens.read().unwrap();
                warn!(
                    "Filtering {} global tokens of disg {} user {}",
                    tokens.len(),
                    did,
                    uid
                );
                return tokens.clone().into_iter().filter(|t| !t.revealed).collect();
            }
        }
        vec![]
    }

    pub fn get_user_tokens(
        &self,
        did: DID,
        data_cap: &DataCap,
        diff_loc_caps: &Vec<LocCap>,
        ownership_loc_caps: &Vec<LocCap>,
    ) -> (Vec<DiffToken>, Vec<OwnershipToken>) {
        let mut diff_tokens = vec![];
        let mut own_tokens = vec![];
        if data_cap.is_empty() {
            return (diff_tokens, own_tokens);
        }
        for loc_cap in diff_loc_caps {
            if let Some(tokenls) = self.enc_diffs_map.get(&loc_cap) {
                warn!("Getting tokens of user from ls len {}", tokenls.len());
                for enc_token in tokenls {
                    // decrypt token with data_cap provided by client
                    let (_, plaintext) = enc_token.decrypt_encdata(data_cap);
                    let token = diff_token_from_bytes(&plaintext);

                    // add token to list only if it hasn't be revealed before
                    if !token.revealed && token.did == did {
                        diff_tokens.push(token.clone());
                    }
                    warn!(
                        "tokens uid {} disguise {} pushed to len {}",
                        token.uid,
                        token.did,
                        diff_tokens.len()
                    );
                }
            }
        }
        // get allowed pseudoprincipal diff tokens for all owned pseudoprincipals
        for lc in ownership_loc_caps {
            if let Some(pks) = self.enc_ownership_map.get(&lc) {
                for enc_pk in &pks.clone() {
                    // decrypt with data_cap provided by client
                    let (_, plaintext) = enc_pk.decrypt_encdata(data_cap);
                    let pk = ownership_token_from_bytes(&plaintext);
                    own_tokens.push(pk.clone());

                    // get all tokens of pseudoprincipal
                    warn!("Getting tokens of pseudoprincipal {}", pk.new_uid);
                    if let Some(pp) = self.principal_data.get(&pk.new_uid) {
                        let (pp_diff_tokens, pp_own_tokens) = self.get_user_tokens(
                            did,
                            &pk.priv_key,
                            &pp.diff_loc_caps,
                            &pp.ownership_loc_caps,
                        );
                        diff_tokens.extend(pp_diff_tokens.iter().cloned());
                        own_tokens.extend(pp_own_tokens.iter().cloned());
                    }
                }
            }
        }
        (diff_tokens, own_tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_logger() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::Warn)
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    #[test]
    fn test_insert_global_diff_token_single() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let did = 1;
        let uid = 11;
        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.register_principal(uid, "email@mail.com".to_string(), &pub_key);

        let mut remove_token = DiffToken::new_delete_token(
            did,
            guise_name,
            guise_ids,
            vec![RowVal {
                column: fk_col.clone(),
                value: old_fk_value.to_string(),
            }],
        );
        remove_token.uid = uid;
        ctrler.insert_global_diff_token(&mut remove_token);
        assert_eq!(ctrler.global_diff_tokens.len(), 1);
        let tokens = ctrler.get_global_diff_tokens(uid, did);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], remove_token);
    }

    #[test]
    fn test_insert_user_token_single() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let did = 1;
        let uid = 11;
        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.register_principal(uid, "email@mail.com".to_string(), &pub_key);

        let mut remove_token = DiffToken::new_delete_token(
            did,
            guise_name,
            guise_ids,
            vec![RowVal {
                column: fk_col.clone(),
                value: old_fk_value.to_string(),
            }],
        );
        remove_token.uid = uid;
        ctrler.insert_user_diff_token(&mut remove_token);
        let lc = ctrler.get_tmp_reveal_capability(uid, did).unwrap().clone();
        ctrler.clear_tmp();
        assert_eq!(ctrler.global_diff_tokens.len(), 0);

        // check principal data
        let p = ctrler
            .principal_data
            .get(&uid)
            .expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert!(p.ownership_loc_caps.is_empty());
        assert!(p.diff_loc_caps.is_empty());
        assert!(ctrler.tmp_diff_loc_caps.is_empty());

        // get tokens
        let (diff_tokens, _) = ctrler.get_user_tokens(did, &private_key_vec, &vec![lc], &vec![]);
        assert_eq!(diff_tokens.len(), 1);
        assert_eq!(diff_tokens[0], remove_token);
    }

    #[test]
    fn test_insert_user_diff_token_multi() {
        init_logger();
        let mut ctrler = TokenCtrler::new();

        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let mut priv_keys = vec![];
        let mut pub_keys = vec![];

        let iters = 5;
        let mut caps = HashMap::new();
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                for i in 0..iters {
                    let mut remove_token = DiffToken::new_delete_token(
                        d,
                        guise_name.clone(),
                        guise_ids.clone(),
                        vec![RowVal {
                            column: fk_col.clone(),
                            value: (old_fk_value + i).to_string(),
                        }],
                    );
                    remove_token.uid = u;
                    ctrler.insert_user_diff_token(&mut remove_token);
                }
                let c = ctrler.get_tmp_reveal_capability(u, d).unwrap().clone();
                caps.insert((u, d), c);
            }
        }
        assert_eq!(ctrler.global_diff_tokens.len(), 0);
        ctrler.clear_tmp();
        assert!(ctrler.tmp_diff_loc_caps.is_empty());

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert!(p.ownership_loc_caps.is_empty());
            assert!(p.diff_loc_caps.is_empty());

            for d in 1..iters {
                let lc = caps.get(&(u, d)).unwrap().clone();
                // get tokens
                let (diff_tokens, _) = ctrler.get_user_tokens(d, &priv_keys[u as usize - 1], &vec![lc], &vec![]);
                assert_eq!(diff_tokens.len(), (iters as usize));
                for i in 0..iters {
                    assert_eq!(
                        diff_tokens[i as usize].old_value[0].value,
                        (old_fk_value + i as u64).to_string()
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
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let mut priv_keys = vec![];
        let mut pub_keys = vec![];

        let iters = 5;
        let mut caps = HashMap::new();
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                let mut remove_token = DiffToken::new_delete_token(
                    d,
                    guise_name.clone(),
                    guise_ids.clone(),
                    vec![RowVal {
                        column: fk_col.clone(),
                        value: (old_fk_value + d).to_string(),
                    }],
                );
                remove_token.uid = u;
                ctrler.insert_user_diff_token(&mut remove_token);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some token for the anon user
                //&mut self,
                ctrler.register_anon_principal(
                    u,
                    anon_uid,
                    d,
                    guise_name.clone(),
                    guise_ids.clone(),
                    referenced_name.clone(),
                    fk_col.clone(),
                    fk_col.clone(),
                );
                let lc = ctrler.get_tmp_reveal_capability(u, d).unwrap().clone();
                caps.insert((u, d), lc);
            }
        }
        assert_eq!(ctrler.global_diff_tokens.len(), 0);
        ctrler.clear_tmp();

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);

            for d in 1..iters {
                let c = caps.get(&(u, d)).unwrap().clone();
                // get tokens
                let (diff_tokens, own_tokens) =
                    ctrler.get_user_tokens(d, &priv_keys[u as usize - 1], &vec![c], &vec![]);
                assert_eq!(diff_tokens.len(), 1);
                assert_eq!(own_tokens.len(), 0);
                assert_eq!(
                    diff_tokens[0].old_value[0].value,
                    (old_fk_value + d).to_string()
                );
            }
        }
    }
}
