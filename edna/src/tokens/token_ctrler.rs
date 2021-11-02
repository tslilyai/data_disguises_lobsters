use crate::helpers::*;
use crate::stats::QueryStat;
use crate::tokens::*;
use crate::{DID, UID};
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use log::warn;
use mysql::prelude::*;
use rand::{rngs::OsRng, RngCore};
use rsa::pkcs1::{FromRsaPrivateKey, FromRsaPublicKey, ToRsaPublicKey};
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::iter::repeat;
use std::sync::{Arc, Mutex, RwLock};

pub type LocCap = u64;
pub type DataCap = Vec<u8>; // private key
const RSA_BITS: usize = 2048;
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

const PRINCIPAL_TABLE: &'static str = "EdnaPrincipals";
const UID_COL: &'static str = "uid";

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
    pub pubkey: RsaPublicKey,
    pub email: String,

    // only nonempty for pseudoprincipals!
    pub ownership_loc_caps: Vec<LocCap>,
    pub diff_loc_caps: Vec<LocCap>,
}

#[derive(Clone)]
pub struct TokenCtrler {
    // principal tokens are stored indexed by some large random num
    pub principal_data: HashMap<UID, PrincipalData>,

    // (p,d) capability -> set of token ciphertext for principal+disguise
    pub enc_diffs_map: HashMap<LocCap, Vec<EncData>>,

    // (p,d) capability -> set of token ciphertext for principal+disguise
    pub enc_ownership_map: HashMap<LocCap, Vec<EncData>>,

    pub global_diff_tokens: HashMap<DID, HashMap<UID, Arc<RwLock<HashSet<DiffTokenWrapper>>>>>,

    // used for randomness stuff
    pub rng: OsRng,
    pub hasher: Sha256,

    // used to temporarily store keys used during disguises
    // TODO have separate caps for pseudoprincipals?
    pub tmp_ownership_loc_caps: HashMap<(UID, DID), LocCap>,
    pub tmp_diff_loc_caps: HashMap<(UID, DID), LocCap>,
    pub tmp_remove_principals: HashSet<UID>,
}

impl TokenCtrler {
    pub fn new(conn: &mut mysql::PooledConn, stats: Arc<Mutex<QueryStat>>) -> TokenCtrler {
        let mut tctrler = TokenCtrler {
            principal_data: HashMap::new(),
            enc_diffs_map: HashMap::new(),
            enc_ownership_map: HashMap::new(),
            global_diff_tokens: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_ownership_loc_caps: HashMap::new(),
            tmp_diff_loc_caps: HashMap::new(),
            tmp_remove_principals: HashSet::new(),
        };

        let createq = format!(
            "CREATE TABLE IF NOT EXISTS {} ({} varchar(255), email varchar(255), pubkey blob, ownershipToks blob, diffToks blob, PRIMARY KEY ({}));",
            PRINCIPAL_TABLE, UID_COL, UID_COL);
        conn.query_drop(&createq).unwrap();
        let selected = get_query_rows_str(
            &format!("SELECT * FROM {}", PRINCIPAL_TABLE),
            conn,
            stats.clone(),
        )
        .unwrap();
        for row in selected {
            let pubkey_bytes: Vec<u8> = serde_json::from_str(&row[2].value).unwrap();
            let pubkey = RsaPublicKey::from_pkcs1_der(&pubkey_bytes).unwrap();
            let ownership_toks = serde_json::from_str(&row[3].value).unwrap();
            let diff_toks = serde_json::from_str(&row[4].value).unwrap();
            tctrler.register_saved_principal(
                &row[0].value,
                &row[1].value,
                &pubkey,
                ownership_toks,
                diff_toks,
            );
        }
        tctrler
    }

    /*
     * LOCATING CAPABILITIES
     */
    pub fn get_tmp_reveal_capability(&self, uid: &UID, did: DID) -> Option<&LocCap> {
        self.tmp_diff_loc_caps.get(&(uid.to_string(), did))
    }

    pub fn get_tmp_ownership_capability(&self, uid: &UID, did: DID) -> Option<&LocCap> {
        self.tmp_diff_loc_caps.get(&(uid.to_string(), did))
    }

    pub fn save_and_clear(
        &mut self,
        did: DID,
        conn: &mut mysql::PooledConn,
    ) -> (HashMap<(UID, DID), LocCap>, HashMap<(UID, DID), LocCap>) {
        let dlcs = self.tmp_diff_loc_caps.clone();
        let olcs = self.tmp_ownership_loc_caps.clone();

        for ((uid, _), c) in dlcs.iter() {
            let p = self.principal_data.get_mut(uid).unwrap();
            // save to principal data if no email (pseudoprincipal)
            if p.email.is_empty() {
                p.diff_loc_caps.push(*c);

                // update persistence
                let uidstr = uid.trim_matches('\'');
                conn.query_drop(&format!(
                    "UPDATE {} SET {} = \'{}\' WHERE {} = \'{}\'",
                    PRINCIPAL_TABLE,
                    "diffToks",
                    serde_json::to_string(&p.diff_loc_caps).unwrap(),
                    UID_COL,
                    uidstr
                ))
                .unwrap();
            }
        }

        for ((uid, _), c) in olcs.iter() {
            let p = self.principal_data.get_mut(uid).unwrap();
            // save to principal data if no email (pseudoprincipal)
            if p.email.is_empty() {
                p.ownership_loc_caps.push(*c);

                // update persistence
                let uidstr = uid.trim_matches('\'');
                conn.query_drop(&format!(
                    "UPDATE {} SET {} = \'{}\' WHERE {} = \'{}\'",
                    PRINCIPAL_TABLE,
                    "ownershipToks",
                    serde_json::to_string(&p.ownership_loc_caps).unwrap(),
                    UID_COL,
                    uidstr
                ))
                .unwrap();
            }
        }

        // actually remove the principals supposed to be removed
        for uid in self.tmp_remove_principals.clone().iter() {
            self.remove_principal(&uid, did, conn);
        }

        self.clear_tmp();
        (dlcs, olcs)
    }

    // XXX note this doesn't allow for concurrent disguising right now
    pub fn clear_tmp(&mut self) {
        self.tmp_diff_loc_caps.clear();
        self.tmp_ownership_loc_caps.clear();
        self.tmp_remove_principals.clear();
    }

    fn get_ownership_loc_cap(&mut self, uid: &UID, did: DID) -> LocCap {
        // get the location capability being used for this disguise
        match self.tmp_ownership_loc_caps.get(&(uid.clone(), did)) {
            // if there's a loccap already, use it
            Some(lc) => return *lc,
            // otherwise generate it (and save it temporarily)
            None => {
                let cap = self.rng.next_u64();
                // temporarily save cap for future use
                assert_eq!(
                    self.tmp_ownership_loc_caps.insert((uid.clone(), did), cap),
                    None
                );
                return cap;
            }
        }
    }

    fn get_diff_loc_cap(&mut self, uid: &UID, did: u64) -> LocCap {
        // get the location capability being used for this disguise
        match self.tmp_diff_loc_caps.get(&(uid.clone(), did)) {
            // if there's a loccap already, use it
            Some(lc) => return *lc,
            // otherwise generate it (and save it temporarily)
            None => {
                let cap = self.rng.next_u64();
                // temporarily save cap for future use
                assert_eq!(self.tmp_diff_loc_caps.insert((uid.clone(), did), cap), None);
                return cap;
            }
        }
    }

    /*
     * REGISTRATION
     */
    pub fn register_saved_principal(
        &mut self,
        uid: &UID,
        email: &str,
        pubkey: &RsaPublicKey,
        ot: Vec<LocCap>,
        dt: Vec<LocCap>,
    ) {
        warn!("Re-registering saved principal {}", uid);
        let pdata = PrincipalData {
            pubkey: pubkey.clone(),
            email: email.to_string(),
            ownership_loc_caps: ot,
            diff_loc_caps: dt,
        };
        self.principal_data.insert(uid.clone(), pdata);
    }

    pub fn register_principal(
        &mut self,
        uid: &UID,
        email: String,
        pubkey: &RsaPublicKey,
        conn: &mut mysql::PooledConn,
    ) {
        warn!("Registering principal {}", uid);
        let pdata = PrincipalData {
            pubkey: pubkey.clone(),
            email: email.clone(),
            ownership_loc_caps: vec![],
            diff_loc_caps: vec![],
        };

        self.persist_principal(uid, &pdata, conn);
        self.principal_data.insert(uid.clone(), pdata);
    }

    pub fn register_anon_principal(
        &mut self,
        uid: &UID,
        anon_uid: &UID,
        did: DID,
        ownership_token_data: Vec<u8>,
        conn: &mut mysql::PooledConn,
    ) {
        let private_key =
            RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        let uidstr = uid.trim_matches('\'');
        let anon_uidstr = anon_uid.trim_matches('\'');

        // save the anon principal as a new principal with a public key
        // and initially empty token vaults
        self.register_principal(&anon_uidstr.to_string(), String::new(), &pub_key, conn);
        let own_token_wrapped = new_generic_ownership_token_wrapper(
            uidstr.to_string(),
            anon_uidstr.to_string(),
            did,
            ownership_token_data,
            &private_key,
        );
        self.insert_ownership_token_wrapper(&own_token_wrapped);
    }

    fn persist_principal(&self, uid: &UID, pdata: &PrincipalData, conn: &mut mysql::PooledConn) {
        let pubkey_vec = pdata.pubkey.to_pkcs1_der().unwrap().as_der().to_vec();
        let v: Vec<String> = vec![];
        let empty_vec = serde_json::to_string(&v).unwrap();
        let uid = uid.trim_matches('\'');
        let insert_q = format!(
            "INSERT INTO {} VALUES (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\');",
            PRINCIPAL_TABLE,
            uid,
            pdata.email,
            serde_json::to_string(&pubkey_vec).unwrap(),
            empty_vec,
            empty_vec
        );
        warn!("Insert q {}", insert_q);
        conn.query_drop(&insert_q).unwrap();
    }

    pub fn mark_principal_to_be_removed(&mut self, uid: &UID) {
        self.tmp_remove_principals.insert(uid.to_string());
    }

    pub fn remove_principal(&mut self, uid: &UID, did: DID, conn: &mut mysql::PooledConn) {
        warn!("Removing principal {}\n", uid);
        let pdata = self.principal_data.get(uid);
        if pdata.is_none() {
            return;
        } else {
            let pdata = pdata.unwrap();
            let mut ptoken = new_remove_principal_token_wrapper(uid, did, &pdata);
            self.insert_user_diff_token_wrapper(&mut ptoken);

            // actually remove
            self.principal_data.remove(uid);
            conn.query_drop(format!(
                "DELETE FROM {} WHERE {} = \'{}\'",
                PRINCIPAL_TABLE,
                UID_COL,
                uid.trim_matches('\'')
            ))
            .unwrap();
        }
    }

    /*
     * PRINCIPAL TOKEN INSERT
     */
    fn insert_ownership_token_wrapper(&mut self, pppk: &OwnershipTokenWrapper) {
        let p = self
            .principal_data
            .get_mut(&pppk.old_uid)
            .expect(&format!("no user with uid {} found?", pppk.old_uid));

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
        let lc = self.get_ownership_loc_cap(&pppk.old_uid, pppk.did);
        match self.enc_ownership_map.get_mut(&lc) {
            Some(ts) => {
                ts.push(enc_pppk);
            }
            None => {
                self.enc_ownership_map.insert(lc, vec![enc_pppk]);
            }
        }
    }

    pub fn insert_user_diff_token_wrapper(&mut self, token: &DiffTokenWrapper) {
        let did = token.did;
        let uid = &token.uid;
        warn!(
            "inserting user token {:?} with uid {} did {}",
            token, uid, did
        );

        let cap = self.get_diff_loc_cap(&uid, did);

        let p = self
            .principal_data
            .get_mut(uid)
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
    pub fn insert_global_diff_token_wrapper(&mut self, token: &DiffTokenWrapper) {
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
                hm.insert(token.uid.clone(), Arc::new(RwLock::new(hs)));
            }
        } else {
            let mut user_hm = HashMap::new();
            let mut hs = HashSet::new();
            hs.insert(token.clone());
            user_hm.insert(token.uid.clone(), Arc::new(RwLock::new(hs)));
            self.global_diff_tokens.insert(token.did, user_hm);
        }
    }

    pub fn check_global_diff_token_for_match(&mut self, token: &DiffTokenWrapper) -> (bool, bool) {
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

    pub fn remove_global_diff_token_wrapper(
        &mut self,
        uid: &UID,
        did: DID,
        token: &DiffTokenWrapper,
    ) -> bool {
        assert!(token.is_global);
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
        self.insert_user_diff_token_wrapper(&new_token_remove(uid.to_string(), did, token));
        return found;
    }

    pub fn update_global_diff_token_from_old_to(
        &mut self,
        old_token: &DiffTokenWrapper,
        new_token: &DiffTokenWrapper,
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
            self.insert_user_diff_token_wrapper(&new_token_modify(uid, did, old_token, new_token));
        }
        found
    }

    /*
     * UPDATE TOKEN FUNCTIONS
     */
    pub fn mark_diff_token_revealed(
        &mut self,
        did: DID,
        token: &DiffTokenWrapper,
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
        token: &OwnershipTokenWrapper,
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
                            token.old_uid, token.did, token.token_id,
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
    pub fn get_all_global_diff_tokens(&self) -> Vec<DiffTokenWrapper> {
        let mut tokens = vec![];
        for (_, global_diff_tokens) in self.global_diff_tokens.iter() {
            for (_, user_tokens) in global_diff_tokens.iter() {
                let utokens = user_tokens.read().unwrap();
                let nonrev_tokens: Vec<DiffTokenWrapper> = utokens
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

    pub fn get_global_diff_tokens_of_disguise(&self, did: DID) -> Vec<DiffTokenWrapper> {
        let mut tokens = vec![];
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&did) {
            for (_, user_tokens) in global_diff_tokens.iter() {
                let utokens = user_tokens.read().unwrap();
                let nonrev_tokens: Vec<DiffTokenWrapper> = utokens
                    .clone()
                    .into_iter()
                    .filter(|t| !t.revealed)
                    .collect();
                for d in nonrev_tokens {
                    tokens.push(d.clone());
                }
            }
        }
        warn!(
            "Got {} global diff tokens for disguise {}\n",
            tokens.len(),
            did
        );
        tokens
    }

    pub fn get_global_diff_tokens(&self, uid: &UID, did: DID) -> Vec<DiffTokenWrapper> {
        if let Some(global_diff_tokens) = self.global_diff_tokens.get(&did) {
            if let Some(user_tokens) = global_diff_tokens.get(uid) {
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
    ) -> (Vec<DiffTokenWrapper>, Vec<OwnershipTokenWrapper>) {
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

    pub fn get_user_pseudoprincipals(
        &self,
        data_cap: &DataCap,
        ownership_loc_caps: &Vec<LocCap>,
    ) -> Vec<UID> {
        let mut uids = vec![];
        if data_cap.is_empty() {
            return vec![];
        }
        for lc in ownership_loc_caps {
            if let Some(pks) = self.enc_ownership_map.get(&lc) {
                for enc_pk in &pks.clone() {
                    // decrypt with data_cap provided by client
                    let (_, plaintext) = enc_pk.decrypt_encdata(data_cap);
                    let pk = ownership_token_from_bytes(&plaintext);
                    if uids.is_empty() {
                        // save the original user too
                        uids.push(pk.old_uid.clone());
                    }
                    uids.push(pk.new_uid.clone());

                    // get all tokens of pseudoprincipal
                    warn!("Getting tokens of pseudoprincipal {}", pk.new_uid);
                    if let Some(pp) = self.principal_data.get(&pk.new_uid) {
                        let ppuids =
                            self.get_user_pseudoprincipals(&pk.priv_key, &pp.ownership_loc_caps);
                        uids.extend(ppuids.iter().cloned());
                    }
                }
            }
        }
        uids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EdnaClient, RowVal, GuiseGen};
    use rsa::pkcs1::ToRsaPrivateKey;

    fn init_logger() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::Warn)
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();
    }

    fn get_insert_guise_cols() -> Vec<String> {
        vec![]
    }

    fn get_insert_guise_vals() -> Vec<sql_parser::ast::Expr> {
        vec![]
    }

    fn get_guise_gen() -> Arc<RwLock<GuiseGen>> {
        Arc::new(RwLock::new(GuiseGen {
            guise_name: "blah".to_string(),
            guise_id_col: "blah".to_string(),
            col_generation: Box::new(get_insert_guise_cols),
            val_generation: Box::new(get_insert_guise_vals),
        }))
    }

    #[test]
    fn test_insert_global_diff_token_single() {
        init_logger();
        let dbname = "testTokenCtrlerGlobal".to_string();
        let edna = EdnaClient::new(true, &dbname, "", true, get_guise_gen());
        let mut conn = edna.get_conn().unwrap();
        let stats = edna.get_stats();

        let mut ctrler = TokenCtrler::new(&mut conn, stats);

        let did = 1;
        let uid = 11;
        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.register_principal(
            &uid.to_string(),
            "email@mail.com".to_string(),
            &pub_key,
            &mut conn,
        );

        let mut remove_token = new_delete_token_wrapper(
            did,
            guise_name,
            guise_ids,
            vec![RowVal {
                column: fk_col.clone(),
                value: old_fk_value.to_string(),
            }],
            true,
        );
        remove_token.uid = uid.to_string();
        ctrler.insert_global_diff_token_wrapper(&mut remove_token);
        assert_eq!(ctrler.global_diff_tokens.len(), 1);
        let tokens = ctrler.get_global_diff_tokens(&uid.to_string(), did);
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], remove_token);
    }

    #[test]
    fn test_insert_user_token_single() {
        init_logger();
        let dbname = "testTokenCtrlerUser".to_string();
        let edna = EdnaClient::new(true, &dbname, "", true, get_guise_gen());
        let mut conn = edna.get_conn().unwrap();
        let stats = edna.get_stats();

        let mut ctrler = TokenCtrler::new(&mut conn, stats);

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
        ctrler.register_principal(
            &uid.to_string(),
            "email@mail.com".to_string(),
            &pub_key,
            &mut conn,
        );

        let mut remove_token = new_delete_token_wrapper(
            did,
            guise_name,
            guise_ids,
            vec![RowVal {
                column: fk_col.clone(),
                value: old_fk_value.to_string(),
            }],
            false,
        );
        remove_token.uid = uid.to_string();
        ctrler.insert_user_diff_token_wrapper(&mut remove_token);
        let lc = ctrler
            .get_tmp_reveal_capability(&uid.to_string(), did)
            .unwrap()
            .clone();
        ctrler.clear_tmp();
        assert_eq!(ctrler.global_diff_tokens.len(), 0);

        // check principal data
        let p = ctrler
            .principal_data
            .get(&uid.to_string())
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
        let dbname = "testTokenCtrlerUserMulti".to_string();
        let edna = EdnaClient::new(true, &dbname, "", true, get_guise_gen());
        let mut conn = edna.get_conn().unwrap();
        let stats = edna.get_stats();

        let mut ctrler = TokenCtrler::new(&mut conn, stats);

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
            ctrler.register_principal(
                &u.to_string(),
                "email@mail.com".to_string(),
                &pub_key,
                &mut conn,
            );
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                for i in 0..iters {
                    let mut remove_token = new_delete_token_wrapper(
                        d,
                        guise_name.clone(),
                        guise_ids.clone(),
                        vec![RowVal {
                            column: fk_col.clone(),
                            value: (old_fk_value + i).to_string(),
                        }],
                        false,
                    );
                    remove_token.uid = u.to_string();
                    ctrler.insert_user_diff_token_wrapper(&mut remove_token);
                }
                let c = ctrler
                    .get_tmp_reveal_capability(&u.to_string(), d)
                    .unwrap()
                    .clone();
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
                .get(&u.to_string())
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert!(p.ownership_loc_caps.is_empty());
            assert!(p.diff_loc_caps.is_empty());

            for d in 1..iters {
                let lc = caps.get(&(u, d)).unwrap().clone();
                // get tokens
                let (diff_tokens, _) =
                    ctrler.get_user_tokens(d, &priv_keys[u as usize - 1], &vec![lc], &vec![]);
                assert_eq!(diff_tokens.len(), (iters as usize));
                for i in 0..iters {
                    let dt = edna_diff_token_from_bytes(&diff_tokens[i as usize].token_data);
                    assert_eq!(dt.old_value[0].value, (old_fk_value + i as u64).to_string());
                }
            }
        }
    }

    #[test]
    fn test_insert_user_token_privkey() {
        init_logger();
        let dbname = "testTokenCtrlerUserPK".to_string();
        let edna = EdnaClient::new(true, &dbname, "", true, get_guise_gen());
        let mut conn = edna.get_conn().unwrap();
        let stats = edna.get_stats();

        let mut ctrler = TokenCtrler::new(&mut conn, stats);

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
            ctrler.register_principal(
                &u.to_string(),
                "email@mail.com".to_string(),
                &pub_key,
                &mut conn,
            );
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                let mut remove_token = new_delete_token_wrapper(
                    d,
                    guise_name.clone(),
                    guise_ids.clone(),
                    vec![RowVal {
                        column: fk_col.clone(),
                        value: (old_fk_value + d).to_string(),
                    }],
                    false,
                );
                remove_token.uid = u.to_string();
                ctrler.insert_user_diff_token_wrapper(&mut remove_token);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some token for the anon user
                let own_token_bytes = edna_own_token_to_bytes(&new_edna_ownership_token(
                    d,
                    guise_name.to_string(),
                    vec![],
                    referenced_name.to_string(),
                    referenced_name.to_string(),
                    fk_col.to_string(),
                    u.to_string(),
                    anon_uid.to_string(),
                ));
                ctrler.register_anon_principal(
                    &u.to_string(),
                    &anon_uid.to_string(),
                    d,
                    own_token_bytes,
                    &mut conn,
                );
                let lc = ctrler
                    .get_tmp_reveal_capability(&u.to_string(), d)
                    .unwrap()
                    .clone();
                caps.insert((u, d), lc);
            }
        }
        assert_eq!(ctrler.global_diff_tokens.len(), 0);
        ctrler.clear_tmp();

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&u.to_string())
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
                let dt = edna_diff_token_from_bytes(&diff_tokens[0].token_data);
                assert_eq!(dt.old_value[0].value, (old_fk_value + d).to_string());
            }
        }
    }
}
