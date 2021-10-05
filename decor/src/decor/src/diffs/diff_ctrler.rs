use crate::diffs::*;
use crate::helpers::*;
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
pub type HashedDataCap= Vec<u8>; // private key
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
    // only for pseudoprincipals!
    loc_caps: Vec<LocCap>,
}

#[derive(Clone)]
pub struct DiffCtrler {
    // principal diffs are stored indexed by some large random num
    pub principal_data: HashMap<UID, PrincipalData>,

    // (p,d) capability -> set of diff ciphertext for principal+disguise
    pub enc_diffs_map: HashMap<LocCap, Vec<EncData>>,

    // (p,d) capability -> set of diff ciphertext for principal+disguise
    pub enc_pks_map: HashMap<HashedDataCap, Vec<EncData>>,

    pub global_diffs: HashMap<DID, HashMap<UID, Arc<RwLock<HashSet<Diff>>>>>,

    // used for randomness stuff
    pub rng: OsRng,
    pub hasher: Sha256,

    // used to temporarily store keys used during disguises
    pub tmp_loc_caps: HashMap<(UID, DID), LocCap>,
}

impl DiffCtrler {
    pub fn new() -> DiffCtrler {
        DiffCtrler {
            principal_data: HashMap::new(),
            enc_diffs_map: HashMap::new(),
            enc_pks_map: HashMap::new(),
            global_diffs: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_loc_caps: HashMap::new(),
        }
    }

    /*
     * TEMP STORAGE AND CLEARING
     */
    pub fn get_tmp_capability(&self, uid: UID, did: DID) -> Option<&LocCap> {
        self.tmp_loc_caps.get(&(uid, did))
    }

    pub fn save_and_clear_loc_caps(&mut self) -> HashMap<(UID, DID), LocCap> {
        let lcs = self.tmp_loc_caps.clone();
        for ((uid, _), c) in lcs.iter() {
            let p = self.principal_data.get_mut(&uid).unwrap();
            // save to principal data if no email (pseudoprincipal)
            if p.email.is_empty() {
                p.loc_caps.push(*c);
            } else {
                // TODO email capability to user if user has email
                //self.loc_caps.insert((*uid, *did), *c);
            }
        }
        self.clear_tmp();
        lcs
    }

    // XXX note this doesn't allow for concurrent disguising right now
    pub fn clear_tmp(&mut self) {
        self.tmp_loc_caps.clear();
    }

    /*
     * LOCATING CAPABILITIES
     */
    fn get_loc_cap(&mut self, uid: u64, did: u64) -> LocCap {
        // get the location capability being used for this disguise
        match self.tmp_loc_caps.get(&(uid, did)) {
            // if there's a loccap already, use it
            Some(lc) => return *lc,
            // otherwise generate it (and save it temporarily)
            None => {
                let cap = self.rng.next_u64();
                // temporarily save cap for future use
                assert_eq!(self.tmp_loc_caps.insert((uid, did), cap), None);
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
                loc_caps: vec![],
            },
        );
    }

    pub fn register_anon_principal(&mut self, uid: UID, anon_uid: UID, did: DID, data_cap: &DataCap) -> UID {
        let private_key =
            RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);

        // save the anon principal as a new principal with a public key
        // and initially empty diff vaults
        self.register_principal(anon_uid, String::new(), &pub_key);
        let mut pppk: PPPrivKey = new_ppprivkey(uid, did, anon_uid, &private_key);
        self.insert_ppprivkey(&mut pppk, data_cap);
        anon_uid
    }

    pub fn remove_anon_principal(&mut self, anon_uid: UID) {
        warn!("Removing principal {}", anon_uid);
        self.principal_data.remove(&anon_uid);
    }

    /*
     * PRINCIPAL DIFF INSERT
     */
    fn insert_ppprivkey(&mut self, pppk: &mut PPPrivKey, data_cap: &DataCap) {
        assert!(pppk.uid != 0);

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

        // insert the encrypted pppk into hash(data_cap)
        self.hasher.update(data_cap);
        let result = self.hasher.finalize_reset().to_vec();
        match self.enc_pks_map.get_mut(&result) {
            Some(ts) => {
                ts.push(enc_pppk);
            }
            None => {
                self.enc_pks_map.insert(result, vec![enc_pppk]);
            }
        }
    }

    pub fn insert_user_data_diff(&mut self, diff: &mut Diff) {
        assert!(diff.uid != 0);
        diff.is_global = false;
        let did = diff.did;
        let uid = diff.uid;
        warn!(
            "inserting user diff {:?} with uid {} did {}",
            diff, uid, did
        );

        let cap = self.get_loc_cap(uid, did);

        let p = self
            .principal_data
            .get_mut(&uid)
            .expect("no user with uid found?");

        // give the diff a random nonce and some id
        diff.nonce = self.rng.next_u64();
        diff.diff_id = self.rng.next_u64();

        // generate key
        let mut key: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut key[..]);

        // encrypt key with pubkey
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_key = p
            .pubkey
            .encrypt(&mut self.rng, padding, &key[..])
            .expect("failed to encrypt");

        // encrypt and add the diff to the map of encrypted diffs
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
        let plaintext = serialize_to_bytes(&diff);
        let encrypted = cipher.encrypt_vec(&plaintext);
        assert_eq!(encrypted.len() % 16, 0);
        let encdiff = EncData {
            enc_key: enc_key,
            enc_data: encrypted,
            iv: iv,
        };
        match self.enc_diffs_map.get_mut(&cap) {
            Some(ts) => {
                ts.push(encdiff);
            }
            None => {
                self.enc_diffs_map.insert(cap, vec![encdiff]);
            }
        }
    }

    /*
     * GLOBAL DIFF FUNCTIONS
     */
    pub fn insert_global_diff(&mut self, diff: &mut Diff) {
        diff.is_global = true;
        diff.diff_id = self.rng.next_u64();
        warn!(
            "Inserting global diff disguise {} user {}",
            diff.did, diff.uid
        );
        if let Some(hm) = self.global_diffs.get_mut(&diff.did) {
            if let Some(user_disguise_diffs) = hm.get_mut(&diff.uid) {
                let mut diffs = user_disguise_diffs.write().unwrap();
                diffs.insert(diff.clone());
            } else {
                let mut hs = HashSet::new();
                hs.insert(diff.clone());
                hm.insert(diff.uid, Arc::new(RwLock::new(hs)));
            }
        } else {
            let mut user_hm = HashMap::new();
            let mut hs = HashSet::new();
            hs.insert(diff.clone());
            user_hm.insert(diff.uid, Arc::new(RwLock::new(hs)));
            self.global_diffs.insert(diff.did, user_hm);
        }
    }

    pub fn check_global_diff_for_match(&mut self, diff: &Diff) -> (bool, bool) {
        if let Some(global_diffs) = self.global_diffs.get(&diff.did) {
            if let Some(user_diffs) = global_diffs.get(&diff.uid) {
                let diffs = user_diffs.read().unwrap();
                for t in diffs.iter() {
                    if t.diff_id == diff.diff_id {
                        // XXX todo this is a bit inefficient
                        let mut mydiff = diff.clone();
                        mydiff.revealed = t.revealed;
                        let eq = mydiff == *t;
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

    pub fn remove_global_diff(&mut self, uid: UID, did: DID, diff: &Diff) -> bool {
        assert!(diff.is_global);
        assert!(uid != 0);
        let mut found = false;

        // delete diff
        if let Some(global_diffs) = self.global_diffs.get(&diff.did) {
            if let Some(user_diffs) = global_diffs.get(&diff.uid) {
                let mut diffs = user_diffs.write().unwrap();
                diffs.remove(&diff);
                found = true;
            }
        }
        // log diff for disguise that marks removal
        self.insert_user_data_diff(&mut Diff::new_diff_remove(uid, did, diff));
        return found;
    }

    pub fn update_global_diff_from_old_to(
        &mut self,
        old_diff: &Diff,
        new_diff: &Diff,
        record_diff_for_disguise: Option<(UID, DID)>,
    ) -> bool {
        assert!(new_diff.is_global);
        let mut found = false;
        if let Some(global_diffs) = self.global_diffs.get(&old_diff.did) {
            if let Some(user_diffs) = global_diffs.get(&old_diff.uid) {
                let mut diffs = user_diffs.write().unwrap();
                // just insert the diff to replace the old one
                diffs.insert(new_diff.clone());
                found = true;
            }
        }
        if let Some((uid, did)) = record_diff_for_disguise {
            self.insert_user_data_diff(&mut Diff::new_diff_modify(uid, did, old_diff, new_diff));
        }
        found
    }

    /*
     * UPDATE DIFF FUNCTIONS
     */
    pub fn mark_diff_revealed(&mut self, diff: &Diff, data_cap: &DataCap, loc_cap: LocCap) -> bool {
        let mut found = false;
        if diff.is_global {
            if let Some(global_diffs) = self.global_diffs.get(&diff.did) {
                if let Some(user_diffs) = global_diffs.get(&diff.uid) {
                    let mut diffs = user_diffs.write().unwrap();
                    let mut t = diff.clone();
                    t.revealed = true;
                    diffs.insert(t);
                    found = true;
                }
                // just return if the disguise was global
            }
            return found;
        }

        // return if no diffs accessible
        if data_cap.is_empty() {
            return false;
        }

        // iterate through user's encrypted datadiffs
        if let Some(diffls) = self.enc_diffs_map.get_mut(&loc_cap) {
            for (i, enc_diff) in diffls.iter_mut().enumerate() {
                // decrypt data and compare
                let (key, diffplaintext) = enc_diff.decrypt_encdata(data_cap);
                let mut curdiff = diff_from_bytes(&diffplaintext);
                if curdiff.diff_id == diff.diff_id {
                    curdiff.revealed = true;
                    let cipher = Aes128Cbc::new_from_slices(&key, &enc_diff.iv).unwrap();
                    let plaintext = serialize_to_bytes(&curdiff);
                    let encrypted = cipher.encrypt_vec(&plaintext);
                    let iv = enc_diff.iv.clone();
                    assert_eq!(encrypted.len() % 16, 0);
                    // replace diff with updated diff
                    diffls[i] = EncData {
                        enc_key: enc_diff.enc_key.clone(),
                        enc_data: encrypted,
                        iv: iv,
                    };
                    warn!(
                        "diff uid {} disguise {} revealed diff {}",
                        diff.uid, diff.did, diff.diff_id,
                    );
                    return true;
                }
            }
        }

        // iterate through allowed pseudoprincipals' diffs as well
        self.hasher.update(data_cap);
        let result = self.hasher.finalize_reset().to_vec();
        if let Some(pks) = self.enc_pks_map.get(&result) {
            for enc_pk in &pks.clone() {
                // decrypt with data_cap provided by client
                let (_, plaintext) = enc_pk.decrypt_encdata(data_cap);
                let pk = ppprivkey_from_bytes(&plaintext);
                
                // get all diffs of pseudoprincipal
                if let Some(pp) = self.principal_data.get(&pk.new_uid) {
                    for lc in &pp.loc_caps.clone() {
                        if self.mark_diff_revealed(diff, &pk.priv_key, *lc) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /*
     * GET DIFF FUNCTIONS
     */
    pub fn get_global_diffs_of_disguise(&self, did: DID) -> Vec<Diff> {
        let mut diffs = vec![];
        if let Some(global_diffs) = self.global_diffs.get(&did) {
            for (_, user_diffs) in global_diffs.iter() {
                let udiffs = user_diffs.read().unwrap();
                let nonrev_diffs : Vec<Diff> = udiffs.clone().into_iter().filter(|t| !t.revealed).collect();
                for d in nonrev_diffs {
                    diffs.push(d.clone());
                }
            }
        }
        diffs
    }

    pub fn get_global_diffs(&self, uid: UID, did: DID) -> Vec<Diff> {
        if let Some(global_diffs) = self.global_diffs.get(&did) {
            if let Some(user_diffs) = global_diffs.get(&uid) {
                let diffs = user_diffs.read().unwrap();
                warn!(
                    "Filtering {} global diffs of disg {} user {}",
                    diffs.len(),
                    did,
                    uid
                );
                return diffs.clone().into_iter().filter(|t| !t.revealed).collect();
            }
        }
        vec![]
    }

    pub fn get_diffs(&mut self, data_cap: &DataCap, loc_cap: LocCap) -> Vec<Diff> {
        let mut diffs = vec![];
        if data_cap.is_empty() {
            return diffs;
        }
        if let Some(diffls) = self.enc_diffs_map.get(&loc_cap) {
            warn!("Getting diffs of user from ls len {}", diffls.len());
            for enc_diff in diffls {
                // decrypt diff with data_cap provided by client
                let (_, plaintext) = enc_diff.decrypt_encdata(data_cap);
                let diff = diff_from_bytes(&plaintext);

                // add diff to list only if it hasn't be revealed before
                if !diff.revealed {
                    diffs.push(diff.clone());
                }
                warn!(
                    "diffs uid {} disguise {} pushed to len {}",
                    diff.uid,
                    diff.did,
                    diffs.len()
                );
            }
        }
        // get allowed pseudoprincipal diffs
        self.hasher.update(data_cap);
        let result = self.hasher.finalize_reset().to_vec();
        if let Some(pks) = self.enc_pks_map.get(&result) {
            for enc_pk in pks.clone() {
                // decrypt with data_cap provided by client
                let (_, plaintext) = enc_pk.decrypt_encdata(data_cap);
                let pk = ppprivkey_from_bytes(&plaintext);
                
                // get all diffs of pseudoprincipal
                warn!("Getting diffs of pseudoprincipal {}", pk.new_uid);
                if let Some(pp) = self.principal_data.get(&pk.new_uid) {
                    for lc in pp.loc_caps.clone() {
                        diffs.extend(
                            self.get_diffs(&pk.priv_key, lc)
                                .iter()
                                .cloned(),
                        );
                    }
                }
            }
        }
        diffs
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
    fn test_insert_global_diff_single() {
        init_logger();
        let mut ctrler = DiffCtrler::new();

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

        let mut decor_diff = Diff::new_decor_diff(
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
        decor_diff.uid = uid;
        ctrler.insert_global_diff(&mut decor_diff);
        assert_eq!(ctrler.global_diffs.len(), 1);
        let diffs = ctrler.get_global_diffs(uid, did);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0], decor_diff);
    }

    #[test]
    fn test_insert_user_diff_single() {
        init_logger();
        let mut ctrler = DiffCtrler::new();

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
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        let pub_key = RsaPublicKey::from(&private_key);
        ctrler.register_principal(uid, "email@mail.com".to_string(), &pub_key);

        let mut decor_diff = Diff::new_decor_diff(
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
        decor_diff.uid = uid;
        ctrler.insert_user_data_diff(&mut decor_diff);
        let lc = ctrler.get_tmp_capability(uid, did).unwrap().clone();
        ctrler.clear_tmp();
        assert_eq!(ctrler.global_diffs.len(), 0);

        // check principal data
        let p = ctrler
            .principal_data
            .get(&uid)
            .expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert!(p.loc_caps.is_empty());
        assert!(ctrler.tmp_loc_caps.is_empty());

        // get diffs
        let diffs = ctrler.get_diffs(&private_key_vec, lc);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0], decor_diff);
    }

    #[test]
    fn test_insert_user_data_diff_multi() {
        init_logger();
        let mut ctrler = DiffCtrler::new();

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
            let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                for i in 0..iters {
                    let mut decor_diff = Diff::new_decor_diff(
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
                    decor_diff.uid = u;
                    ctrler.insert_user_data_diff(&mut decor_diff);
                }
                let c = ctrler.get_tmp_capability(u, d).unwrap().clone();
                caps.insert((u, d), c);
            }
        }
        assert_eq!(ctrler.global_diffs.len(), 0);
        ctrler.clear_tmp();
        assert!(ctrler.tmp_loc_caps.is_empty());

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&(u))
                .expect("failed to get user?")
                .clone();
            assert_eq!(pub_keys[u as usize - 1], p.pubkey);
            assert!(p.loc_caps.is_empty());

            for d in 1..iters {
                let lc = caps.get(&(u, d)).unwrap().clone();
                // get diffs
                let diffs = ctrler.get_diffs(&priv_keys[u as usize - 1], lc);
                assert_eq!(diffs.len(), (iters as usize));
                for i in 0..iters {
                    assert_eq!(
                        diffs[i as usize].old_value[0].value,
                        (old_fk_value + i as u64).to_string()
                    );
                    assert_eq!(
                        diffs[i as usize].new_value[0].value,
                        (new_fk_value + i as u64).to_string()
                    );
                }
            }
        }
    }

    #[test]
    fn test_insert_user_diff_privkey() {
        init_logger();
        let mut ctrler = DiffCtrler::new();

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
            let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                let mut decor_diff = Diff::new_decor_diff(
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
                decor_diff.uid = u;
                ctrler.insert_user_data_diff(&mut decor_diff);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some diff for the anon user
                ctrler.register_anon_principal(u, anon_uid, d, &priv_keys[u as usize - 1]);
                let lc = ctrler.get_tmp_capability(u, d).unwrap().clone();
                caps.insert((u, d), lc);
            }
        }
        assert_eq!(ctrler.global_diffs.len(), 0);
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
                // get diffs
                let diffs = ctrler.get_diffs(&priv_keys[u as usize - 1], c);
                assert_eq!(diffs.len(), 1);
                assert_eq!(diffs[0].old_value[0].value, (old_fk_value + d).to_string());
                assert_eq!(diffs[0].new_value[0].value, (new_fk_value + d).to_string());
            }
        }
    }
}
