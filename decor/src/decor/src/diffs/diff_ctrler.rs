use crate::helpers::*;
use crate::diffs::*;
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
pub struct EncData {
    pub data: Vec<u8>,
    pub iv: Vec<u8>,
}

#[derive(Clone)]
pub struct PrincipalData {
    locked_pks: Vec<LockedPPPrivKey>,
    pubkey: RsaPublicKey,
    email: String,
    // only for pseudoprincipals!
    capabilities: Vec<Capability>,
}

#[derive(Clone)]
pub struct DiffCtrler {
    // principal diffs are stored indexed by some large random num
    pub principal_data: HashMap<UID, PrincipalData>,

    // (p,d) capability -> set of diff ciphertext for principal+disguise
    pub data_diffs_map: HashMap<Capability, Vec<EncData>>,

    // (p,d) capability -> encrypted symkey for principal+disguise
    pub enc_diff_symkeys_map: HashMap<Capability, EncSymKey>,

    pub global_diffs: HashMap<DID, HashMap<UID, Arc<RwLock<HashSet<Diff>>>>>,

    // used for randomness stuff
    pub rng: OsRng,
    pub hasher: Sha256,

    // used to temporarily store keys used during disguises
    pub tmp_symkeys: HashMap<(UID, DID), SymKey>,
    pub tmp_capabilities: HashMap<(UID, DID), Capability>,

    // XXX get rid of this, just for testing
    pub capabilities: HashMap<(UID, DID), Capability>,
}

impl DiffCtrler {
    pub fn new() -> DiffCtrler {
        DiffCtrler {
            principal_data: HashMap::new(),
            data_diffs_map: HashMap::new(),
            enc_diff_symkeys_map: HashMap::new(),
            global_diffs: HashMap::new(),
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
                locked_pks: vec![],
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
        // and initially empty diff vaults
        self.register_principal(anon_uid, String::new(), &pub_key);
        let mut pppk: PPPrivKey = new_ppprivkey(uid, did, anon_uid, &private_key);
        self.insert_ppprivkey(&mut pppk);
        anon_uid
    }

    pub fn get_locked_ppprivkeys_of_user(&self, uid: UID) -> Vec<LockedPPPrivKey> {
        match self.principal_data.get(&uid) {
            Some(p) => p.locked_pks.clone(),
            None => vec![],
        }
    }

    pub fn remove_anon_principal(&mut self, anon_uid: UID) {
        self.principal_data.remove(&anon_uid);
    }

    /*
     * TOKEN INSERT
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

    pub fn insert_ppprivkey(&mut self, pppk: &mut PPPrivKey) {
        assert!(pppk.uid != 0);

        let p = self
            .principal_data
            .get_mut(&pppk.uid)
            .expect("no user with uid found?");

        // generate key
        let mut key: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut key[..]);

        // encrypt pppk with key
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
        let plaintext = serialize_to_bytes(&pppk);
        let encrypted = cipher.encrypt_vec(&plaintext);
        let enc_pppk = EncData {
            data: encrypted,
            iv: iv,
        };

        // encrypt key with pubkey
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_symkey = p
            .pubkey
            .encrypt(&mut self.rng, padding, &key[..])
            .expect("failed to encrypt");

        // save
        p.locked_pks.push(LockedPPPrivKey{
            enc_key: enc_symkey,
            enc_pppk: enc_pppk,
        });
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
                    self.enc_diff_symkeys_map.insert(
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

        // give the diff a random nonce and some id
        diff.nonce = self.rng.next_u64();
        diff.diff_id = self.rng.next_u64();

        // encrypt and add the diff to the map of encrypted diffs corresponding to this
        // capability
        let mut iv: Vec<u8> = repeat(0u8).take(16).collect();
        self.rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&symkey, &iv).unwrap();
        let plaintext = serialize_to_bytes(&diff);
        let encrypted = cipher.encrypt_vec(&plaintext);
        assert_eq!(encrypted.len() % 16, 0);
        warn!(
            "Encrypted diff data of len {} with symkey {:?}-{:?}",
            encrypted.len(),
            symkey,
            iv
        );
        let enctok = EncData {
            data: encrypted,
            iv: iv,
        };
        match self.data_diffs_map.get_mut(&cap) {
            Some(ts) => {
                ts.push(enctok);
            }
            None => {
                self.data_diffs_map.insert(*cap, vec![enctok]);
            }
        }
    }

    /*
     * GLOBAL TOKEN FUNCTIONS
     */
    pub fn check_global_diff_for_match(&mut self, diff: &Diff) -> (bool, bool) {
        if let Some(global_diffs) = self.global_diffs.get(&diff.did) {
            if let Some(user_diffs) = global_diffs.get(&diff.uid) {
                let diffs = user_diffs.read().unwrap();
                for t in diffs.iter() {
                    if t.diff_id == diff.diff_id {
                        // XXX todo this is a bit inefficient
                        let mut mytok = diff.clone();
                        mytok.revealed = t.revealed;
                        let eq = mytok == *t;
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
            self.insert_user_data_diff(&mut Diff::new_diff_modify(
                uid, did, old_diff, new_diff,
            ));
        }
        found
    }

    /*
     * UPDATE TOKEN FUNCTIONS
     */
    pub fn mark_diff_revealed(&mut self, diff: &Diff) -> bool {
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

        // otherwise search the user list
        let symkey = match self.tmp_symkeys.get(&(diff.uid, diff.did)) {
            Some(sk) => sk,
            None => unimplemented!("Diff to update inaccessible!"),
        };
        let cap = match self.tmp_capabilities.get(&(diff.uid, diff.did)) {
            Some(c) => c,
            None => unimplemented!("Diff to update inaccessible!"),
        };

        // iterate through user's encrypted datadiffs
        if let Some(diffls) = self.data_diffs_map.get_mut(&cap) {
            for (i, enc_diff) in diffls.iter_mut().enumerate() {
                // decrypt diff with symkey provided by client
                warn!(
                    "Got cd data of len {} with symkey {:?}-{:?}",
                    enc_diff.data.len(),
                    symkey.symkey,
                    enc_diff.iv
                );
                let cipher = Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                let plaintext = cipher.decrypt_vec(&mut enc_diff.data).unwrap();
                let mut t = diff_from_bytes(plaintext);
                if t.diff_id == diff.diff_id {
                    t.revealed = true;
                    // XXX do we need a new IV?
                    let cipher = Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                    let plaintext = serialize_to_bytes(&t);
                    let encrypted = cipher.encrypt_vec(&plaintext);
                    let iv = enc_diff.iv.clone();
                    assert_eq!(encrypted.len() % 16, 0);
                    // replace diff with updated diff
                    diffls[i] = EncData {
                        data: encrypted,
                        iv: iv,
                    };
                    warn!(
                        "diff uid {} disguise {} revealed diff {}",
                        diff.uid, diff.did, diff.diff_id,
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
    pub fn get_global_diffs(&self, uid: UID, did: DID) -> Vec<Diff> {
        if let Some(global_diffs) = self.global_diffs.get(&did) {
            if let Some(user_diffs) = global_diffs.get(&uid) {
                let diffs = user_diffs.read().unwrap();
                warn!("Filtering {} global diffs of disg {} user {}", diffs.len(), did, uid);
                return diffs.clone().into_iter().filter(|t| !t.revealed).collect();
            }
        }
        vec![]
    }

    pub fn get_diffs(
        &mut self,
        symkeys: &Vec<(SymKey, Capability)>,
        global_diffs_of: Vec<(DID, UID)>,
        for_disguise: bool,
    ) -> Vec<Diff> {
        let mut diffs = vec![];
        for (did, uid) in global_diffs_of {
            diffs.append(&mut self.get_global_diffs(uid, did));
        }
        warn!("cd diffs global pushed to len {}", diffs.len());

        for (symkey, cap) in symkeys {
            if for_disguise {
                // save symkeys for later in the disguise
                self.tmp_symkeys
                    .insert((symkey.uid, symkey.did), symkey.clone());
                self.tmp_capabilities.insert((symkey.uid, symkey.did), *cap);
            }

            // get all of this user's globally accessible diffs
            diffs.append(&mut self.get_global_diffs(symkey.uid, symkey.did));
            warn!("cd diffs global pushed to len {}", diffs.len());

            // get all of this user's encrypted correlation/datadiffs
            if let Some(diffls) = self.data_diffs_map.get_mut(&cap) {
                for enc_diff in diffls {
                    // decrypt diff with symkey provided by client
                    warn!(
                        "Got cd data of len {} with symkey {:?}-{:?}",
                        enc_diff.data.len(),
                        symkey.symkey,
                        enc_diff.iv
                    );
                    let cipher = Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                    let plaintext = cipher.decrypt_vec(&mut enc_diff.data).unwrap();
                    let diff = diff_from_bytes(plaintext);

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
        }
        diffs
    }

    /*
     * GET ENC SYMKEYS FUNCTIONS
     */
    pub fn get_pseudouid_enc_diff_symkeys(&self, puid: UID) -> Vec<(EncSymKey, Capability)> {
        let p = self
            .principal_data
            .get(&puid)
            .expect("no user with uid found?");
        assert!(p.email.is_empty());

        let mut esks = vec![];
        for c in &p.capabilities {
            match self.enc_diff_symkeys_map.get(&c) {
                Some(esk) => esks.push((esk.clone(), *c)),
                None => (),
            }
        }
        esks
    }

    pub fn get_enc_symkey(&self, cap: Capability) -> Option<EncSymKey> {
        if let Some(esk) = self.enc_diff_symkeys_map.get(&cap) {
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
        let c = ctrler.get_tmp_capability(uid, did).unwrap().clone();
        ctrler.clear_tmp();
        assert_eq!(ctrler.global_diffs.len(), 0);

        // check principal data
        let p = ctrler
            .principal_data
            .get(&uid)
            .expect("failed to get user?");
        assert_eq!(pub_key, p.pubkey);
        assert!(p.locked_pks.is_empty());
        assert!(p.capabilities.is_empty());
        assert!(ctrler.tmp_symkeys.is_empty());
        assert!(ctrler.tmp_capabilities.is_empty());

        // check symkey stored for principal lists
        let encsymkey = ctrler
            .enc_diff_symkeys_map
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

        // get diffs
        let diffs = ctrler.get_diffs(&keys, vec![], true);
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
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key.clone());

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
            assert!(p.locked_pks.is_empty());
            assert!(p.capabilities.is_empty());

            for d in 1..iters {
                let c = caps.get(&(u, d)).unwrap().clone();
                let encsymkey = ctrler
                    .enc_diff_symkeys_map
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
                // get diffs
                let diffs = ctrler.get_diffs(&keys, vec![], true);
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
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, "email@mail.com".to_string(), &pub_key);
            pub_keys.push(pub_key.clone());
            priv_keys.push(private_key.clone());

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
                ctrler.register_anon_principal(u, anon_uid, d);
                let c = ctrler.get_tmp_capability(u, d).unwrap().clone();
                caps.insert((u, d), c);
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
            assert_eq!(p.locked_pks.len() as u64, iters - 1);

            for d in 1..iters {
                let c = caps.get(&(u, d)).unwrap().clone();
                let encsymkey = ctrler
                    .enc_diff_symkeys_map
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
                // get diffs
                let diffs = ctrler.get_diffs(&keys, vec![], true);
                assert_eq!(diffs.len(), 1);
                assert_eq!(diffs[0].old_value[0].value, (old_fk_value + d).to_string());
                assert_eq!(diffs[0].new_value[0].value, (new_fk_value + d).to_string());
            }
        }
    }
}
