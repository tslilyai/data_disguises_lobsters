use crate::helpers::*;
use crate::diffs::*;
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
    // each principal has two lists of encrypted diffs,
    // sharded by disguise ID
    cd_lists: HashMap<DID, EncListTail>,
    privkey_lists: HashMap<DID, EncListTail>,
    pubkey: RsaPublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedDiff {
    diff_data: Vec<u8>,
    iv: Vec<u8>,
}

#[derive(Clone)]
pub struct DiffCtrler {
    // principal diffs are stored indexed by some large random num
    pub principal_diffs: HashMap<UID, PrincipalData>,

    // a large array of encrypted diffs indexed by random number
    pub user_vaults_map: HashMap<u64, EncryptedDiff>,

    pub global_vault: HashMap<(DID, UID), Arc<RwLock<HashSet<Diff>>>>,

    pub rng: OsRng,
    pub hasher: Sha256,

    pub tmp_symkeys: HashMap<(UID, DID), ListSymKey>,
}

impl DiffCtrler {
    pub fn new() -> DiffCtrler {
        DiffCtrler {
            principal_diffs: HashMap::new(),
            user_vaults_map: HashMap::new(),
            global_vault: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_symkeys: HashMap::new(),
        }
    }

    pub fn insert_global_diff(&mut self, diff: &mut Diff) {
        diff.is_global = true;
        diff.diff_id = self.rng.next_u64();
        warn!(
            "Inserting global diff disguise {} user {}",
            diff.did, diff.uid
        );
        if let Some(user_disguise_diffs) = self.global_vault.get_mut(&(diff.did, diff.uid)) {
            let mut diffs = user_disguise_diffs.write().unwrap();
            diffs.insert(diff.clone());
        } else {
            let mut hs = HashSet::new();
            hs.insert(diff.clone());
            self.global_vault
                .insert((diff.did, diff.uid), Arc::new(RwLock::new(hs)));
        }
    }

    pub fn insert_user_diff(&mut self, diff_type: DiffType, diff: &mut Diff) {
        assert!(diff.uid != 0);
        diff.is_global = false;
        let did = diff.did;
        let uid = diff.uid;
        warn!("inserting user diff {:?} with uid {} did {}", diff, uid, did);
        let p = self
            .principal_diffs
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

        // give the diff a random nonce
        diff.nonce = self.rng.next_u64();

        // insert encrypted diff into list for principal
        let next_diff_ptr = self.rng.next_u64();
        diff.diff_id = next_diff_ptr;

        let disguise_lists = match diff_type {
            DiffType::Data => &mut p.cd_lists,
            DiffType::PrivKey => &mut p.privkey_lists,
        };
        match disguise_lists.get_mut(&did) {
            // if the list exists, just append and set the tail
            Some(diffls) => {
                diff.last_tail = diffls.tail;
                diffls.tail = next_diff_ptr;
            }
            // if the list doesn't exist, also encrypt and set the symmetric key
            None => {
                // XXX the last tail could legit be 0, although this is so improbable
                diff.last_tail = 0;
                let padding = PaddingScheme::new_pkcs1v15_encrypt();
                let enc_symkey = p
                    .pubkey
                    .encrypt(&mut self.rng, padding, &symkey[..])
                    .expect("failed to encrypt");
                let diffls = EncListTail {
                    tail: next_diff_ptr,
                    list_enc_symkey: EncListSymKey {
                        enc_symkey: enc_symkey,
                        uid: uid,
                        did: did,
                    },
                };
                disguise_lists.insert(did, diffls);
            }
        }

        // encrypt and add the diff to the encrypted diffs array
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
        // ensure that no diff existed at this pointer before
        assert_eq!(
            self.user_vaults_map.insert(
                next_diff_ptr,
                EncryptedDiff {
                    diff_data: encrypted,
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
        self.principal_diffs.insert(
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
        // and initially empty diff vaults
        self.register_principal(anon_uid, &pub_key);
        let mut diff: Diff = Diff::new_privkey_diff(uid, did, anon_uid, &private_key);
        self.insert_user_diff(DiffType::PrivKey, &mut diff);
        anon_uid
    }

    pub fn remove_anon_principal(&mut self, anon_uid: UID) {
        self.principal_diffs.remove(&anon_uid);
    }
    
    pub fn check_global_diff_for_match(&mut self, diff: &Diff) -> (bool, bool) {
        if let Some(global_diffs) = self.global_vault.get(&(diff.did, diff.uid)) {
            let diffs = global_diffs.read().unwrap();
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
        return (false, false);
    }
 
    pub fn remove_global_diff(&mut self, uid: UID, did: DID, diff: &Diff) -> bool {
        assert!(diff.is_global);
        assert!(uid != 0);
        let mut found = false;
        
        // delete diff 
        if let Some(global_diffs) = self.global_vault.get(&(diff.did, diff.uid)) {
            let mut diffs = global_diffs.write().unwrap();
            // just insert the diff to replace the old one
            diffs.remove(&diff);
            found = true;
        }
        // log diff for disguise that marks removal
        self.insert_user_diff(
            DiffType::Data,
            &mut Diff::new_diff_remove(uid, did, diff),
        );
        return found;
    }

    pub fn update_global_diff_from_old_to (
        &mut self,
        old_diff: &Diff,
        new_diff: &Diff,
        record_diff_for_disguise: Option<(UID,DID)> 
    ) -> bool {
        assert!(new_diff.is_global);
        let mut found = false;
        if let Some(global_diffs) = self.global_vault.get(&(new_diff.did, new_diff.uid)) {
            let mut diffs = global_diffs.write().unwrap();
            // just insert the diff to replace the old one
            diffs.insert(new_diff.clone());
            found = true;
        }
        if let Some((uid, did)) = record_diff_for_disguise {
            self.insert_user_diff(
                DiffType::Data,
                &mut Diff::new_diff_modify(uid, did, old_diff, new_diff),
            );
        }
        found
    }

    pub fn mark_diff_revealed(
        &mut self,
        diff: &Diff,
    ) -> bool {
        let mut found = false;
        if diff.is_global {
            if let Some(global_diffs) = self.global_vault.get(&(diff.did, diff.uid)) {
                let mut diffs = global_diffs.write().unwrap();
                // just insert the diff to replace the old one
                let mut t = diff.clone();
                t.revealed = true;
                diffs.insert(t);
                found = true;
            }
            return found;
        }

        // otherwise search the user list
        let symkey = match self.tmp_symkeys.get(&(diff.uid, diff.did)) {
            Some(sk) => sk,
            None => unimplemented!("Diff to update inaccessible!"),
        };

        let p = self
            .principal_diffs
            .get(&symkey.uid)
            .expect("no user with uid found?");

        // iterate through user's encrypted correlation/datadiffs
        if let Some(diffls) = p.cd_lists.get(&symkey.did) {
            let mut tail_ptr = diffls.tail;
            loop {
                match self.user_vaults_map.get_mut(&tail_ptr) {
                    Some(enc_diff) => {
                        // decrypt diff with symkey provided by client
                        warn!(
                            "Got cd data of len {} with symkey {:?}-{:?}",
                            enc_diff.diff_data.len(),
                            symkey.symkey,
                            enc_diff.iv
                        );
                        let cipher =
                            Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                        let plaintext = cipher.decrypt_vec(&mut enc_diff.diff_data).unwrap();
                        let mut t = Diff::diff_from_bytes(plaintext);
                        if t.diff_id == diff.diff_id {
                            t.revealed = true;
                            // XXX do we need a new IV?
                            let cipher =
                                Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                            let plaintext = serialize_to_bytes(&t);
                            let encrypted = cipher.encrypt_vec(&plaintext);
                            let iv = enc_diff.iv.clone();
                            assert_eq!(encrypted.len() % 16, 0);
                            self.user_vaults_map.insert(
                                tail_ptr,
                                EncryptedDiff {
                                    diff_data: encrypted,
                                    iv: iv,
                                },
                            );
                            warn!(
                                "diff uid {} disguise {} revealed diff {}",
                                diff.uid, diff.did, diff.diff_id,
                            );
                            found = true;
                            break;
                        }

                        // update which encrypted diff is to be next in list
                        tail_ptr = diff.last_tail;
                    }
                    None => break,
                }
            }
        }
        found
    }

    pub fn get_global_diffs(&self, uid: UID, did: DID) -> Vec<Diff> {
        if let Some(global_diffs) = self.global_vault.get(&(did, uid)) {
            let diffs = global_diffs.read().unwrap();
            return diffs.clone().into_iter().filter(|t| !t.revealed).collect();
        }
        vec![]
    }

    pub fn get_diffs(&mut self, symkeys: &HashSet<ListSymKey>, save_keys: bool) -> Vec<Diff> {
        let mut cd_diffs = vec![];

        for symkey in symkeys {
            if save_keys {
                // save symkeys for later in the disguise
                self.tmp_symkeys
                    .insert((symkey.uid, symkey.did), symkey.clone());
            }

            let p = self
                .principal_diffs
                .get(&symkey.uid)
                .expect("no user with uid found?");

            // XXX should we check that client didn't forge symkey?
            // we would need to remember padding scheme

            // get all of this user's globally accessible diffs
            cd_diffs.append(&mut self.get_global_diffs(symkey.uid, symkey.did));
            warn!("cd diffs global pushed to len {}", cd_diffs.len());

            // get all of this user's encrypted correlation/datadiffs
            if let Some(diffls) = p.cd_lists.get(&symkey.did) {
                let mut tail_ptr = diffls.tail;
                loop {
                    match self.user_vaults_map.get_mut(&tail_ptr) {
                        Some(enc_diff) => {
                            // decrypt diff with symkey provided by client
                            warn!(
                                "Got cd data of len {} with symkey {:?}-{:?}",
                                enc_diff.diff_data.len(),
                                symkey.symkey,
                                enc_diff.iv
                            );
                            let cipher =
                                Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                            let plaintext = cipher.decrypt_vec(&mut enc_diff.diff_data).unwrap();
                            let diff = Diff::diff_from_bytes(plaintext);

                            // add diff to list only if it hasn't be revealed before
                            if !diff.revealed {
                                cd_diffs.push(diff.clone());
                            }
                            warn!(
                                "cd diffs uid {} disguise {} pushed to len {}",
                                diff.uid,
                                diff.did,
                                cd_diffs.len()
                            );

                            // update which encrypted diff is to be next in list
                            tail_ptr = diff.last_tail;
                        }
                        None => break,
                    }
                }
            }

            // get all privkey diffs, even from other disguises
            let mut privkey_diffs = vec![];
            if let Some(diffls) = p.privkey_lists.get(&symkey.did) {
                let mut tail_ptr = diffls.tail;
                loop {
                    match self.user_vaults_map.get_mut(&tail_ptr) {
                        Some(enc_diff) => {
                            // decrypt diff with symkey provided by client
                            warn!(
                                "Got privkey data of len {} with symkey {:?}-{:?}",
                                enc_diff.diff_data.len(),
                                symkey.symkey,
                                enc_diff.iv
                            );
                            let cipher =
                                Aes128Cbc::new_from_slices(&symkey.symkey, &enc_diff.iv).unwrap();
                            let plaintext = cipher.decrypt_vec(&mut enc_diff.diff_data).unwrap();
                            let diff = Diff::diff_from_bytes(plaintext);

                            // add diff to list
                            privkey_diffs.push(diff.clone());

                            // go to next encrypted diff in list
                            tail_ptr = diff.last_tail;
                        }
                        None => break,
                    }
                }
            }

            // use privkey diffs to decrypt symkeys of anon principles, and recursively get all of their cd_diffs
            let mut new_symkeys = HashSet::new();
            for pk_diff in &privkey_diffs {
                let priv_key = RsaPrivateKey::from_pkcs1_der(&pk_diff.priv_key).unwrap();
                new_symkeys.extend(self.get_all_principal_symkeys(pk_diff.new_uid, priv_key));
            }
            cd_diffs.extend(self.get_diffs(&new_symkeys, save_keys));
            warn!("cd diffs extended to len {}", cd_diffs.len());
        }
        cd_diffs
    }

    pub fn get_encrypted_symkey(&self, uid: UID, did: DID) -> Option<EncListSymKey> {
        let p = self
            .principal_diffs
            .get(&uid)
            .expect("no user with uid found?");
        if let Some(diffls) = p.cd_lists.get(&did) {
            return Some(diffls.list_enc_symkey.clone());
        }
        None
    }

    fn get_all_principal_symkeys(&self, uid: UID, priv_key: RsaPrivateKey) -> HashSet<ListSymKey> {
        let mut symkeys = HashSet::new();
        let p = self
            .principal_diffs
            .get(&uid)
            .expect("no user with uid found?");
        for (_, diffls) in &p.cd_lists {
            let padding = PaddingScheme::new_pkcs1v15_encrypt();
            let symkey = priv_key
                .decrypt(padding, &diffls.list_enc_symkey.enc_symkey)
                .expect("failed to decrypt");
            symkeys.insert(ListSymKey {
                uid: uid,
                did: diffls.list_enc_symkey.did,
                symkey: symkey,
            });
        }
        for (_, diffls) in &p.privkey_lists {
            let padding = PaddingScheme::new_pkcs1v15_encrypt();
            let symkey = priv_key
                .decrypt(padding, &diffls.list_enc_symkey.enc_symkey)
                .expect("failed to decrypt");
            symkeys.insert(ListSymKey {
                uid: uid,
                did: diffls.list_enc_symkey.did,
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
        ctrler.register_principal(uid, &pub_key);

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
        assert_eq!(ctrler.global_vault.len(), 1);
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
        ctrler.register_principal(uid, &pub_key);

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
        ctrler.insert_user_diff(DiffType::Data, &mut decor_diff);
        ctrler.clear_symkeys();
        assert_eq!(ctrler.global_vault.len(), 0);

        // check principal data
        let p = ctrler
            .principal_diffs
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

        // get diffs
        let cddiffs = ctrler.get_diffs(&hs, true);
        assert_eq!(cddiffs.len(), 1);
        assert_eq!(cddiffs[0], decor_diff);
    }

    #[test]
    fn test_insert_user_diff_multi() {
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
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, &pub_key);
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
                    ctrler.insert_user_diff(DiffType::Data, &mut decor_diff);
                }
            }
        }
        assert_eq!(ctrler.global_vault.len(), 0);
        ctrler.clear_symkeys();
        assert!(ctrler.tmp_symkeys.is_empty());

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_diffs
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

                // get diffs
                let cddiffs = ctrler.get_diffs(&hs, true);
                assert_eq!(cddiffs.len(), (iters as usize));
                for i in 0..iters {
                    assert_eq!(
                        cddiffs[i as usize].old_value[0].value,
                        (old_fk_value + (iters - i - 1) as u64).to_string()
                    );
                    assert_eq!(
                        cddiffs[i as usize].new_value[0].value,
                        (new_fk_value + (iters - i - 1) as u64).to_string()
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
        for u in 1..iters {
            let private_key =
                RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
            let pub_key = RsaPublicKey::from(&private_key);
            ctrler.register_principal(u, &pub_key);
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
                ctrler.insert_user_diff(DiffType::Data, &mut decor_diff);

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some diff for the anon user
                ctrler.register_anon_principal(u, anon_uid, d);
            }
        }
        assert_eq!(ctrler.global_vault.len(), 0);
        ctrler.clear_symkeys();
        assert!(ctrler.tmp_symkeys.is_empty());

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_diffs
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

                // get diffs
                let cddiffs = ctrler.get_diffs(&hs, true);
                assert_eq!(cddiffs.len(), 1);
                assert_eq!(
                    cddiffs[0].old_value[0].value,
                    (old_fk_value + d).to_string()
                );
                assert_eq!(
                    cddiffs[0].new_value[0].value,
                    (new_fk_value + d).to_string()
                );
            }
        }
    }
}
