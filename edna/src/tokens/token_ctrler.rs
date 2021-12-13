use crate::generate_keys::get_keys;
use crate::helpers::*;
use crate::stats::QueryStat;
use crate::tokens::*;
use crate::{DID, UID};
use aes::Aes128;
use block_modes::block_padding::Pkcs7;
use block_modes::{BlockMode, Cbc};
use log::{warn, error};
use mysql::prelude::*;
use rand::{rngs::OsRng, Rng, RngCore};
use rsa::pkcs1::{FromRsaPrivateKey, FromRsaPublicKey, ToRsaPublicKey};
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::iter::repeat;
use std::sync::{Arc, Mutex, RwLock};
use std::time;
use  std::mem::size_of_val;

pub type Loc = u64; // locator
pub type DecryptCap = Vec<u8>; // private key
type Aes128Cbc = Cbc<Aes128, Pkcs7>;

const AES_BYTES: usize = 16;
const PRINCIPAL_TABLE: &'static str = "EdnaPrincipals";
const UID_COL: &'static str = "uid";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash, Default)]
pub struct EncData {
    pub enc_key: Vec<u8>,
    pub enc_data: Vec<u8>,
    pub iv: Vec<u8>,
}

impl EncData {
    pub fn decrypt_encdata(&self, decrypt_cap: &DecryptCap) -> (bool, Vec<u8>) {
        if decrypt_cap.is_empty() {
            return (false, vec![]);
        }

        let start = time::Instant::now();
        let priv_key = RsaPrivateKey::from_pkcs1_der(decrypt_cap).unwrap();
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let key: Vec<u8>;
        match priv_key.decrypt(padding, &self.enc_key) {
            Ok(k) => key = k.clone(),
            _ => return (false, vec![]),
        }
        let cipher = Aes128Cbc::new_from_slices(&key, &self.iv).unwrap();
        let mut edata = self.enc_data.clone();
        let plaintext = cipher.decrypt_vec(&mut edata).unwrap();
        warn!("decrypted len is {}: {}", plaintext.len(), start.elapsed().as_micros());
        (true, plaintext)
    }
    pub fn encrypt_with_pubkey(pubkey: &RsaPublicKey, bytes: &Vec<u8>) -> EncData {
        let start = time::Instant::now();
        let mut rng = rand::thread_rng();
        // generate key
        let mut key: Vec<u8> = repeat(0u8).take(AES_BYTES).collect();
        rng.fill_bytes(&mut key[..]);

        // encrypt key with pubkey
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_key = pubkey
            .encrypt(&mut rng, padding, &key[..])
            .expect("failed to encrypt");

        // encrypt pppk with key
        let mut iv: Vec<u8> = repeat(0u8).take(AES_BYTES).collect();
        rng.fill_bytes(&mut iv[..]);
        let cipher = Aes128Cbc::new_from_slices(&key, &iv).unwrap();
        let encrypted = cipher.encrypt_vec(bytes);
        warn!("encrypted len is {}, {}: {}", enc_key.len(), encrypted.len(), start.elapsed().as_micros());
        EncData {
            enc_key: enc_key,
            enc_data: encrypted,
            iv: iv,
        }
    }
}

#[derive(Clone)]
pub struct PrincipalData {
    pub pubkey: RsaPublicKey,
    pub is_anon: bool,

    // only nonempty for pseudoprincipals!
    pub loc_caps: HashSet<EncData>,
}

// OWNER: who gets sent the locator for the bag
// UID OF TOKENS: metadata about locator/how to encrypt at end/etc. uid for L_{uid-d}, but this
// will get sent to OWNER
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Bag {
    pub difftoks: Vec<DiffTokenWrapper>,
    pub owntoks: Vec<OwnershipTokenWrapper>,
    pub pktoks: HashMap<UID, PrivkeyToken>,
    pub owner: UID,
    pub random_padding: Vec<u8>,
}

impl Bag {
    pub fn new(owner: &UID) -> Bag {
        let mut rng = rand::thread_rng();
        let size = rng.gen_range(512..4096);
        let mut padding: Vec<u8> = repeat(0u8).take(size).collect();
        rng.fill_bytes(&mut padding[..]);

        let mut bag: Bag = Default::default();
        bag.owner = owner.clone();
        bag.random_padding = padding;
        bag
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default, Hash)]
pub struct LocCap {
    pub loc: Loc,
    pub uid: UID,
    pub did: DID,
}

#[derive(Clone)]
pub struct TokenCtrler {
    // principal tokens are stored indexed by some large random num
    pub principal_data: HashMap<UID, PrincipalData>,

    pseudoprincipal_keys_pool: Vec<(RsaPrivateKey, RsaPublicKey)>,
    poolsize: usize,
    batch: bool,
    dbserver: String,

    // XXX remove me
    pub plaintext_sz: usize,

    // (p,d) capability -> set of token ciphertext for principal+disguise
    pub enc_map: HashMap<Loc, EncData>,
    pub pps_to_remove: HashSet<UID>,

    pub global_diff_tokens: HashMap<DID, HashMap<UID, Arc<RwLock<HashSet<DiffTokenWrapper>>>>>,

    // used for randomness stuff
    pub rng: OsRng,
    pub hasher: Sha256,

    // used to temporarily store keys used during disguises
    pub tmp_loc_caps: HashMap<(UID, DID), LocCap>,
    pub tmp_remove_principals: HashSet<UID>,
    pub tmp_principals_to_insert: Vec<(UID, PrincipalData)>,
    pub tmp_bags: HashMap<(UID, DID), Bag>,
}

impl TokenCtrler {
    pub fn new(
        poolsize: usize,
        dbserver: &str,
        db: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
        batch: bool,
    ) -> TokenCtrler {
        let mut tctrler = TokenCtrler {
            principal_data: HashMap::new(),
            pseudoprincipal_keys_pool: vec![],
            poolsize: poolsize,
            plaintext_sz: 0,
            batch: batch,
            dbserver: dbserver.to_string(),
            enc_map: HashMap::new(),
            pps_to_remove: Default::default(),
            global_diff_tokens: HashMap::new(),
            rng: OsRng,
            hasher: Sha256::new(),
            tmp_loc_caps: HashMap::new(),
            tmp_remove_principals: HashSet::new(),
            tmp_principals_to_insert: vec![],
            tmp_bags: HashMap::new(),
        };
        // TODO always an in-memory table
        db.query_drop("SET max_heap_table_size = 4294967295;")
            .unwrap();
        //db.query_drop(&format!("DROP TABLE {};", PRINCIPAL_TABLE)).unwrap();
        let createq = format!(
            "CREATE TABLE IF NOT EXISTS {} ({} varchar(255), is_anon tinyint, pubkey varchar(1024), locs varchar(2048), PRIMARY KEY ({})) ENGINE = MEMORY;",
            PRINCIPAL_TABLE, UID_COL, UID_COL);
        db.query_drop(&createq).unwrap();
        let selected = get_query_rows_str(
            &format!("SELECT * FROM {}", PRINCIPAL_TABLE),
            db,
            stats.clone(),
        )
        .unwrap();
        for row in selected {
            let is_anon: bool = row[1].value == "1";
            let pubkey_bytes: Vec<u8> = serde_json::from_str(&row[2].value).unwrap();
            let pubkey = RsaPublicKey::from_pkcs1_der(&pubkey_bytes).unwrap();
            let locs = serde_json::from_str(&row[3].value).unwrap();
            tctrler.register_saved_principal::<mysql::PooledConn>(
                &row[0].value,
                is_anon,
                &pubkey,
                locs,
                false,
                db,
            );
        }
        tctrler.repopulate_pseudoprincipal_keys_pool();
        tctrler
    }

    pub fn get_sizes(&self) -> usize {
        let mut bytes = 0;
        for (key, pd) in self.principal_data.iter() {
            bytes += size_of_val(&*key);
            bytes += size_of_val(&*pd);
            warn!("{}", bytes);
        }
        for (key, em) in self.enc_map.iter() {
            bytes += size_of_val(&*key);
            bytes += size_of_val(&*em);
            warn!("{}", bytes);
        }
        for ppks in &self.pseudoprincipal_keys_pool {
            bytes += size_of_val(&*ppks);
            warn!("{}", bytes);
        }
        for ppuid in self.pps_to_remove.iter() {
            bytes += size_of_val(&*ppuid);
            warn!("{}", bytes);
        }
        error!("PLAINTEXT {}", self.plaintext_sz);
        bytes
    }

    pub fn repopulate_pseudoprincipal_keys_pool(&mut self) {
        warn!(
            "Edna: Repopulating pseudoprincipal key pool of size {}",
            self.poolsize,
        );
        let start = time::Instant::now();
        let keys = get_keys(&self.dbserver).unwrap();
        warn!("Got {} keys", keys.len());
        //let curlen = self.pseudoprincipal_keys_pool.len();
        //for _ in curlen..self.poolsize {
        //let private_key =
        //   RsaPrivateKey::new(&mut self.rng, RSA_BITS).expect("failed to generate a key");
        //let pub_key = RsaPublicKey::from(&private_key);
        self.pseudoprincipal_keys_pool.extend(keys);
        warn!(
            "Edna: Repopulated pseudoprincipal key pool of size {}: {}",
            self.poolsize,
            start.elapsed().as_micros()
        );
    }

    pub fn get_pseudoprincipal_key_from_pool(&mut self) -> (RsaPrivateKey, RsaPublicKey) {
        match self.pseudoprincipal_keys_pool.pop() {
            Some(key) => key,
            None => {
                // XXX todo queue up to run later, but just generate one key first
                self.repopulate_pseudoprincipal_keys_pool();
                self.pseudoprincipal_keys_pool.pop().unwrap()
            }
        }
    }

    /*
     * LOCATING CAPABILITIES
     */
    pub fn get_tmp_capability(&self, uid: &UID, did: DID) -> Option<&LocCap> {
        self.tmp_loc_caps.get(&(uid.to_string(), did))
    }

    pub fn save_and_clear<Q: Queryable>(&mut self, db: &mut Q) -> HashMap<(UID, DID), Vec<LocCap>> {
        // this creates dlcs and olcs btw, so we have to do it first
        assert!(self.batch);
        let mut lcs_to_return: HashMap<(UID, DID), Vec<LocCap>> = HashMap::new();
        let bags = self.tmp_bags.keys().cloned().collect::<Vec<_>>();
        for (uid, did) in &bags {
            let lc = self.get_loc_cap(uid, *did);
            let bag = self.tmp_bags.get(&(uid.to_string(), *did)).unwrap().clone();
            let owner = bag.owner.clone();
            self.update_bag_at_loc(uid.to_string(), &lc, &bag);
            warn!("EdnaBatch: Inserted bag with {} dt, {} wt, {} pks for owner {} uid {}", bag.difftoks.len(), bag.owntoks.len(), bag.pktoks.len(), bag.owner, uid);

            // if we are going to return this to a user, actually return it
            if &owner != uid {
                match lcs_to_return.get_mut(&(owner.clone(), *did)) {
                    Some(lcs) => lcs.push(lc),
                    None => {
                        lcs_to_return.insert((owner.clone(), *did), vec![lc]);
                    }
                }
                warn!("Going to return bag to owner {}", &owner);
                continue;
            } else {
                // otherwise, this pp should exist and be anon?
                warn!("Going to return bag to uid {}", &uid);
                let p = self
                    .principal_data
                    .get_mut(uid)
                    .expect(&format!("no user with uid {} when saving?", uid));
                // save to principal data if no email (pseudoprincipal)
                if p.is_anon {
                    let enc_lc = EncData::encrypt_with_pubkey(&p.pubkey, &serialize_to_bytes(&lc));
                    p.loc_caps.insert(enc_lc);

                    // update persistence
                    let uidstr = uid.trim_matches('\'');
                    db.query_drop(&format!(
                        "UPDATE {} SET {} = \'{}\' WHERE {} = \'{}\'",
                        PRINCIPAL_TABLE,
                        "locs",
                        serde_json::to_string(&p.loc_caps).unwrap(),
                        UID_COL,
                        uidstr
                    ))
                    .unwrap();
                } else {
                    match lcs_to_return.get_mut(&(owner.clone(), *did)) {
                        Some(lcs) => lcs.push(lc),
                        None => {
                            lcs_to_return.insert((owner.clone(), *did), vec![lc]);
                        }
                    }
                }
            }
        }
        // actually remove the principals supposed to be removed
        for uid in self.tmp_remove_principals.clone().iter() {
            self.remove_principal::<Q>(&uid, db);
        }
        self.persist_principals::<Q>(db);
        self.clear_tmp();
        lcs_to_return
    }

    // XXX note this doesn't allow for concurrent disguising right now
    pub fn clear_tmp(&mut self) {
        self.tmp_loc_caps.clear();
        self.tmp_remove_principals.clear();
        self.tmp_bags.clear();
    }

    fn get_loc_cap(&mut self, uid: &UID, did: DID) -> LocCap {
        // get the location capability being used for this disguise
        match self.tmp_loc_caps.get(&(uid.clone(), did)) {
            // if there's a loccap already, use it
            Some(lc) => return lc.clone(),
            // otherwise generate it (and save it temporarily)
            None => {
                let cap = LocCap {
                    loc: self.rng.next_u64(),
                    uid: uid.clone(),
                    did: did,
                };
                // temporarily save cap for future use
                assert_eq!(self.tmp_loc_caps.insert((uid.clone(), did), cap.clone()), None);
                return cap;
            }
        }
    }

    /*
     * REGISTRATION
     */
    pub fn register_saved_principal<Q: Queryable>(
        &mut self,
        uid: &UID,
        is_anon: bool,
        pubkey: &RsaPublicKey,
        lc: HashSet<EncData>,
        persist: bool,
        db: &mut Q,
    ) {
        warn!("Re-registering saved principal {}", uid);
        let pdata = PrincipalData {
            pubkey: pubkey.clone(),
            is_anon: is_anon,
            loc_caps: lc,
        };
        if persist {
            self.mark_principal_to_insert(uid, &pdata);
            self.persist_principals::<Q>(db);
        }
        self.principal_data.insert(uid.clone(), pdata);
    }

    pub fn register_principal<Q: Queryable>(
        &mut self,
        uid: &UID,
        is_anon: bool,
        db: &mut Q,
        persist: bool,
    ) -> RsaPrivateKey {
        warn!("Registering principal {}", uid);
        let (private_key, pubkey) = self.get_pseudoprincipal_key_from_pool();
        let pdata = PrincipalData {
            pubkey: pubkey,
            is_anon: is_anon,
            loc_caps: HashSet::new(),
        };

        self.mark_principal_to_insert(uid, &pdata);
        if persist {
            self.persist_principals::<Q>(db);
        }
        self.principal_data.insert(uid.clone(), pdata);
        private_key
    }

    pub fn register_anon_principal<Q: Queryable>(
        &mut self,
        uid: &UID,
        anon_uid: &UID,
        did: DID,
        ownership_token_data: Vec<u8>,
        db: &mut Q,
        original_uid: &Option<UID>,
    ) {
        let start = time::Instant::now();
        let uidstr = uid.trim_matches('\'');
        let anon_uidstr = anon_uid.trim_matches('\'');

        // save the anon principal as a new principal with a public key
        // and initially empty token vaults
        let private_key = self.register_principal::<Q>(&anon_uidstr.to_string(), true, db, false);
        warn!(
            "Edna: ownership token from anon principal {} to original {}",
            anon_uid, uid
        );
        let own_token_wrapped = new_generic_ownership_token_wrapper(
            uidstr.to_string(),
            anon_uidstr.to_string(),
            did,
            ownership_token_data,
        );
        let privkey_token = new_privkey_token(
            uidstr.to_string(),
            anon_uidstr.to_string(),
            did,
            &private_key,
        );
        let foruid = match original_uid {
            None => uid,
            Some(ouid) => ouid
        };
        self.insert_ownership_token_wrapper_for(&own_token_wrapped, foruid);
        self.insert_privkey_token_for(&privkey_token, foruid);
        warn!(
            "Edna: register anon principal: {}",
            start.elapsed().as_micros()
        );
    }

    fn mark_principal_to_insert(&mut self, uid: &UID, pdata: &PrincipalData) {
        self.tmp_principals_to_insert
            .push((uid.clone(), pdata.clone()));
    }

    fn persist_principals<Q: Queryable>(&mut self, db: &mut Q) {
        if self.tmp_principals_to_insert.is_empty() {
            return;
        }
        let start = time::Instant::now();
        let mut values = vec![];
        for (uid, pdata) in &self.tmp_principals_to_insert {
            let pubkey_vec = pdata.pubkey.to_pkcs1_der().unwrap().as_der().to_vec();
            let v: Vec<String> = vec![];
            let empty_vec = serde_json::to_string(&v).unwrap();
            let uid = uid.trim_matches('\'');
            values.push(format!(
                "(\'{}\', {}, \'{}\', \'{}\')",
                uid,
                if pdata.is_anon { 1 } else { 0 },
                serde_json::to_string(&pubkey_vec).unwrap(),
                empty_vec
            ));
        }
        let insert_q = format!(
            "INSERT INTO {} ({}, is_anon, pubkey, locs) \
                VALUES {} ON DUPLICATE KEY UPDATE {} = VALUES({});",
            PRINCIPAL_TABLE,
            UID_COL,
            values.join(", "),
            "locs",
            "locs",
        );
        warn!("Persist Principals insert q {}", insert_q);
        db.query_drop(&insert_q).unwrap();
        warn!(
            "Edna persist {} principals: {}",
            self.tmp_principals_to_insert.len(),
            start.elapsed().as_micros()
        );
        self.tmp_principals_to_insert.clear();
    }

    pub fn mark_principal_to_be_removed(&mut self, uid: &UID, did: DID) {
        let start = time::Instant::now();
        let p = self.principal_data.get_mut(uid).unwrap();
        // save to principal data if anon (pseudoprincipal)
        // we only want to remove PPs if the corresponding owntokens have been cleared
        if p.is_anon {
            return;
        }
        let mut ptoken = new_remove_principal_token_wrapper(uid, did, &p);
        self.insert_user_diff_token_wrapper_for(&mut ptoken, uid);
        self.tmp_remove_principals.insert(uid.to_string());
        warn!(
            "Edna: mark principal {} to remove : {}",
            uid,
            start.elapsed().as_micros()
        );
    }

    pub fn remove_principal<Q: Queryable>(&mut self, uid: &UID, db: &mut Q) {
        // actually remove
        let start = time::Instant::now();
        let pdata = self.principal_data.get_mut(uid);
        if pdata.is_none() {
            return;
        }
        let pdata = pdata.unwrap();
        if !pdata.loc_caps.is_empty() {
            // save as to_remove
            self.pps_to_remove.insert(uid.to_string());
            // TODO persist this?
        } else {
            // actually remove metadata
            warn!("Actually removing principal metadata {}\n", uid);
            self.principal_data.remove(uid);
            warn!(
                "DELETE FROM {} WHERE {} = \'{}\'",
                PRINCIPAL_TABLE,
                UID_COL,
                uid.trim_matches('\'')
            );
            db.query_drop(format!(
                "DELETE FROM {} WHERE {} = \'{}\'",
                PRINCIPAL_TABLE,
                UID_COL,
                uid.trim_matches('\'')
            ))
            .unwrap();
        }
        warn!("Edna: remove principal: {}", start.elapsed().as_micros());
    }

    /*
     * PRINCIPAL TOKEN INSERT
     */
    fn update_bag_at_loc(&mut self, uid: UID, lc: &LocCap, bag: &Bag) {
        let p = self
            .principal_data
            .get(&uid)
            .expect("no user with uid found?")
            .clone();
        let plaintext = serialize_to_bytes(bag);
        self.plaintext_sz += plaintext.len();
        let enc_bag = EncData::encrypt_with_pubkey(&p.pubkey, &plaintext);
        // insert the encrypted pppk into locating capability
        self.enc_map.insert(lc.loc, enc_bag);
        warn!("EdnaBatch: Saved bag for {}", uid);
    }

    fn insert_privkey_token_for(&mut self, pppk: &PrivkeyToken, uid: &UID) {
        let start = time::Instant::now();
        let p = self.principal_data.get_mut(&pppk.old_uid);
        if p.is_none() {
            warn!("no user with uid {} found?", pppk.old_uid);
            return;
        }
        assert!(self.batch);
        match self.tmp_bags.get_mut(&(uid.clone(), pppk.did.clone())) {
            Some(bag) => {
                bag.owner = uid.clone();
                // important: insert the mapping from new_uid to pppk
                bag.pktoks.insert(pppk.new_uid.clone(), pppk.clone());
            }
            None => {
                let mut new_bag = Bag::new(uid);
                new_bag.pktoks.insert(pppk.new_uid.clone(), pppk.clone());
                self.tmp_bags
                    .insert((uid.clone(), pppk.did.clone()), new_bag);
            }
        }
        warn!("Inserted privkey token from uid {} for {}: {}", pppk.new_uid, uid, start.elapsed().as_micros());
    }

    fn insert_ownership_token_wrapper_for(&mut self, pppk: &OwnershipTokenWrapper, uid: &UID) {
        let start = time::Instant::now();
        let p = self.principal_data.get_mut(&pppk.old_uid);
        if p.is_none() {
            warn!("no user with uid {} found?", pppk.old_uid);
            return;
        }
        assert!(self.batch);
        match self.tmp_bags.get_mut(&(pppk.old_uid.clone(), pppk.did)) {
            Some(bag) => {
                bag.owner = uid.clone();
                bag.owntoks.push(pppk.clone());
            }
            None => {
                let mut new_bag = Bag::new(uid);
                new_bag.owntoks.push(pppk.clone());
                self.tmp_bags
                    .insert((pppk.old_uid.clone(), pppk.did.clone()), new_bag);
            }
        }
        warn!("Inserted own token: {}", start.elapsed().as_micros());
    }

    pub fn insert_user_diff_token_wrapper_for(&mut self, token: &DiffTokenWrapper, uid: &UID) {
        let start = time::Instant::now();
        let did = token.did;
        warn!(
            "inserting user diff token for owner {} did {} of uid {}",
            uid, did, token.uid
        );

        assert!(self.batch);
        match self.tmp_bags.get_mut(&(token.uid.clone(), did.clone())) {
            Some(bag) => {
                if !(&bag.owner == "" || &bag.owner == uid) {
                    warn!("Owner was {}, setting to {}", bag.owner, uid);
                    assert!(false);
                }
                bag.owner = uid.clone();
                bag.difftoks.push(token.clone());
            }
            None => {
                let mut new_bag = Bag::new(uid);
                new_bag.difftoks.push(token.clone());
                self.tmp_bags
                    .insert((token.uid.clone(), did.clone()), new_bag);
            }
        }
        warn!("Inserted diff token: {}", start.elapsed().as_micros());
    }

    /*
     * GLOBAL TOKEN FUNCTIONS
     */
    /*pub fn insert_global_diff_token_wrapper(&mut self, token: &DiffTokenWrapper) {
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
    }*/

    /*
     * GET TOKEN FUNCTIONS
     */
    /*pub fn get_all_global_diff_tokens(&self) -> Vec<DiffTokenWrapper> {
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
    }*/

    //XXX could have flag to remove locators so we don't traverse twice
    //this would remove locators regardless of success in revealing
    pub fn get_user_tokens(
        &mut self,
        decrypt_cap: &DecryptCap,
        lc: &LocCap,
    ) -> (
        Vec<DiffTokenWrapper>,
        Vec<OwnershipTokenWrapper>,
        HashMap<UID, DecryptCap>,
    ) {
        let mut diff_tokens = vec![];
        let mut own_tokens = vec![];
        let mut pk_tokens = HashMap::new();
        if decrypt_cap.is_empty() {
            return (diff_tokens, own_tokens, pk_tokens);
        }
        assert!(self.batch);
        if let Some(encbag) = self.enc_map.get(&lc.loc) {
            warn!("Getting tokens of user {} with lc {}", lc.uid, lc.loc);
            let start = time::Instant::now();
            // decrypt token with decrypt_cap provided by client
            let (succeeded, plaintext) = encbag.decrypt_encdata(decrypt_cap);
            if !succeeded {
                return (diff_tokens, own_tokens, pk_tokens);
            }
            let mut bag: Bag = serde_json::from_slice(&plaintext).unwrap();

            // remove if we found a matching token for the disguise
            diff_tokens.append(&mut bag.difftoks);
            own_tokens.append(&mut bag.owntoks);
            warn!(
                "EdnaBatch: Decrypted diff, own, pk tokens added {}, {}, {}: {}",
                bag.difftoks.len(),
                bag.owntoks.len(),
                bag.pktoks.len(),
                start.elapsed().as_micros(),
            );

            // get ALL new_uids regardless of disguise that token came from
            let mut new_uids = vec![];
            for (new_uid, pk) in &bag.pktoks {
                new_uids.push((new_uid.clone(), pk.priv_key.clone()));
                pk_tokens.insert(new_uid.clone(), pk.priv_key.clone());
            }
            // get all tokens of pseudoprincipal
            for (new_uid, privkey) in new_uids {
                if let Some(pp) = self.principal_data.get(&new_uid) {
                    let pp = pp.clone();
                    warn!(
                        "Getting tokens of pseudoprincipal {} with data {}, {:?}",
                        new_uid,
                        privkey.len(),
                        &pp.loc_caps,
                    );
                    for enclc in pp.loc_caps {
                        let (_, lcbytes) = enclc.decrypt_encdata(&privkey);
                        let tmp: [u8; 8] = lcbytes
                            .try_into()
                            .expect("Could not turn u64 vec into bytes?");
                        let lc: LocCap = serde_json::from_slice(&tmp).unwrap();
                        let (mut pp_diff_tokens, mut pp_own_tokens, pp_pk_tokens) =
                            self.get_user_tokens(&privkey, &lc);
                        diff_tokens.append(&mut pp_diff_tokens);
                        own_tokens.append(&mut pp_own_tokens);
                        for (new_uid, pk) in &pp_pk_tokens{
                            pk_tokens.insert(new_uid.clone(), pk.clone());
                        }
                    }
                }
            }
        }
        // return tokens matching disguise and the removed locs from this iteration
        (diff_tokens, own_tokens, pk_tokens)
    }

    pub fn cleanup_user_tokens(
        &mut self,
        did: DID,
        decrypt_cap: &DecryptCap,
        lc: &LocCap,
        db: &mut mysql::PooledConn,
    ) -> (bool, bool, bool) {
        // delete locators + encrypted tokens
        // remove pseudoprincipal metadata if caps are empty
        let mut no_diffs_at_loc = true;
        let mut no_owns_at_loc = true;
        let mut no_pks_at_loc = true;
        let mut kept_privkeys = HashMap::new();
        let mut changed = false;
        if decrypt_cap.is_empty() {
            return (false, false, false);
        }
        assert!(self.batch);
        if let Some(encbag) = self.enc_map.get(&lc.loc) {
            let start = time::Instant::now();
            no_diffs_at_loc = false;
            no_owns_at_loc = false;
            no_pks_at_loc = false;

            let (success, plaintext) = encbag.decrypt_encdata(decrypt_cap);
            if !success {
                warn!(
                    "Could not decrypt encdata at {:?} with decryptcap", lc 
                );
                return (false, false, false);
            }
            let mut bag: Bag = serde_json::from_slice(&plaintext).unwrap();
            let tokens = bag.difftoks.clone();
            warn!(
                "EdnaBatch: Decrypted diff tokens added {}: {}",
                tokens.len(),
                start.elapsed().as_micros(),
            );
            // remove if we found a matching token for the disguise
            if tokens.is_empty() || tokens[0].did == did {
                changed = true;
                no_diffs_at_loc = true;
                bag.difftoks = vec![];
            }
            let tokens = bag.owntoks.clone();
            warn!(
                "EdnaBatchCleanup: Decrypted own tokens added {}: {}",
                tokens.len(),
                start.elapsed().as_micros(),
            );
            if tokens.is_empty() || tokens[0].did == did {
                changed = true;
                no_owns_at_loc = true;
                bag.owntoks = vec![];
            }
            let mut new_uids = vec![];
            let tokens = bag.pktoks;
            warn!(
                "EdnaBatchCleanup: Decrypted pk tokens added {}: {}",
                tokens.len(),
                start.elapsed().as_micros(),
            );
            // get ALL new_uids regardless of disguise that token came from
            for (new_uid, pk) in &tokens {
                new_uids.push((new_uid.clone(), pk.clone()));
            }
            // remove matching tokens of pseudoprincipals
            // for each pseudoprincipal for which we hold a private key
            for (new_uid, pkt) in &new_uids {
                if let Some(pp) = self.principal_data.get(new_uid) {
                    let mut new_pp = pp.clone();
                    warn!(
                        "Cleanup: Getting tokens of pseudoprincipal {} with data {}, {:?}",
                        new_uid,
                        pkt.priv_key.len(),
                        new_pp.loc_caps,
                    );

                    for enclc in new_pp.loc_caps.clone() {
                        let (_, lcbytes) = enclc.decrypt_encdata(&pkt.priv_key);
                        let pplc: LocCap = serde_json::from_slice(&lcbytes).unwrap();

                        // for each locator that the pp has
                        // clean up the tokens at the locators
                        let (no_diffs_at_loc, no_owns_at_loc, no_pks_at_loc) =
                            self.cleanup_user_tokens(did, &pkt.priv_key, &pplc, db);

                        // remove loc from pp if nothing's left at that loc
                        let mut removed = false;
                        if no_diffs_at_loc && no_owns_at_loc && no_pks_at_loc {
                            removed = new_pp.loc_caps.remove(&enclc);
                            changed |= removed;
                        }

                        // check if we should remove the pp
                        let should_remove = self.pps_to_remove.contains(new_uid);

                        // remove the pp if it has no more bags and should be removed,
                        // otherwise update the pp's metadata in edna if changed
                        if should_remove && new_pp.loc_caps.is_empty() {
                            // either remove the principal metadata
                            warn!("Removing metadata of {}", new_uid);
                            self.remove_principal(&new_uid, db);
                            self.pps_to_remove.remove(new_uid);
                        } else if removed {
                            warn!("Updating metadata of {}", new_uid);
                            self.mark_principal_to_insert(&new_uid, &new_pp);
                            self.persist_principals::<mysql::PooledConn>(db);
                            self.principal_data.insert(new_uid.clone(), new_pp.clone());
                        }

                        // this is a privkey whose corresponding pp still has data :\
                        // we need to keep it
                        if !(should_remove && new_pp.loc_caps.is_empty()) {
                            kept_privkeys.insert(new_uid.clone(), pkt.clone());
                        }
                    }
                }
            }
            // if this is empty, yay! no more private keys at this principal
            if kept_privkeys.is_empty() {
                no_pks_at_loc = true;
            }
            // actually remove locs
            if no_diffs_at_loc && no_owns_at_loc && no_pks_at_loc {
                self.enc_map.remove(&lc.loc);
            } else if changed {
                // update the encrypted store of stuff if changed at all
                bag.pktoks = kept_privkeys;
                //self.update_bag_at_loc(uid.to_string(), lc, &bag);
            }
        }
        // return whether we removed bags
        (no_diffs_at_loc, no_owns_at_loc, no_pks_at_loc)
    }

    pub fn get_user_pseudoprincipals(
        &self,
        decrypt_cap: &DecryptCap,
        loc_caps: &HashSet<LocCap>,
    ) -> Vec<UID> {
        let mut uids = vec![];
        if decrypt_cap.is_empty() {
            return vec![];
        }
        for lc in loc_caps {
            if let Some(encbag) = self.enc_map.get(&lc.loc) {
                warn!("Getting pps of user");
                let start = time::Instant::now();
                // decrypt token with decrypt_cap provided by client
                let (_, plaintext) = encbag.decrypt_encdata(decrypt_cap);
                let bag: Bag = serde_json::from_slice(&plaintext).unwrap();
                let tokens = bag.owntoks;
                for pk in &tokens {
                    if uids.is_empty() {
                        // save the original user too
                        uids.push(pk.old_uid.clone());
                    }
                    uids.push(pk.new_uid.clone());
                }
                let mut new_uids = vec![];
                let tokens = bag.pktoks;
                for (new_uid, pk) in &tokens {
                    new_uids.push((new_uid.clone(), pk.priv_key.clone()));
                }
                // get all tokens of pseudoprincipal
                for (new_uid, pk) in new_uids {
                    warn!("Getting tokens of pseudoprincipal {}", new_uid);
                    if let Some(pp) = self.principal_data.get(&new_uid) {
                        let mut pplcs = HashSet::new();
                        for enclc in &pp.loc_caps {
                            let (_, lcbytes) = enclc.decrypt_encdata(&pk);
                            pplcs.insert(serde_json::from_slice(&lcbytes).unwrap());
                        }
                        let ppuids = self.get_user_pseudoprincipals(&pk, &pplcs);
                        uids.extend(ppuids.iter().cloned());
                    }
                }
                warn!(
                    "Got tokens of pseudoprincipal: {}",
                    start.elapsed().as_micros()
                );
            }
        }
        uids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EdnaClient, GuiseGen, RowVal};
    use rsa::pkcs1::ToRsaPrivateKey;
    use sql_parser::ast::*;

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
        vec!["id".to_string()]
    }

    fn get_insert_guise_vals() -> Vec<Expr> {
        vec![Expr::Value(Value::Number(0.to_string()))]
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
    fn test_insert_user_diff_token_multi() {
        init_logger();
        let iters = 5;
        let dbname = "testTokenCtrlerUserMulti".to_string();
        let edna = EdnaClient::new(
            true,
            true,
            "127.0.0.1",
            &dbname,
            "",
            true,
            iters,
            get_guise_gen(),
        );
        let mut db = edna.get_conn().unwrap();
        let stats = edna.get_stats();

        let mut ctrler = TokenCtrler::new(
            iters,
            "mysql://tslilyai:pass@127.0.0.1",
            &mut db,
            stats,
            true,
        );

        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut priv_keys = vec![];

        let mut caps = HashMap::new();
        for u in 1..iters {
            let private_key = ctrler.register_principal::<mysql::PooledConn>(
                &u.to_string(),
                false,
                &mut db,
                true,
            );
            let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                for i in 0..iters {
                    let mut remove_token = new_delete_token_wrapper(
                        d as u64,
                        guise_name.clone(),
                        guise_ids.clone(),
                        vec![RowVal {
                            column: fk_col.clone(),
                            value: (old_fk_value + (i as u64)).to_string(),
                        }],
                        false,
                    );
                    remove_token.uid = u.to_string();
                    ctrler.insert_user_diff_token_wrapper_for(&mut remove_token, &u.to_string());
                }
                let lcs = ctrler.save_and_clear::<mysql::PooledConn>(&mut db);
                caps.extend(lcs);
            }
        }
        assert_eq!(ctrler.global_diff_tokens.len(), 0);
        ctrler.clear_tmp();
        assert!(ctrler.tmp_loc_caps.is_empty());

        for u in 1..iters {
            // check principal data
            let p = ctrler
                .principal_data
                .get(&u.to_string())
                .expect("failed to get user?")
                .clone();
            assert!(p.loc_caps.is_empty());

            for d in 1..iters {
                let lc = caps.get(&(u.to_string(), d as u64)).unwrap().clone();
                let (diff_tokens, _, _) = ctrler.get_user_tokens(&priv_keys[u - 1], &lc[0]);
                assert_eq!(diff_tokens.len(), (iters as usize));
                for i in 0..iters {
                    let dt = edna_diff_token_from_bytes(&diff_tokens[i].token_data);
                    assert_eq!(
                        dt.old_value[0].value,
                        (old_fk_value + (i as u64)).to_string()
                    );
                }
            }
        }
    }

    #[test]
    fn test_insert_user_token_privkey() {
        init_logger();
        let iters = 5;
        let dbname = "testTokenCtrlerUserPK".to_string();
        let edna = EdnaClient::new(
            true,
            true,
            "127.0.0.1",
            &dbname,
            "",
            true,
            iters,
            get_guise_gen(),
        );
        let mut db = edna.get_conn().unwrap();
        let stats = edna.get_stats();

        let mut ctrler = TokenCtrler::new(
            iters,
            "mysql://tslilyai:pass@127.0.0.1",
            &mut db,
            stats,
            true,
        );

        let guise_name = "guise".to_string();
        let guise_ids = vec![];
        let referenced_name = "referenced".to_string();
        let old_fk_value = 5;
        let fk_col = "fk_col".to_string();

        let mut rng = OsRng;
        let mut priv_keys = vec![];

        let mut caps = HashMap::new();
        for u in 1..iters {
            let private_key = ctrler.register_principal::<mysql::PooledConn>(
                &u.to_string(),
                false,
                &mut db,
                true,
            );
            let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
            priv_keys.push(private_key_vec.clone());

            for d in 1..iters {
                let mut remove_token = new_delete_token_wrapper(
                    d as u64,
                    guise_name.clone(),
                    guise_ids.clone(),
                    vec![RowVal {
                        column: fk_col.clone(),
                        value: (old_fk_value + (d as u64)).to_string(),
                    }],
                    false,
                );
                remove_token.uid = u.to_string();
                ctrler.insert_user_diff_token_wrapper_for(&mut remove_token, &u.to_string());

                let anon_uid: u64 = rng.next_u64();
                // create an anonymous user
                // and insert some token for the anon user
                let own_token_bytes = edna_own_token_to_bytes(&new_edna_ownership_token(
                    d as u64,
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
                    d as u64,
                    own_token_bytes,
                    &mut db,
                    &None,
                );
                let lc = ctrler.save_and_clear::<mysql::PooledConn>(&mut db);
                caps.extend(lc);
            }
        }
        assert_eq!(ctrler.global_diff_tokens.len(), 0);

        for u in 1..iters {
            // check principal data
            ctrler
                .principal_data
                .get(&u.to_string())
                .expect("failed to get user?");

            for d in 1..iters {
                let lc = caps.get(&(u.to_string(), d as u64)).unwrap().clone();
                let (diff_tokens, own_tokens, _) =
                    ctrler.get_user_tokens(&priv_keys[u as usize - 1], &lc[0]);
                assert_eq!(diff_tokens.len(), 1);
                assert_eq!(own_tokens.len(), 1);
                let dt = edna_diff_token_from_bytes(&diff_tokens[0].token_data);
                assert_eq!(
                    dt.old_value[0].value,
                    (old_fk_value + (d as u64)).to_string()
                );
            }
        }
    }
}
