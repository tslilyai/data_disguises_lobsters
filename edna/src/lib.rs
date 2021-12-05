extern crate base64;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
extern crate flamer;
extern crate mysql;
extern crate ordered_float;

use log::warn;
use mysql::prelude::*;
use mysql::Opts;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::*;

pub mod disguise;
pub mod generate_keys;
pub mod helpers;
pub mod predicate;
pub mod spec;
pub mod stats;
pub mod tokens;

pub type DID = u64;
pub type UID = String;

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RowVal {
    pub column: String,
    pub value: String,
}
pub struct GuiseGen {
    pub guise_name: String,
    pub guise_id_col: String,
    pub col_generation: Box<dyn Fn() -> Vec<String> + Send + Sync>,
    pub val_generation: Box<dyn Fn() -> Vec<Expr> + Send + Sync>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub use_decor: bool,
    pub parse: bool,
    pub in_memory: bool,
    pub prime: bool,
}

pub struct EdnaClient {
    pub schema: String,
    pub in_memory: bool,
    pub disguiser: disguise::Disguiser,
}

impl EdnaClient {
    /* EXTRA FXNS */
    pub fn clear_stats(&mut self) {
        warn!("EDNA: Clearing stats!");
        let mut stats = self.disguiser.stats.lock().unwrap();
        stats.clear();
        drop(stats);
    }

    /********************************
     * EDNA-APPLICATION API
     ********************************/
    pub fn new(
        prime: bool,
        batch: bool,
        host: &str,
        dbname: &str,
        schema: &str,
        in_memory: bool,
        keypool_size: usize,
        guise_gen: Arc<RwLock<GuiseGen>>,
    ) -> EdnaClient {
        init_db(prime, in_memory, host, dbname, schema);
        let url = format!("mysql://root:password@{}/{}", host, dbname);
        EdnaClient {
            schema: schema.to_string(),
            in_memory: in_memory,
            disguiser: disguise::Disguiser::new(&url, keypool_size, guise_gen, batch),
        }
    }

    //-----------------------------------------------------------------------------
    // Necessary to make Edna aware of all principals in the system
    // so Edna can link these to pseudoprincipals/do crypto stuff
    //-----------------------------------------------------------------------------
    pub fn register_principal(&mut self, uid: &UID) -> RsaPrivateKey {
        let mut db = self.get_conn().unwrap();
        let mut locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        let priv_key = locked_token_ctrler.register_principal(uid, false, &mut db, true);
        drop(locked_token_ctrler);
        priv_key
    }

    //-----------------------------------------------------------------------------
    // To register and end a disguise (and get the corresponding capabilities)
    //-----------------------------------------------------------------------------
    pub fn start_disguise(&self, _did: DID) {}

    pub fn end_disguise(&self) -> (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ) {
        let mut locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        let loc_caps = locked_token_ctrler.save_and_clear(&mut self.get_conn().unwrap());
        drop(locked_token_ctrler);
        loc_caps
    }

    //-----------------------------------------------------------------------------
    // Get all tokens of a particular disguise
    // returns all the diff tokens and all the ownership token blobs
    // Additional function to get and mark tokens revealed (if tokens are retrieved for the
    // purpose of reversal)
    //-----------------------------------------------------------------------------
    pub fn get_tokens_of_disguise(
        &self,
        did: DID,
        decrypt_cap: tokens::DecryptCap,
        diff_loc_caps: Vec<tokens::LocCap>,
        own_loc_caps: Vec<tokens::LocCap>,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        let mut diff_tokens = locked_token_ctrler.get_global_diff_tokens_of_disguise(did);
        let (dts, own_tokens) =
            locked_token_ctrler.get_user_tokens(did, &decrypt_cap, &diff_loc_caps, &own_loc_caps);
        diff_tokens.extend(dts.iter().cloned());
        drop(locked_token_ctrler);
        (
            diff_tokens
                .iter()
                .map(|wrapper| wrapper.token_data.clone())
                .collect(),
            own_tokens
                .iter()
                .map(|wrapper| wrapper.token_data.clone())
                .collect(),
        )
    }
    /*
    pub fn get_tokens_of_disguise_and_mark_revealed(
        &self,
        did: DID,
        decrypt_cap: tokens::DecryptCap,
        diff_loc_caps: Vec<tokens::LocCap>,
        own_loc_caps: Vec<tokens::LocCap>,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let mut locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        let mut diff_tokens = locked_token_ctrler.get_global_diff_tokens_of_disguise(did);
        let (dts, own_tokens) =
            locked_token_ctrler.get_user_tokens(did, &decrypt_cap, &diff_loc_caps, &own_loc_caps);
        diff_tokens.extend(dts.iter().cloned());

        for dwrapper in &diff_tokens {
            locked_token_ctrler.mark_diff_token_revealed(
                did,
                dwrapper,
                &decrypt_cap,
                &diff_loc_caps,
                &own_loc_caps,
            );
        }

        for owrapper in &own_tokens {
            locked_token_ctrler.mark_ownership_token_revealed(
                did,
                owrapper,
                &decrypt_cap,
                &own_loc_caps,
            );
        }

        drop(locked_token_ctrler);
        (
            diff_tokens
                .iter()
                .map(|wrapper| wrapper.token_data.clone())
                .collect(),
            own_tokens
                .iter()
                .map(|wrapper| wrapper.token_data.clone())
                .collect(),
        )
    }

    pub fn get_global_tokens(&self) -> Vec<Vec<u8>> {
        let locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        let global_diff_tokens = locked_token_ctrler.get_all_global_diff_tokens();
        drop(locked_token_ctrler);
        global_diff_tokens
            .iter()
            .map(|wrapper| wrapper.token_data.clone())
            .collect()
    }

    // TODO add API calls to remove/modify global tokens?
    */

    //-----------------------------------------------------------------------------
    // Save arbitrary diffs performed by the disguise for the purpose of later
    // restoring.
    //-----------------------------------------------------------------------------
    pub fn save_diff_token(&self, uid: UID, did: DID, data: Vec<u8>, is_global: bool) {
        let mut locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        let tok = tokens::new_generic_diff_token_wrapper(&uid, did, data, is_global);
        if is_global {
            locked_token_ctrler.insert_global_diff_token_wrapper(&tok);
        } else {
            locked_token_ctrler.insert_user_diff_token_wrapper(&tok);
        }
        drop(locked_token_ctrler);
    }

    //-----------------------------------------------------------------------------
    // Save information about decorrelation/ownership
    //-----------------------------------------------------------------------------
    pub fn remove_principal_metadata(&mut self, uid: &UID, did: DID) {
        let mut locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        locked_token_ctrler.mark_principal_to_be_removed(uid, did);
        drop(locked_token_ctrler);
    }

    pub fn save_pseudoprincipal_token(
        &self,
        did: DID,
        old_uid: UID,
        new_uid: UID,
        token_bytes: Vec<u8>,
    ) {
        let mut locked_token_ctrler = self.disguiser.token_ctrler.lock().unwrap();
        locked_token_ctrler.register_anon_principal(
            &old_uid,
            &new_uid,
            did,
            token_bytes,
            &mut self.get_conn().unwrap(),
        );
        drop(locked_token_ctrler);
    }

    pub fn create_new_pseudoprincipal(&mut self) -> (UID, Vec<RowVal>) {
        // ignore other metadata when application is handling the blobs being stored in tokens
        self.disguiser.create_new_pseudoprincipal()
    }

    pub fn get_pseudoprincipals(
        &self,
        decrypt_cap: tokens::DecryptCap,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Vec<UID> {
        self.disguiser
            .get_pseudoprincipals(&decrypt_cap, &ownership_loc_caps)
    }

    /**********************************************************************
     * If using the high-level spec API where Edna performs DB statements
     **********************************************************************/
    pub fn apply_disguise(
        &mut self,
        disguise: Arc<spec::Disguise>,
        decrypt_cap: tokens::DecryptCap,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<
        (
            HashMap<(UID, DID), tokens::LocCap>,
            HashMap<(UID, DID), tokens::LocCap>,
        ),
        mysql::Error,
    > {
        warn!("EDNA: APPLYING Disguise {}", disguise.clone().did);
        self.disguiser
            .apply(disguise.clone(), decrypt_cap, ownership_loc_caps)
    }

    pub fn reverse_disguise(
        &mut self,
        did: DID,
        decrypt_cap: tokens::DecryptCap,
        diff_loc_caps: Vec<tokens::LocCap>,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<(), mysql::Error> {
        warn!("EDNA: REVERSING Disguise {}", did);
        self.disguiser
            .reverse(did, decrypt_cap, diff_loc_caps, ownership_loc_caps)?;
        Ok(())
    }

    pub fn get_conn(&self) -> Result<mysql::PooledConn, mysql::Error> {
        self.disguiser.pool.get_conn()
    }

    pub fn get_stats(&self) -> Arc<Mutex<stats::QueryStat>> {
        self.disguiser.stats.clone()
    }
}

fn create_schema(db: &mut mysql::Conn, in_memory: bool, schema: &str) -> Result<(), mysql::Error> {
    db.query_drop("SET max_heap_table_size = 4294967295;")?;

    /* issue schema statements */
    let mut sql = String::new();
    let mut stmt = String::new();
    for line in schema.lines() {
        if line.starts_with("--") || line.is_empty() {
            continue;
        }
        if !sql.is_empty() {
            sql.push_str(" ");
            stmt.push_str(" ");
        }
        stmt.push_str(line);
        if stmt.ends_with(';') {
            // ignore query statements in schema
            if !stmt.to_lowercase().contains("create table") {
                continue;
            }
            let new_stmt = helpers::process_schema_stmt(&stmt, in_memory);
            warn!("create_schema issuing new_stmt {}", new_stmt);
            db.query_drop(new_stmt.to_string())?;
            stmt = String::new();
        }
    }
    Ok(())
}

pub fn init_db(prime: bool, in_memory: bool, host: &str, dbname: &str, schema: &str) {
    warn!("EDNA: Init db {}!", dbname);
    let url = format!("mysql://root:password@{}", host);
    let mut db = mysql::Conn::new(Opts::from_url(&url).unwrap()).unwrap();
    if prime {
        warn!("Priming database");
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", dbname))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", dbname))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", dbname)), true);
        create_schema(&mut db, in_memory, schema).unwrap();
    } else {
        assert_eq!(db.select_db(&format!("{}", dbname)), true);
    }
}
