extern crate mysql;
extern crate ordered_float;

use log::{debug, warn};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::sync::{Arc, Mutex};
use rsa::{RsaPublicKey};
use std::*;

pub mod disguise;
pub mod helpers;
pub mod predicate;
pub mod stats;
pub mod tokens;

pub type DID = u64;
pub type UID = u64;

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
    pub fn new(prime: bool, dbname: &str, schema: &str, in_memory: bool) -> EdnaClient {
        init_db(prime, in_memory, dbname, schema);
        let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname);
        EdnaClient {
            schema: schema.to_string(),
            in_memory: in_memory,
            disguiser: disguise::Disguiser::new(&url),
        }
    }

    pub fn clear_stats(&mut self) {
        warn!("EDNA: Clearing stats!");
        let mut stats = self.disguiser.stats.lock().unwrap();
        stats.clear();
        drop(stats);
    }

    pub fn register_principal(&mut self, uid: u64, email: String, pubkey: &RsaPublicKey) {
        self.disguiser.register_principal(uid, email, pubkey);
    }

    // XXX get rid of this
    pub fn get_pseudoprincipal_enc_privkeys(
        &mut self,
        uid: UID,
    ) -> Vec<tokens::EncPrivKeyToken> {
        self.disguiser.get_pseudoprincipal_enc_privkeys(uid)
    }

    pub fn get_capability(
        &mut self,
        uid: UID,
        did: DID,
    ) -> Option<tokens::Capability> {
        self.disguiser.get_capability(uid, did)
    }

    pub fn get_enc_token_symkeys_of_capabilities_and_pseudoprincipals(
        &mut self,
        caps: Vec<tokens::Capability>,
        pseudouids: Vec<UID>,
    ) -> Vec<(tokens::EncSymKey, tokens::Capability)> {
        self.disguiser.get_enc_token_symkeys_with_capabilities_and_pseudoprincipals(caps, pseudouids)
    }

    pub fn get_tokens_of_disguise_keys(
        &mut self,
        keys: Vec<(tokens::SymKey, tokens::Capability)>,
        global_tokens_of: Vec<(DID,UID)>,
    ) -> Vec<tokens::Token> {
        self.disguiser.get_tokens_of_disguise_keys(keys, global_tokens_of, false)
    }

    pub fn apply_disguise(
        // TODO filter all global tokens?????
        &mut self,
        disguise: Arc<disguise::Disguise>,
        keys: Vec<(tokens::SymKey, tokens::Capability)>,
        global_tokens_of: Vec<(DID,UID)>,
    ) -> Result<(), mysql::Error> {
        let tokens = self.disguiser.get_tokens_of_disguise_keys(keys, global_tokens_of, true);
        self.disguiser.apply(disguise.clone(), tokens)?;
        warn!("EDNA: APPLIED Disguise {}", disguise.clone().did);
        Ok(())
    }

    pub fn reverse_disguise(
        &mut self,
        disguise: Arc<disguise::Disguise>,
        keys: Vec<(tokens::SymKey, tokens::Capability)>,
        global_tokens_of: Vec<(DID,UID)>,
    ) -> Result<(), mysql::Error> {
        let tokens = self.disguiser.get_tokens_of_disguise_keys(keys, global_tokens_of, true);
        self.disguiser.reverse(disguise.clone(), tokens)?;
        warn!("EDNA: REVERSED Disguise {}", disguise.clone().did);
        Ok(())
    }

    pub fn get_conn(&mut self) -> Result<mysql::PooledConn, mysql::Error> {
        self.disguiser.pool.get_conn()
    }

    pub fn get_stats(&mut self) -> Arc<Mutex<stats::QueryStat>> {
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
            let new_stmt = helpers::process_schema_stmt(&stmt, in_memory);
            warn!("create_schema issuing new_stmt {}", new_stmt);
            db.query_drop(new_stmt.to_string())?;
            stmt = String::new();
        }
    }
    Ok(())
}

pub fn init_db(prime: bool, in_memory: bool, dbname: &str, schema: &str) {
    warn!("EDNA: Init db!");
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
    let mut db = mysql::Conn::new(&url).unwrap();
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
