extern crate mysql;
extern crate ordered_float;

use log::{debug, warn};
use mysql::prelude::*;
use mysql::Opts;
use rsa::RsaPublicKey;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::*;

pub mod tokens;
pub mod disguise;
pub mod helpers;
pub mod predicate;
pub mod stats;

pub type DID = u64;
pub type UID = String;

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
    pub fn new(prime: bool, dbname: &str, schema: &str, in_memory: bool) -> EdnaClient {
        init_db(prime, in_memory, dbname, schema);
        let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname);
        EdnaClient {
            schema: schema.to_string(),
            in_memory: in_memory,
            disguiser: disguise::Disguiser::new(&url),
        }
    }

    pub fn register_principal(&mut self, uid: UID, email: String, pubkey: &RsaPublicKey) {
        self.disguiser.register_principal(&uid, email, pubkey);
    }

    pub fn has_ownership(
        &self,
        _uid: UID,
        _data_cap: tokens::DataCap,
        _loc_caps: Vec<tokens::LocCap>,
    ) -> bool {
        // TODO
        false
    }

    pub fn apply_disguise(
        &mut self,
        disguise: Arc<disguise::Disguise>,
        data_cap: tokens::DataCap,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<(HashMap<(UID, DID), tokens::LocCap>, HashMap<(UID, DID), tokens::LocCap>), mysql::Error> {
        warn!("EDNA: APPLYING Disguise {}", disguise.clone().did);
        self.disguiser.apply(disguise.clone(), data_cap, ownership_loc_caps)
    }

    pub fn reverse_disguise(
        &mut self,
        disguise: Arc<disguise::Disguise>,
        data_cap: tokens::DataCap,
        diff_loc_caps: Vec<tokens::LocCap>,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<(), mysql::Error> {
        warn!("EDNA: REVERSING Disguise {}", disguise.clone().did);
        self.disguiser
            .reverse(disguise.clone(), data_cap, diff_loc_caps, ownership_loc_caps)?;
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

pub fn init_db(prime: bool, in_memory: bool, dbname: &str, schema: &str) {
    warn!("EDNA: Init db {}!", dbname);
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
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
