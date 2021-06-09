extern crate crypto;
extern crate hex;
extern crate mysql;
extern crate ordered_float;
extern crate rusoto_core;
extern crate rusoto_s3;

use log::{debug, warn};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::*;

mod disguise;
pub mod helpers;
mod history;
mod uvclient;
pub mod stats;
pub mod types;
mod vault;

const GUISE_ID_LB: u64 = 1 << 5;

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub use_decor: bool,
    pub parse: bool,
    pub in_memory: bool,
    pub prime: bool,
}

pub struct EdnaClient {
    pub uvclient : uvclient::UVClient,
    pub schema: String,
    pub in_memory: bool,
    pub disguiser: disguise::Disguiser,
}

impl EdnaClient {
    pub fn new(url: &str, schema: &str, in_memory: bool) -> EdnaClient {
        EdnaClient {
            uvclient: uvclient::UVClient::new("", "", ""),
            schema: schema.to_string(),
            in_memory: in_memory,
            disguiser: disguise::Disguiser::new(url),
        }
    }

    pub fn init_db(&mut self, prime: bool, dbname: &str) {
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
        } else {
            assert_eq!(db.select_db(&format!("{}", dbname)), true);
        }
    }

    pub fn clear_stats(&mut self) {
        warn!("EDNA: Clearing stats!");
        let mut stats = self.disguiser.stats.lock().unwrap();
        stats.clear();
        drop(stats);
    }

    pub fn create_schema(&self) -> Result<(), mysql::Error> {
        let mut conn = self.disguiser.pool.get_conn()?;
        conn.query_drop("SET max_heap_table_size = 4294967295;")?;

        /* issue schema statements */
        let mut sql = String::new();
        let mut stmt = String::new();
        for line in self.schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !sql.is_empty() {
                sql.push_str(" ");
                stmt.push_str(" ");
            }
            stmt.push_str(line);
            if stmt.ends_with(';') {
                let new_stmt = helpers::process_schema_stmt(&stmt, self.in_memory);
                warn!("create_schema issuing new_stmt {}", new_stmt);
                conn.query_drop(new_stmt.to_string())?;
                stmt = String::new();
            }
        }

        vault::create_vault(self.in_memory, &mut conn)?;
        history::create_history(self.in_memory, &mut conn)?;
        Ok(())
    }

    pub fn apply_disguise(
        &mut self,
        user_id: Option<u64>,
        disguise: Arc<types::Disguise>,
    ) -> Result<(), mysql::Error> {
        self.disguiser.apply(disguise.clone(), user_id)?;
        warn!("EDNA: Applied Disguise {}", disguise.clone().disguise_id);
        Ok(())
    }

    pub fn get_conn(&mut self) -> Result<mysql::PooledConn, mysql::Error> {
        self.disguiser.pool.get_conn()
    }

    pub fn get_stats(&mut self) -> Arc<Mutex<stats::QueryStat>> {
        self.disguiser.stats.clone()
    }
}
