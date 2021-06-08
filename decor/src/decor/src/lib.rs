extern crate crypto;
extern crate hex;
extern crate mysql;
extern crate ordered_float;
extern crate rusoto_core;
extern crate rusoto_s3;

use std::collections::{HashMap};
use log::{debug, warn};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::*;
use rusoto_s3::{
    S3Client
};

mod disguise;
mod helpers;
mod history;
mod s3client;
mod spec;
mod stats;
mod types;
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
    //pub s3: S3Client,
    pub disguises: HashMap<u64, types::Disguise>,
    pub schema: String,
    pub in_memory: bool,
    pub disguiser: disguise::Disguiser,
}

impl EdnaClient {
    pub fn new(url: &str, disguises: HashMap<u64, types::Disguise>, schema: &str, in_memory: bool) -> EdnaClient {
        EdnaClient {
            //s3: S3Client::new(),//new_s3client_with_credentials(region, access_key, secret_key),
            schema: schema.to_string(),
            in_memory: in_memory,
            disguises: disguises,
            disguiser: disguise::Disguiser::new(url),
        }
    }

    pub fn clear_stats(&mut self) {
        self.disguiser.stats.lock().unwrap().clear();
    }

    pub fn create_schema(
        &self,
    ) -> Result<(), mysql::Error> {
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

    pub fn apply_disguise(&mut self, user_id: Option<u64>, did: u64) -> Result<(), mysql::Error> {
        let disguise = self.disguises.get(&did).unwrap();
        self.disguiser.apply(&disguise, user_id)
    }
}
