extern crate crypto;
extern crate hex;
extern crate mysql;
extern crate ordered_float;

use log::{warn, error};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::*;
use std::sync::{Arc, Mutex};

pub mod disguise;
pub mod helpers;
pub mod history;
pub mod spec;
pub mod stats;
pub mod types;
pub mod vault;

const GUISE_ID_LB : u64 = 1<<5;

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub use_decor: bool,
    pub parse: bool,
    pub in_memory: bool,
    pub prime: bool,
}

pub fn create_schema(
    schema: &str,
    in_memory: bool,
    db: &mut mysql::Conn,
) -> Result<(), mysql::Error> {
    let mut txn = db.start_transaction(mysql::TxOpts::default())?;
    txn.query_drop("SET max_heap_table_size = 4294967295;")?;

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
            txn.query_drop(new_stmt.to_string())?;
            stmt = String::new();
        }
    }

    vault::create_vault(in_memory, &mut txn)?;
    history::create_history(in_memory, &mut txn)?;
    txn.commit()?;
    Ok(())
}

pub fn record_disguise(
    de: &history::DisguiseEntry,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<stats::QueryStat>>,
) -> Result<(), mysql::Error> {
    history::insert_disguise_history_entry(de, conn, stats);
    Ok(())
}

pub fn is_guise(
    id: u64,
) -> bool {
    id > GUISE_ID_LB
}
