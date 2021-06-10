use crate::helpers::*;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::sync::atomic::{AtomicU64};

pub const VAULT_TABLE: &'static str = "VaultTable";
pub const INSERT_GUISE: u64 = 0;
pub const DELETE_GUISE: u64 = 1;
pub const UPDATE_GUISE: u64 = 2;

pub static VAULT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct VaultEntry {
    pub vault_id: u64,
    pub disguise_id: u64,
    pub user_id: u64,
    pub guise_name: String,
    pub guise_id_cols: Vec<String>,
    pub guise_ids: Vec<String>,
    pub referencer_name: String,
    pub update_type: u64,
    pub modified_cols: Vec<String>,
    pub old_value: Vec<RowVal>,
    pub new_value: Vec<RowVal>,
    pub reverses: Option<u64>,
}
pub fn ve_to_bytes(ve: &VaultEntry) -> Vec<u8> {
    let s = serde_json::to_string(ve).unwrap();
    s.as_bytes().to_vec()
}

pub fn vec_to_expr<T: Serialize>(vs: &Vec<T>) -> Expr {
    if vs.is_empty() {
        Expr::Value(Value::Null)
    } else {
        let serialized = serde_json::to_string(&vs).unwrap();
        Expr::Value(Value::String(serialized))
    }
}
