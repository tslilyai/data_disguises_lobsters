use crate::helpers::*;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

fn ve_to_bytes(ve: &VaultEntry) -> Vec<u8> {
    let s = serde_json::to_string(ve).unwrap();
    s.as_bytes().to_vec()
}

pub fn ves_to_bytes(ves: &Vec<VaultEntry>) -> Vec<u8> {
    let s = serde_json::to_string(ves).unwrap();
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
