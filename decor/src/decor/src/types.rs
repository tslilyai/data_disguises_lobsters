use serde::{Deserialize, Serialize};
use sql_parser::ast::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColFormat {
    Quoted,
    NonQuoted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowVal {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FK {
    pub referencer_col: String,
    pub fk_name: String,
    pub fk_col: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableColumns {
    pub name: String,
    pub cols: Vec<String>,
    pub colformats: Vec<ColFormat>,
}

pub struct ColumnModification {
    pub col: String,
    pub satisfies_modification: Box<dyn Fn(&str) -> bool>,
    pub generate_modified_value: Box<dyn Fn() -> String>,
}

pub struct TableInfo {
    pub name: String,
    pub id_cols: Vec<String>,
    // which columns are modified and how they should be modified
    pub cols_to_update: Vec<ColumnModification>,
    // which columns should refer to guises after updates.
    // if a disguise userID is specified, only decorrelate FKs == userID
    pub fks_to_decor: Vec<FK>,
}

pub struct GuiseInfo {
    pub name: String,
    pub id_col: String, // XXX assume there's only one id col for a guise
    pub col_generation: Box<dyn Fn() -> Vec<&'static str>>,
    pub val_generation: Box<dyn Fn() -> Vec<Expr>>,
}

pub struct Disguise {
    pub user_id: Option<u64>,
    pub disguise_id: u64,
    pub update_names: Vec<TableInfo>,
    pub remove_names: Vec<TableInfo>,
    pub guise_info: GuiseInfo,
}
