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
    pub used_cols: Vec<ColumnModification>,
    // which columns should refer to guises; if a userID is specified, only those FKs that were the
    // userID should be set to guiseIDs; the others simply cannot be equal to the userID
    pub used_fks: Vec<FK>,
}

pub struct GuiseInfo {
    pub name: String,
    // assume there's only one id col for a guise
    pub id: String,
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
