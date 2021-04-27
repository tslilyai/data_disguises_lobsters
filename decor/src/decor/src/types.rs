use serde::{Deserialize, Serialize};
use sql_parser::ast::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColFormat {
    Quoted,
    NonQuoted
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
    // does this FK point to a owning user of this referencer?
    // i.e., do we decorrelate + put it in the corresponding vault?
    pub is_owner: bool,
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
    pub used_cols: Vec<ColumnModification>,
    pub used_fks: Vec<FK>,
}

pub struct GuiseInfo {
    pub name: String,
    pub ids: Vec<String>,
    pub col_generation: Box<dyn Fn() -> Vec<&'static str>>,
    pub val_generation: Box<dyn Fn() -> Vec<Expr>>,
}

pub struct Disguise {
    pub disguise_id: u64,
    pub update_names: Vec<TableInfo>,
    pub remove_names: Vec<TableInfo>,
    pub guise_info: GuiseInfo, 
}
