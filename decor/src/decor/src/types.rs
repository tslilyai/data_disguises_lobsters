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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    // name of column
    pub col: String,
    // how to generate a modified value
    pub generate_modified_value: Box<dyn Fn(&str) -> String>,
    // post-application check that value satisfies modification
    pub satisfies_modification: Box<dyn Fn(&str) -> bool>,
}

pub enum ModificationType {
    Remove{pred: Option<Expr>},
    Modify{pred: Option<Expr>, colmod: ColumnModification},
    Decor{pred: Option<Expr>, fk: FK},
}

pub struct TableDisguise {
    pub name: String,
    pub id_cols: Vec<String>,
    pub modifications: Vec<ModificationType>,
}

pub struct GuiseInfo {
    pub name: String,
    pub id_col: String, // XXX assume there's only one id col for a guise
    pub col_generation: Box<dyn Fn() -> Vec<&'static str>>,
    pub val_generation: Box<dyn Fn() -> Vec<Expr>>,
}

pub struct Disguise {
    pub disguise_id: u64,
    pub tables: Vec<TableDisguise>,
    pub guise_info: GuiseInfo,
}
