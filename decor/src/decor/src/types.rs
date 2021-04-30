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
pub struct TableColumns {
    pub name: String,
    pub cols: Vec<String>,
    pub colformats: Vec<ColFormat>,
}

pub enum Transform {
    Remove {
        pred: Option<Expr>,
    },
    Modify {
        pred: Option<Expr>,
        // name of column
        col: String,
        // how to generate a modified value
        generate_modified_value: Box<dyn Fn(&str) -> String>,
        // post-application check that value satisfies modification
        satisfies_modification: Box<dyn Fn(&str) -> bool>,
    },
    Decor {
        pred: Option<Expr>,
        referencer_col: String,
        fk_name: String,
        fk_col: String,
    },
}

pub struct TableDisguise {
    pub name: String,
    pub id_cols: Vec<String>,
    pub transforms: Vec<Transform>,
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
