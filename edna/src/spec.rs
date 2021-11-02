use crate::predicate::*;
use crate::*;
use std::sync::{Arc, RwLock};

pub enum TransformArgs {
    Remove,
    Modify {
        // name of column
        col: String,
        // how to generate a modified value
        generate_modified_value: Box<dyn Fn(&str) -> String + Send + Sync>,
        // post-application check that value satisfies modification
        satisfies_modification: Box<dyn Fn(&str) -> bool + Send + Sync>,
    },
    Decor {
        fk_col: String,
        fk_name: String,
    },
}

#[derive(Clone)]
pub struct ObjectTransformation {
    pub pred: Vec<Vec<PredClause>>,
    pub trans: Arc<RwLock<TransformArgs>>,
    pub global: bool,
}

#[derive(Clone)]
pub struct TableInfo {
    pub name: String,
    pub id_cols: Vec<String>,
    pub owner_cols: Vec<String>,
}

pub struct Disguise {
    pub did: u64,
    pub user: String,
    pub table_disguises: HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>>,
    pub table_info: Arc<RwLock<HashMap<String, TableInfo>>>,
}


