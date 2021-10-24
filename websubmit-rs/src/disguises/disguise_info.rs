use edna::disguise::*;
use edna::predicate::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use rand::prelude::*;

pub fn get_insert_guise_cols() -> Vec<String> {
    vec!["email".to_string(), "apikey".to_string(), "is_admin".to_string()]
}

pub fn get_insert_guise_vals() -> Vec<Expr> {
    let mut rng = rand::thread_rng();
    let gid : u64 = rng.gen();
    let email : u32 = rng.gen();
    vec![
        Expr::Value(Value::String(format!("{}@anon.com", email.to_string()))),
        Expr::Value(Value::String(gid.to_string())),
        Expr::Value(Value::Boolean(false)),
    ]
}

pub fn get_true_pred() -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::Bool(true)]]
}

pub fn get_eq_pred(col: &str, val: String) -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::ColValCmp {
        col: col.to_string(),
        val: val,
        op: BinaryOperator::Eq,
    }]]
}

pub fn get_guise_gen() -> Arc<RwLock<HashMap<String, GuiseGen>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "users".to_string(),
        GuiseGen {
            col_generation: Box::new(get_insert_guise_cols),
            val_generation: Box::new(get_insert_guise_vals),
        },
    );
    Arc::new(RwLock::new(hm))
}

pub fn get_table_info() -> Arc<RwLock<HashMap<String, TableInfo>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "users".to_string(),
        TableInfo {
            name: "users".to_string(),
            id_cols: vec!["apikey".to_string()],
            owner_cols: vec!["apikey".to_string()],
        },
    );
    hm.insert(
        "lectures".to_string(),
        TableInfo {
            name: "lectures".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec![],
        },
    );
    hm.insert(
        "questions".to_string(),
        TableInfo {
            name: "questions".to_string(),
            id_cols: vec!["lec".to_string(), "q".to_string()],
            owner_cols: vec![],
        },
    );
    hm.insert(
        "answers".to_string(),
        TableInfo {
            name: "answers".to_string(),
            id_cols: vec!["user".to_string(), "lec".to_string(), "q".to_string()],
            owner_cols: vec!["user".to_string()],
        },
    );
    Arc::new(RwLock::new(hm))
}
