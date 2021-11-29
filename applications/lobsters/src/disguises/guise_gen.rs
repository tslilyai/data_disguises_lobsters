use edna::GuiseGen;
use rand::Rng;
use sql_parser::ast::*;
use std::sync::{Arc, RwLock};

pub fn get_insert_guise_cols() -> Vec<String> {
    vec![
        "contactId".to_string(),
        "firstName".to_string(),
        "lastName".to_string(),
        "unaccentedName".to_string(),
        "email".to_string(),
        "preferredEmail".to_string(),
        "affiliation".to_string(),
        "phone".to_string(),
        "country".to_string(),
        "password".to_string(),
        "passwordTime".to_string(),
        "passwordUseTime".to_string(),
        "collaborators".to_string(),
        "updateTime".to_string(),
        "lastLogin".to_string(),
        "defaultWatch".to_string(),
        "roles".to_string(),
        "disabled".to_string(),
        "contactTags".to_string(),
        "data".to_string(),
    ]
}

pub fn get_insert_guise_vals() -> Vec<Expr> {
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen::<u64>();
    let email: String = format!("{}@anon.com", gid);
    let pass: String = format!("{}pass", gid);
    vec![
        Expr::Value(Value::Number(gid.to_string())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(email)),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(pass)),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(2.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
    ]
}

pub fn get_guise_gen() -> Arc<RwLock<GuiseGen>> {
    Arc::new(RwLock::new(GuiseGen {
        guise_name: "ContactInfo".to_string(),
        guise_id_col: "ContactId".to_string(),
        col_generation: Box::new(get_insert_guise_cols),
        val_generation: Box::new(get_insert_guise_vals),
    }))
}

