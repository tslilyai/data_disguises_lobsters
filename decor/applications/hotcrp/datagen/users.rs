use crate::datagen::*;
use decor::helpers::*;
use sql_parser::ast::*;
use std::sync::atomic::{AtomicU64, Ordering};
   
static GUISE_ID : AtomicU64 = AtomicU64::new(1<<5);

pub const ANON_PW: &'static str = "password123";

pub fn get_random_email() -> String {
    format!("anonymous{}@secret.mail", get_random_string())
}

pub fn get_insert_guise_contact_info_cols() -> Vec<String> {
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

pub fn get_insert_guise_contact_info_vals() -> Vec<Expr> {
    GUISE_ID.fetch_add(1, Ordering::SeqCst);
    vec![
        Expr::Value(Value::Number(GUISE_ID.load(Ordering::SeqCst).to_string())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(get_random_email())),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(ANON_PW.to_string())),
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

pub fn get_contact_info_cols() -> Vec<&'static str> {
    vec![
        "contactId",
        "firstName",
        "lastName",
        "unaccentedName",
        "email",
        "preferredEmail",
        "affiliation",
        "phone",
        "country",
        "password",
        "passwordTime",
        "passwordUseTime",
        "collaborators",
        "updateTime",
        "lastLogin",
        "defaultWatch",
        "roles",
        "disabled",
        "contactTags",
        "data",
    ]
}

pub fn get_contact_info_vals(uid: usize) -> Vec<Expr> {
    assert!((uid as u64) < GUISE_ID.load(Ordering::SeqCst));
    vec![
        Expr::Value(Value::Number(uid.to_string())),
        Expr::Value(Value::String(get_random_string())),
        Expr::Value(Value::String(get_random_string())),
        Expr::Value(Value::String(get_random_string())),
        Expr::Value(Value::String(get_random_email())),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String("password".to_string())),
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

pub fn insert_users(nusers: usize, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    // insert users
    let mut new_ci = vec![];
    for uid in 1..nusers + 1 {
        new_ci.push(get_contact_info_vals(uid));
    }
    let fk_cols = get_contact_info_cols();
    get_query_rows_db(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("ContactInfo"),
            columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
            source: InsertSource::Query(Box::new(values_query(new_ci))),
        }),
        db,
    )?;
    Ok(())
}
