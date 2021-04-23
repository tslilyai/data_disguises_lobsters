use crate::datagen::*;
use decor::helpers::*;
use decor::stats::*;
use sql_parser::ast::*;

pub const ANON_PW: &'static str = "password123";

fn get_random_email() -> String {
    format!("anonymous{}@secret.mail", get_random_string())
}

pub fn get_insert_guise_contact_info_cols() -> Vec<&'static str> {
    vec![
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
        "isGuise",
    ]
}

pub fn get_insert_guise_contact_info_vals() -> Vec<Expr> {
    vec![
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
        Expr::Value(Value::Boolean(true)),
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
        "isGuise",
    ]
}

pub fn get_contact_info_vals(uid: usize) -> Vec<Expr> {
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
        Expr::Value(Value::Boolean(false)),
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
