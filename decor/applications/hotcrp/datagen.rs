use crate::*;
use decor::helpers::*;
use rand::{distributions::Alphanumeric, Rng};
use sql_parser::ast::*;

fn get_random_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

fn get_random_email() -> String {
    format!("anonymous{}@secret.mail", get_random_string())
}

fn get_contact_info_cols() -> Vec<&'static str> {
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
    ]
}

fn get_contact_info_vals() -> Vec<Expr> {
    vec![
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

pub fn insert_contact_info(n: usize, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    if n <= 0 {
        return Ok(());
    }

    let mut new_ci = vec![];
    let fk_cols = get_contact_info_cols();
    for _ in 0..n {
        new_ci.push(get_contact_info_vals());
    }
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
