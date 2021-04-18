use crate::datagen::*;
use decor::helpers::*;
use sql_parser::ast::*;

pub const ANON_PW: &'static str = "password123";

fn get_random_email() -> String {
    format!("anonymous{}@secret.mail", get_random_string())
}

pub fn is_guise(table_name: &str, id: u64, txn: &mut mysql::Transaction, stats: &mut stats::QueryStat) -> Result<bool, mysql::Error> {
    let is_guise = get_query_rows_txn(
        &select_1_statement(
            &table_name,
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new("isGuise".to_string())])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(id.to_string()))),
            }),
        ),
        txn,
        stats,
    )?;

    // if it is a guise, continue
    Ok(is_guise.is_empty())
}

pub fn get_guise_contact_info_cols() -> Vec<&'static str> {
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

pub fn get_guise_contact_info_vals() -> Vec<Expr> {
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
    for uid in 1..nusers+1 {
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
