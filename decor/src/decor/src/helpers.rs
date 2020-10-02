use mysql::prelude::*;
use sql_parser::ast::*;
use std::*;
use super::config;
use std::collections::HashMap;

pub fn process_schema(schema: &str) -> String {
    // get rid of unsupported types
    let mut new = schema.replace(r"int unsigned", "int");

    // get rid of ENGINE/etc. commands after query
    let mut end_index = new.len();
    if let Some(i) = new.find("ENGINE") {
        end_index = i; 
    } else if let Some(i) = new.find("engine") {
        end_index = i;
    }
    new.truncate(end_index);
    if !new.ends_with(';') {
        new.push_str(";");
    }
    new
}

pub fn mysql_val_to_parser_val(val: &mysql::Value) -> Value {
    match val {
        mysql::Value::NULL => Value::Null,
        mysql::Value::Bytes(bs) => {
            let res = str::from_utf8(&bs);
            match res {
                Err(_) => Value::String(String::new()),
                Ok(s) => Value::String(s.to_string()),
            }
        }
        mysql::Value::Int(i) => Value::Number(format!("{}", i)),
        mysql::Value::UInt(i) => Value::Number(format!("{}", i)),
        mysql::Value::Float(f) => Value::Number(format!("{}", f)),
        _ => unimplemented!("No sqlparser support for dates yet?")
        /*mysql::Date(u16, u8, u8, u8, u8, u8, u32),
        mysql::Time(bool, u32, u8, u8, u8, u32),8*/
    }
}

pub fn get_user_cols_of_datatable(cfg: &config::Config, table_name: &ObjectName) -> Vec<String> {
    let mut res : Vec<String> = vec![];
    let table_str = table_name.to_string();
    'dtloop: for dt in &cfg.data_tables {
        if table_str.ends_with(&dt.name) || table_str == dt.name {
            for uc in &dt.user_cols {
                let mut new_table_str = table_str.clone();
                new_table_str.push_str(".");
                new_table_str.push_str(uc);
                res.push(new_table_str);
            }
            break 'dtloop;
        }
    }
    res
}

pub fn get_uid2gids_for_uids(uids_to_match: Vec<Expr>, db: &mut mysql::Conn)
        -> Result<HashMap<Value, Vec<Expr>>, mysql::Error> 
{
    let get_gids_stmt_from_ghosts = Query::select(Select{
        distinct: true,
        projection: vec![
            SelectItem::Expr{
                expr: Expr::Identifier(string_to_objname(&super::GHOST_USER_COL).0),
                alias: None,
            },
            SelectItem::Expr{
                expr: Expr::Identifier(string_to_objname(&super::GHOST_ID_COL).0),
                alias: None,
            }
        ],
        from: vec![TableWithJoins{
            relation: TableFactor::Table{
                name: string_to_objname(&super::GHOST_TABLE_NAME),
                alias: None,
            },
            joins: vec![],
        }],
        selection: Some(Expr::InList{
            expr: Box::new(Expr::Identifier(string_to_idents(&super::GHOST_USER_COL))),
            list: uids_to_match,
            negated: false,
        }),
        group_by: vec![],
        having: None,
    });

    let mut uid_to_gids : HashMap<Value, Vec<Expr>> = HashMap::new();
    let res = db.query_iter(format!("{}", get_gids_stmt_from_ghosts.to_string()))?;
    for row in res {
        let vals : Vec<Value> = row.unwrap().unwrap()
            .iter()
            .map(|v| mysql_val_to_parser_val(&v))
            .collect();
        match uid_to_gids.get_mut(&vals[0]) {
            Some(gids) => (*gids).push(Expr::Value(vals[1].clone())),
            None => {
                uid_to_gids.insert(vals[0].clone(), vec![Expr::Value(vals[1].clone())]);
            }
        }
    }
    Ok(uid_to_gids)
}

/***************************
 * IDENT STUFF
 ***************************/
pub fn trim_quotes(s: &str) -> &str {
    let mut s = s;
    if s.ends_with('"') && s.starts_with('"') {
        s = &s[1..s.len() - 1]
    } 
    if s.ends_with("'") && s.starts_with("'") {
        s = &s[1..s.len() - 1]
    } 
    s
}

pub fn string_to_idents(s: &str) -> Vec<Ident> {
    s.split(".")
        .into_iter()
        .map(|i| Ident::new(trim_quotes(i)))
        .collect()
}

pub fn string_to_objname(s: &str) -> ObjectName {
    let idents = string_to_idents(s);
    ObjectName(idents)
}

pub fn str_subset_of_idents(dt: &str, ids: &Vec<Ident>) -> Option<(usize, usize)> {
    let dt_split : Vec<Ident> = dt.split(".")
        .map(|i| Ident::new(i))
        .collect();
    idents_subset_of_idents(&dt_split, ids)
 }

// end exclusive
pub fn str_ident_match(shorts: &str, longs: &str) -> bool {
    let mut i = 0;
    let mut j = 0;
    let shortvs : Vec<&str> = shorts.split(".").collect();
    let longvs : Vec<&str> = longs.split(".").collect();
    while j < longvs.len() {
        if i < shortvs.len() {
            if shortvs[i] == longvs[j] {
                i+=1;
            } else {
                // reset comparison from beginning of dt
                i = 0; 
            }
            j+=1;
        } else {
            break;
        }
    }
    if i == shortvs.len() {
        return true;
    } 
    false
}

// end exclusive
pub fn idents_subset_of_idents(id1: &Vec<Ident>, id2: &Vec<Ident>) -> Option<(usize, usize)> {
    let mut i = 0;
    let mut j = 0;
    while j < id2.len() {
        if i < id1.len() {
            if id1[i] == id2[j] {
                i+=1;
            } else {
                // reset comparison from beginning of dt
                i = 0; 
            }
            j+=1;
        } else {
            break;
        }
    }
    if i == id1.len() {
        return Some((j-i, j));
    } 
    None
}


