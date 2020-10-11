use sql_parser::ast::*;
use std::*;
use super::config;

pub fn process_schema_stmt(stmt: &str) -> String {
    // get rid of unsupported types
    let mut new = stmt.replace(r"int unsigned", "int");

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


