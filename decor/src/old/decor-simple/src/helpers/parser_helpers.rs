use sql_parser::ast::{Expr, Ident, ObjectName, DataType, UnaryOperator, Value};
use std::*;
use std::cmp::Ordering;
use std::str::FromStr;
use rand;
use log::{debug};
use crate::{views};

/*****************************************
 * Parser helpers 
 ****************************************/
// returns if the first value is larger than the second
pub fn parser_vals_cmp(v1: &sql_parser::ast::Value, v2: &sql_parser::ast::Value) -> cmp::Ordering {
    let res : cmp::Ordering;
    debug!("comparing {:?} =? {:?}", v1, v2);
    match (v1, v2) {
        (Value::Number(i1), Value::Number(i2)) => res = f64::from_str(i1).unwrap().partial_cmp(&f64::from_str(i2).unwrap()).unwrap(),
        (Value::String(i1), Value::Number(i2)) => res = f64::from_str(i1).unwrap().partial_cmp(&f64::from_str(i2).unwrap()).unwrap(),
        (Value::Number(i1), Value::String(i2)) => res = f64::from_str(i1).unwrap().partial_cmp(&f64::from_str(i2).unwrap()).unwrap(),
        (Value::String(i1), Value::String(i2)) => res = i1.cmp(i2),
        (Value::Null, Value::Null) => res = Ordering::Equal,
        (_, Value::Null) => res = Ordering::Greater,
        (Value::Null, _) => res = Ordering::Less,
        _ => unimplemented!("value not comparable! {:?} and {:?}", v1, v2),
    }
    debug!("comparing {:?} =? {:?} : {:?}", v1, v2, res);
    res
}

pub fn plus_parser_vals(v1: &sql_parser::ast::Value, v2: &sql_parser::ast::Value) -> sql_parser::ast::Value {
    Value::Number((parser_val_to_f64(v1) + parser_val_to_f64(v2)).to_string())
}

pub fn minus_parser_vals(v1: &sql_parser::ast::Value, v2: &sql_parser::ast::Value) -> sql_parser::ast::Value {
    Value::Number((parser_val_to_f64(v1) - parser_val_to_f64(v2)).to_string())
}

pub fn get_computed_parser_val_with(base_val: &Value, f: &Box<dyn Fn(&str) -> String>) -> Value {
    match base_val {
        Value::Number(i) => Value::Number(f(i)),
        Value::String(i) => Value::String(f(i)),
        _ => unimplemented!("value not supported ! {}", base_val),
    }
}

pub fn get_default_parser_val_with(base_val: &Value, val: &str) -> Value {
    match base_val {
        Value::Number(_i) => Value::Number(val.to_string()),
        Value::String(_i) => Value::String(val.to_string()),
        Value::Null => Value::String(val.to_string()),
        _ => unimplemented!("value not supported ! {}", base_val),
    }
}

pub fn get_random_parser_val_from(val: &Value) -> Value {
    match val {
        Value::Number(_i) => Value::Number(rand::random::<u32>().to_string()),
        Value::String(_i) => Value::String(rand::random::<u64>().to_string()),
        Value::Null => Value::Number(rand::random::<u64>().to_string()),
        _ => unimplemented!("value not supported ! {}", val),
    }
}

pub fn parser_val_to_f64(val: &sql_parser::ast::Value) -> f64 {
    match val {
        Value::Number(i) => f64::from_str(i).unwrap(),
        Value::String(i) => f64::from_str(i).unwrap(),
        _ => unimplemented!("value not a number! {}", val),
    }
}

pub fn parser_val_to_u64(val: &sql_parser::ast::Value) -> u64 {
    match val {
        Value::Number(i) => u64::from_str(i).unwrap(),
        Value::String(i) => u64::from_str(i).unwrap(),
        _ => unimplemented!("value not a number! {}", val),
    }
}

pub fn parser_val_to_u64_opt(val: &sql_parser::ast::Value) -> Option<u64> {
    match val {
        Value::Number(i) => Some(u64::from_str(i).unwrap()),
        Value::String(i) => Some(u64::from_str(i).unwrap()),
        _ => None,
    }
}

pub fn parser_expr_to_u64(val: &Expr) -> Result<u64, mysql::Error> {
    match val {
        Expr::Value(Value::Number(i)) => Ok(u64::from_str(i).unwrap()),
        Expr::Value(Value::String(i)) => {
            match u64::from_str(i) {
                Ok(v) => Ok(v),
                Err(_e) => 
                    Err(mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, format!("expr {:?} is not an int", val)))),
            }
        }
        _ => Err(mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, format!("expr {:?} is not an int", val)))),
    }
}

/// Convert a parser type to MySQL_svr type 
pub fn get_parser_coltype(t: &DataType) -> msql_srv::ColumnType {
    use msql_srv::ColumnType as ColumnType;
    match t {
        DataType::Decimal(..) => ColumnType::MYSQL_TYPE_DECIMAL,
        DataType::Float(..) => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Double => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Timestamp => ColumnType::MYSQL_TYPE_TIMESTAMP,
        DataType::BigInt => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::SmallInt => ColumnType::MYSQL_TYPE_INT24,
        DataType::TinyInt(..) => ColumnType::MYSQL_TYPE_INT24,
        DataType::Int => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::Date => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time => ColumnType::MYSQL_TYPE_TIME,
        DataType::Varchar(..) => ColumnType::MYSQL_TYPE_VARCHAR,
        DataType::Jsonb => ColumnType::MYSQL_TYPE_JSON,
        DataType::Blob(..) => ColumnType::MYSQL_TYPE_BLOB,
        DataType::Char(..) => ColumnType::MYSQL_TYPE_STRING,
        DataType::Boolean => ColumnType::MYSQL_TYPE_BIT,
        DataType::DateTime => ColumnType::MYSQL_TYPE_DATETIME,
        _ => unimplemented!("not a valid data type {:?}", t),
    }
}

pub fn parser_val_to_common_val(val: &sql_parser::ast::Value) -> mysql_common::value::Value {
    match val {
        Value::Null => mysql_common::value::Value::NULL,
        Value::String(s) => mysql_common::value::Value::Bytes(s.as_bytes().to_vec()),
        Value::HexString(s) => mysql_common::value::Value::Bytes(hex::decode(s).unwrap()),
        Value::Number(i) => {
            if !i.contains('.') {
                mysql_common::value::Value::Int(i64::from_str(i).unwrap())
            } else {
                mysql_common::value::Value::Double(f64::from_str(i).unwrap())
            }
        }
        Value::Boolean(b) => {
            let bit = match b {
                true => 1,
                false => 0,
            };
            mysql_common::value::Value::Int(bit)
        }
        _ => unimplemented!("Value not supported: {}", val),
    }
}

pub fn string_vals_to_parser_vals(valstrs: &Vec<String>, columns: &Vec<views::TableColumnDef>) -> Vec<Value> {
    let mut valsvec = vec![];
    for ci in 0..columns.len() {
        let valstr = trim_quotes(&valstrs[ci]);
        if valstr == "NULL" {
            valsvec.push(Value::Null);
        } else {
            valsvec.push(
                match columns[ci].column.data_type {
                    DataType::Decimal(..) 
                        | DataType::Float(..)
                        | DataType::Double 
                        | DataType::BigInt 
                        | DataType::SmallInt
                        | DataType::TinyInt(..) 
                        | DataType::Int => Value::Number(valstr.to_string()),
                    DataType::Timestamp 
                        | DataType::Date 
                        | DataType::DateTime
                        | DataType::Time 
                        | DataType::Varchar(..) 
                        | DataType::Blob(..) 
                        | DataType::Char(..) => Value::String(trim_quotes(&valstr).to_string()),
                    DataType::Boolean => Value::Boolean(valstr == "1"),
                    _ => unimplemented!("type not supported yet {:?}", columns[ci].column.data_type)
                });
        }
    }
    valsvec
}

/***************************
 * IDENT STUFF
 ***************************/

pub fn trim_quotes(s: &str) -> &str {
    let mut s = s.trim_matches('\'');
    s = s.trim_matches('\"');
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


/***************************
 * EXPR STUFF
 ***************************/
// return table name and optionally column if not wildcard
pub fn expr_to_col(e: &Expr) -> (String, String) {
    //debug!("expr_to_col: {:?}", e);
    match e {
        // only support form column or table.column
        Expr::Identifier(ids) => {
            if ids.len() > 2 || ids.len() < 1 {
                unimplemented!("expr needs to be of form table.column {}", e);
            }
            if ids.len() == 2 {
                return (ids[0].to_string(), ids[1].to_string());
            }
            return ("".to_string(), ids[0].to_string());
        }
        _ => unimplemented!("expr_to_col {} not supported", e),
    }
}

pub fn expr_to_guise_parent_key(expr:&Expr, guiseed_cols : &Vec<(String, String)>) -> Option<(String, String)> {
    match expr {
        Expr::Identifier(ids) => {
            let col = ids[ids.len()-1].to_string();
            if let Some(i) = guiseed_cols.iter().position(|(gc, _pc)| *gc == col) {
                Some(guiseed_cols[i].clone())
            } else {
                None
            }
        }
        _ => unimplemented!("Expr is not a col {}", expr),
    } 
}

pub fn expr_is_col(expr:&Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::QualifiedWildcard(_) => true,
        _ => false,
    } 
}

pub fn expr_is_value(expr:&Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        _ => false,
    } 
}

pub fn lhs_expr_to_name(left: &Expr) -> String {
    match left {
        Expr::Identifier(_) => {
            let (tab, mut col) = expr_to_col(&left);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            col
        }
        _ => unimplemented!("Bad lhs {}", left),
    }
}

pub fn rhs_expr_to_name_or_value(right: &Expr) -> (Option<String>, Option<Value>) {
    let mut rval = None;
    let mut rname = None;
    match right {
        Expr::Identifier(_) => {
            let (tab, mut col) = expr_to_col(&right);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            rname = Some(col);
        }
        Expr::Value(val) => {
            rval = Some(val.clone());
        }
        Expr::UnaryOp{op, expr} => {
            if let Expr::Value(ref val) = **expr {
                match op {
                    UnaryOperator::Minus => {
                        let n = -1.0 * parser_val_to_f64(&val);
                        rval = Some(Value::Number(n.to_string()));
                    }
                    _ => unimplemented!("Unary op not supported! {:?}", expr),
                }
            } else {
                unimplemented!("Unary op not supported! {:?}", expr);
            }
        }
        _ => unimplemented!("Bad rhs? {}", right),
    }
    (rname, rval)
}
