use sql_parser::ast::{Expr, Ident, ObjectName, DataType, UnaryOperator, Value};
use std::*;
use std::collections::HashMap;
use std::cmp::Ordering;
use crate::{config, views};
use std::str::FromStr;
use msql_srv::{QueryResultWriter, Column, ColumnFlags};
use log::{debug};

/*******************************************
 * Column stuff
 *******************************************/
pub fn tablecolumn_matches_col(c: &views::TableColumnDef, col: &str) -> bool {
    debug!("matching {} or {} to {}", c.column.name, c.fullname, col);
    c.colname == col || c.fullname == col
}

pub fn get_col_index(col: &str, columns: &Vec<views::TableColumnDef>) -> Option<usize> {
    columns.iter().position(|c| tablecolumn_matches_col(c, col))
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

/*******************************************
 * Schema/MySql datatable stuff
 *******************************************/
pub fn get_ghosted_cols_of_datatable(ghost_tables: &HashMap<String, Vec<String>>, table_name: &ObjectName) -> Vec<String> {
    let mut res : Vec<String> = vec![];
    if let Some(colnames) = ghost_tables.get(&table_name.to_string()) {
        return colnames.clone();
    }
    vec![]
}

pub fn get_user_col_idents_of_datatable(cfg: &config::Config, table_name: &ObjectName) -> Vec<Ident> {
    let mut res : Vec<Ident> = vec![];
    let table_str = table_name.to_string();
    'dtloop: for dt in &cfg.data_tables {
        if table_str.ends_with(&dt.name) || table_str == dt.name {
            for uc in &dt.user_cols {
                res.push(Ident::new(uc));
            }
            break 'dtloop;
        }
    }
    res
}

pub fn datatable_contains_ghosts(cfg: &config::Config, table_name: &ObjectName) -> bool {
    let table_str = table_name.to_string();
    for dt in &cfg.data_tables {
        if table_str.ends_with(&dt.name) || table_str == dt.name {
            return true;
        }
    }
    false
}

pub fn process_schema_stmt(stmt: &str, in_memory: bool) -> String {
    // get rid of unsupported types
    debug!("helpers:{}", stmt);
    let mut new = stmt.replace(r"int unsigned", "int");
    if in_memory {
        new = new.replace(r"mediumtext", "varchar(255)");
        new = new.replace(r"tinytext", "varchar(255)");
        new = new.replace(r" text ", " varchar(255) ");
        new = new.replace(r" text,", " varchar(255),");
        new = new.replace(r" text)", " varchar(255))");
        new = new.replace(r"FULLTEXT", "");
        new = new.replace(r"fulltext", "");
        new = new.replace(r"InnoDB", "MEMORY");
    }

    // get rid of DEFAULT/etc. commands after query
    let mut end_index = new.len();
    if let Some(i) = new.find("DEFAULT CHARSET") {
        end_index = i; 
    } else if let Some(i) = new.find("default charset") {
        end_index = i;
    }
    new.truncate(end_index);
    if !new.ends_with(';') {
        new.push_str(";");
    }
    debug!("helpers new:{}", new);
    new
}


/***************************
 * IDENT STUFF
 ***************************/
pub fn expr_is_ghosted_col(expr:&Expr, ghosted_cols : &Vec<String>) -> bool {
    match expr {
        Expr::Identifier(_) => 
            ghosted_cols.iter().any(|uc| uc.ends_with(&expr.to_string())),
        Expr::QualifiedWildcard(ids) => 
            ghosted_cols.iter().any(|uc| uc.contains(&(Expr::Identifier(ids.to_vec())).to_string())),
        // currently don't handle nested expressions inside LHS of expr (be conservative!)
        _ => false,
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


/************************************ 
 * MYSQL HELPERS
 ************************************/
pub fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>) 
    -> Result<(), mysql::Error> 
{
    match rows {
        Ok(rows) => {
            let cols : Vec<_> = rows
                .columns()
                .as_ref()
                .into_iter()
                .map(|c| {
                    Column {
                    table : c.table_str().to_string(),
                    column : c.name_str().to_string(),
                    coltype : get_coltype(&c.column_type()),
                    colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                }
            })
            .collect();
            let mut writer = results.start(&cols)?;
            for row in rows {
                let vals = row.unwrap().unwrap();
                for v in vals {
                    writer.write_col(mysql_val_to_common_val(&v))?;
                }
                writer.end_row()?;
            }
            writer.finish()?;
        }
        Err(e) => {
            results.error(msql_srv::ErrorKind::ER_BAD_SLAVE, format!("{:?}", e).as_bytes())?;
        }
    }
    Ok(())
}

/// Convert a MySQL type to MySQL_svr type 
pub fn get_coltype(t: &mysql::consts::ColumnType) -> msql_srv::ColumnType {
    use msql_srv::ColumnType as ColumnType;
    match t {
        mysql::consts::ColumnType::MYSQL_TYPE_DECIMAL => ColumnType::MYSQL_TYPE_DECIMAL,
        mysql::consts::ColumnType::MYSQL_TYPE_TINY => ColumnType::MYSQL_TYPE_TINY,
        mysql::consts::ColumnType::MYSQL_TYPE_SHORT => ColumnType::MYSQL_TYPE_SHORT,
        mysql::consts::ColumnType::MYSQL_TYPE_LONG => ColumnType::MYSQL_TYPE_LONG,
        mysql::consts::ColumnType::MYSQL_TYPE_FLOAT => ColumnType::MYSQL_TYPE_FLOAT,
        mysql::consts::ColumnType::MYSQL_TYPE_DOUBLE => ColumnType::MYSQL_TYPE_DOUBLE,
        mysql::consts::ColumnType::MYSQL_TYPE_NULL => ColumnType::MYSQL_TYPE_NULL,
        mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => ColumnType::MYSQL_TYPE_TIMESTAMP,
        mysql::consts::ColumnType::MYSQL_TYPE_LONGLONG => ColumnType::MYSQL_TYPE_LONGLONG,
        mysql::consts::ColumnType::MYSQL_TYPE_INT24 => ColumnType::MYSQL_TYPE_INT24,
        mysql::consts::ColumnType::MYSQL_TYPE_DATE => ColumnType::MYSQL_TYPE_DATE,
        mysql::consts::ColumnType::MYSQL_TYPE_TIME => ColumnType::MYSQL_TYPE_TIME,
        mysql::consts::ColumnType::MYSQL_TYPE_DATETIME => ColumnType::MYSQL_TYPE_DATETIME,
        mysql::consts::ColumnType::MYSQL_TYPE_YEAR => ColumnType::MYSQL_TYPE_YEAR,
        mysql::consts::ColumnType::MYSQL_TYPE_NEWDATE => ColumnType::MYSQL_TYPE_NEWDATE,
        mysql::consts::ColumnType::MYSQL_TYPE_VARCHAR => ColumnType::MYSQL_TYPE_VARCHAR,
        mysql::consts::ColumnType::MYSQL_TYPE_BIT => ColumnType::MYSQL_TYPE_BIT,
        mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP2 => ColumnType::MYSQL_TYPE_TIMESTAMP2,
        mysql::consts::ColumnType::MYSQL_TYPE_DATETIME2 => ColumnType::MYSQL_TYPE_DATETIME2,
        mysql::consts::ColumnType::MYSQL_TYPE_TIME2 => ColumnType::MYSQL_TYPE_TIME2,
        mysql::consts::ColumnType::MYSQL_TYPE_JSON => ColumnType::MYSQL_TYPE_JSON,
        mysql::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        mysql::consts::ColumnType::MYSQL_TYPE_ENUM => ColumnType::MYSQL_TYPE_ENUM,
        mysql::consts::ColumnType::MYSQL_TYPE_SET => ColumnType::MYSQL_TYPE_SET,
        mysql::consts::ColumnType::MYSQL_TYPE_TINY_BLOB => ColumnType::MYSQL_TYPE_TINY_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_MEDIUM_BLOB => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_LONG_BLOB => ColumnType::MYSQL_TYPE_LONG_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_BLOB => ColumnType::MYSQL_TYPE_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_VAR_STRING => ColumnType::MYSQL_TYPE_VAR_STRING,
        mysql::consts::ColumnType::MYSQL_TYPE_STRING => ColumnType::MYSQL_TYPE_STRING,
        mysql::consts::ColumnType::MYSQL_TYPE_GEOMETRY => ColumnType::MYSQL_TYPE_GEOMETRY,
    }
}

pub fn mysql_val_to_common_val(val: &mysql::Value) -> mysql_common::value::Value {
    match val {
        mysql::Value::NULL => mysql_common::value::Value::NULL,
        mysql::Value::Bytes(bs) => mysql_common::value::Value::Bytes(bs.clone()),
        mysql::Value::Int(i) => mysql_common::value::Value::Int(*i),
        mysql::Value::UInt(i) => mysql_common::value::Value::UInt(*i),
        mysql::Value::Float(f) => mysql_common::value::Value::Double(*f),
        mysql::Value::Date(a,b,c,d,e,f,g) => mysql_common::value::Value::Date(*a,*b,*c,*d,*e,*f,*g),
        mysql::Value::Time(a,b,c,d,e,f) => mysql_common::value::Value::Time(*a,*b,*c,*d,*e,*f),
    }
}

pub fn mysql_val_to_parser_val(val: &mysql::Value) -> sql_parser::ast::Value {
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

pub fn mysql_val_to_u64(val: &mysql::Value) -> Result<u64, mysql::Error> {
    match val {
        mysql::Value::Bytes(bs) => {
            let res = str::from_utf8(&bs).unwrap();
            Ok(u64::from_str(res).unwrap())
        }
        mysql::Value::Int(i) => Ok(u64::from_str(&i.to_string()).unwrap()), // TODO fix?
        mysql::Value::UInt(i) => Ok(*i),
        _ => Err(mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, format!("value {:?} is not an int", val)))),
    }
}
