use crate::{policy, views};
use crate::ghosts_map::GHOST_ID_START;
use sql_parser::ast::{Expr, Ident, ObjectName, DataType, UnaryOperator, Value};
use std::*;
use std::cmp::Ordering;
use std::str::FromStr;
use rand;
use msql_srv::{QueryResultWriter, Column, ColumnFlags};
use log::{debug, warn};

pub fn is_ghost_eid(val: &Value) -> bool {
    let gid = parser_val_to_u64(val);
    gid >= GHOST_ID_START
}

/*******************************************
 * Column stuff
 *******************************************/
pub fn tablecolumn_matches_col(c: &views::TableColumnDef, col: &str) -> bool {
    debug!("matching {} or {} to {}", c.colname, c.fullname, col);
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
pub fn contains_ghosted_columns(decor_config: &policy::Config, table_name: &str) -> bool {
    decor_config.parent_child_ghosted_tables.contains_key(table_name)
    || decor_config.child_parent_ghosted_tables.contains_key(table_name)
}
pub fn get_ghosted_cols_of_datatable(decor_config: &policy::Config, table_name: &ObjectName) -> Vec<(String, String)> {
    let mut c = vec![];
    if let Some(colnames) = decor_config.parent_child_ghosted_tables.get(&table_name.to_string()) {
        c.append(&mut colnames.clone());
    } 
    //XXX if you decorrelate parent->child, you should decorrelate child->parent
    /*if let Some(colnames) = decor_config.child_parent_ghosted_tables.get(&table_name.to_string()) {
        c.append(&mut colnames.clone());
    }*/
    c
}
pub fn get_ghosted_col_indices_of(decor_config: &policy::Config, table_name: &str, columns: &Vec<views::TableColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(colnames) = decor_config.parent_child_ghosted_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.colname == colname.0).unwrap(), colname.1.clone()));
        } 
    } 
    cis
}

pub fn get_sensitive_col_indices_of(decor_config: &policy::Config, table_name: &str, columns: &Vec<views::TableColumnDef>) -> Vec<(usize, String, f64)> {
    let mut cis = vec![];
    if let Some(colnames) = decor_config.parent_child_sensitive_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.colname == colname.0).unwrap(), colname.1.clone(), colname.2));
        } 
    } 
    cis
}

pub fn get_parent_col_indices_of_datatable(decor_config: &policy::Config, table_name: &ObjectName, columns: &Vec<sql_parser::ast::ColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(colnames) = decor_config.parent_child_ghosted_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.name.to_string() == colname.0).unwrap(), colname.1.clone()));
        } 
    }
    if let Some(colnames) = decor_config.parent_child_sensitive_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.name.to_string() == colname.0).unwrap(), colname.1.clone()));
        } 
    } 
    cis
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
pub fn expr_to_ghosted_col(expr:&Expr, ghosted_cols : &Vec<(String, String)>) -> Option<(String, String)> {
    match expr {
        Expr::Identifier(ids) => {
            let col = ids[ids.len()-1].to_string();
            if let Some(i) = ghosted_cols.iter().position(|(gc, _pc)| *gc == col) {
                Some(ghosted_cols[i].clone())
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
        Value::String(_i) => Value::String(rand::random::<u32>().to_string()),
        Value::Null => Value::Number(rand::random::<u32>().to_string()),
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
            //warn!("Parsing number {}", i);
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
                    | DataType::Time 
                    | DataType::Varchar(..) 
                    | DataType::Blob(..) 
                    | DataType::Char(..) => Value::String(trim_quotes(&valstr).to_string()),
                DataType::Boolean => Value::Boolean(valstr == "1"),
                _ => unimplemented!("type not supported yet")
            });
    }
    valsvec
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

pub fn mysql_val_to_string(val: &mysql::Value) -> String {
    match val {
        mysql::Value::NULL => "NULL".to_string(),
        mysql::Value::Bytes(bs) => {
            let res = str::from_utf8(&bs);
            match res {
                Err(_) => String::new(),
                Ok(s) => s.to_string(),
            }
        }
        mysql::Value::Int(i) => format!("{}", i),
        mysql::Value::UInt(i) => format!("{}", i),
        mysql::Value::Float(f) => format!("{}", f),
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
