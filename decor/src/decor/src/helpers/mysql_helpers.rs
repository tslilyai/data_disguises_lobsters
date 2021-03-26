use crate::types::*;
use log::debug;
use msql_srv::{Column, ColumnFlags, QueryResultWriter};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::str::FromStr;
use std::*;

/************************************
 * MYSQL HELPERS
 ************************************/
pub fn get_rows_of_query(
    q: &str,
    table: &str,
    id_col_name: &str,
    db: &mut mysql::Conn,
) -> Result<Vec<Row>, mysql::Error> {
    let mut rows = vec![];
    let mut id_col_index = 0;
    let mut i = 0;

    let res = db.query_iter(q)?;
    let cols: Vec<TableCol> = res
        .columns()
        .as_ref()
        .iter()
        .map(|c| {
            let index = i;
            let name = c.name_str().to_string();
            if name == id_col_name {
                id_col_index = index;
            }
            i += 1;
            TableCol {
                table: table.to_string(),
                col_index: index,
                col_name: name,
            }
        })
        .collect();

    for row in res {
        let rowvals = row.unwrap().unwrap();
        let vals : Vec<String> = rowvals
            .iter()
            .map(|v| mysql_val_to_string(v))
            .collect();
        rows.push(Row {
            id: ID {
                table: table.to_string(),
                id: u64::from_str(&vals[id_col_index]).unwrap(),
                id_col_index: id_col_index,
                id_col_name: id_col_name.to_string(),
            },
            columns: cols.clone(),
            values: vals,
        });
    }
    Ok(rows)
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

pub fn escape_quotes_mysql(s: &str) -> String {
    let mut s = s.replace("\'", "\'\'");
    s = s.replace("\"", "\"\"");
    s
}

pub fn remove_escaped_chars(s: &str) -> String {
    let mut s = s.replace("\'\'", "\'");
    s = s.replace("\"\"", "\"");
    s = s.replace("\"\'\"", "\"\'\'\"");
    s
}

pub fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>,
) -> Result<(), mysql::Error> {
    match rows {
        Ok(rows) => {
            let cols: Vec<_> = rows
                .columns()
                .as_ref()
                .into_iter()
                .map(|c| Column {
                    table: c.table_str().to_string(),
                    column: c.name_str().to_string(),
                    coltype: get_coltype(&c.column_type()),
                    colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
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
            results.error(
                msql_srv::ErrorKind::ER_BAD_SLAVE,
                format!("{:?}", e).as_bytes(),
            )?;
        }
    }
    Ok(())
}

/// Convert a MySQL type to MySQL_svr type
pub fn get_coltype(t: &mysql::consts::ColumnType) -> msql_srv::ColumnType {
    use msql_srv::ColumnType;
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
        mysql::Value::Float(f) => mysql_common::value::Value::Double((*f).into()),
        mysql::Value::Double(f) => mysql_common::value::Value::Double((*f).into()),
        mysql::Value::Date(a, b, c, d, e, f, g) => {
            mysql_common::value::Value::Date(*a, *b, *c, *d, *e, *f, *g)
        }
        mysql::Value::Time(a, b, c, d, e, f) => {
            mysql_common::value::Value::Time(*a, *b, *c, *d, *e, *f)
        }
    }
}

pub fn mysql_val_to_parser_val(val: &mysql::Value) -> sql_parser::ast::Value {
    match val {
        mysql::Value::NULL => Value::Null,
        mysql::Value::Bytes(bs) => {
            let res = str::from_utf8(&bs);
            match res {
                Err(_) => Value::String(String::new()),
                Ok(s) => Value::String(remove_escaped_chars(s).to_string()),
            }
        }
        mysql::Value::Int(i) => Value::Number(format!("{}", i)),
        mysql::Value::UInt(i) => Value::Number(format!("{}", i)),
        mysql::Value::Float(f) => Value::Number(format!("{}", f)),
        _ => unimplemented!("No sqlparser support for dates yet?"), /*mysql::Date(u16, u8, u8, u8, u8, u8, u32),
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
                Ok(s) => remove_escaped_chars(s),
            }
        }
        mysql::Value::Int(i) => format!("{}", i),
        mysql::Value::UInt(i) => format!("{}", i),
        mysql::Value::Float(f) => format!("{}", f),
        _ => unimplemented!("No sqlparser support for dates yet?"), /*mysql::Date(u16, u8, u8, u8, u8, u8, u32),
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
            io::ErrorKind::Other,
            format!("value {:?} is not an int", val),
        ))),
    }
}
