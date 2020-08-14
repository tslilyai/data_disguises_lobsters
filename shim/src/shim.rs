extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use regex::Regex;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::*;
use std::*;
mod config;

pub const SCHEMA : &'static str = include_str!("./schema.sql");
const CONFIG : &'static str = include_str!("./config.json");

fn gen_create_ghost_metadata_query(user_col_names : &mut Vec<String>) -> String
{
    user_col_names.sort_unstable();
    user_col_names.dedup();

    let mut user_id_cols_str  = String::new();
    for col in user_col_names {
        user_id_cols_str.push_str(&format!(", {} int unsigned DEFAULT NULL", col));
    }
    let query = format!(r"CREATE TABLE IF NOT EXISTS ghost_metadata (
                gmd_user_id int unsigned,
                gmd_record_id int unsigned NOT NULL,
                gmd_table_name varchar(50)
                {}
            );", user_id_cols_str);
    println!("Create ghost metadata query: {}", query);
    return query;
}

fn gen_create_user_mv_query(
    user_table_name: &String, 
    user_id_column: &String,
    all_columns: &Vec<String>) -> String
{
    let mut other_columns = String::new();
    for col in all_columns {
        if col == user_id_column {
            continue;
        }
        other_columns.push_str(&format!(", {}.{}", user_table_name, col));
    }

    let query = format!(
    // XXX mysql doesn't support materialized views? only views?
    r"CREATE VIEW {user_table_name}_mv AS SELECT 
        COALESCE ({user_table_name}.{user_id_column}, ghost_metadata.{user_id_column}) as {user_id_column} 
        {other_columns}
        FROM ghost_metadata LEFT JOIN {user_table_name} 
        ON ghost_metadata.gmd_user_id = {user_table_name}.{user_id_column};",
        user_table_name = user_table_name,
        user_id_column = user_id_column,
        other_columns = other_columns);
    println!("Create user mv query: {}", query);
    return query;
}

fn gen_create_data_mv_query(
    table_name: &String, 
    table_id_col: &String, 
    user_id_columns: &Vec<String>,
    all_columns: &Vec<String>) -> String
{
    let mut user_id_col_str = String::new();
    for (i, col) in user_id_columns.iter().enumerate() {
        // only replace user id with ghost id if its present in the appropriate user id column
        user_id_col_str.push_str(&format!(
                "COALESCE({}.{}, ghost_metadata.{}) as {}",
                    table_name, col, col, col));
        if i < user_id_columns.len()-1 {
            user_id_col_str.push_str(", ");
        }
    }
    
    let mut data_col_str = String::new();
    for col in all_columns {
        if user_id_columns.contains(&col) {
            continue;
        }
        data_col_str.push_str(", ");
        data_col_str.push_str(&format!("{}.{}", table_name, col));
    }

    // XXX mysql doesn't support materialized views? only views?
    // add GROUP BY to ensure that we only get one row per data table record
    // this is necessary in cases where a data record has more than 1 user_id_col
    let query = format!(
    "CREATE VIEW {table_name}_mv AS SELECT 
        {id_col_str} {data_col_str}
        FROM {table_name} LEFT JOIN ghost_metadata
        ON 
            ghost_metadata.gmd_record_id = {table_name}.{record_id}
            AND ghost_metadata.gmd_table_name = '{table_name}'
        GROUP BY
           {table_name}.{record_id};",
        table_name = table_name,
        record_id = table_id_col,
        id_col_str = user_id_col_str,
        data_col_str = data_col_str);
    println!("Create data mv query: {}", query);
    return query;
}

pub struct Shim { db: mysql::Conn }

impl Shim {
    pub fn new(db: mysql::Conn) -> Self {
        Shim{db}
    }   
}

impl Drop for Shim {
    fn drop(&mut self) {
        // drop the connection (implicitly done).
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }
    
    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(0, 0)
    }
    
    fn on_close(&mut self, _: u32) {
    }

    fn on_init(&mut self, schema: &str, _: InitWriter<W>) -> io::Result<()> { 
        println!("On init called!");
        let res = self.db.select_db(schema);
        if !res {
            return Err(
                io::Error::new(
                    io::ErrorKind::Other,
                    "select db packet error",
                ));
        }   

        /* Set up schema */
        let mut current_q = String::new();
        for line in SCHEMA.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !current_q.is_empty() {
                current_q.push_str(" ");
            }
            current_q.push_str(line);
            if current_q.ends_with(';') {
                self.db.query_drop(&current_q).unwrap();
                println!("Query executed: {}", current_q);
                current_q.clear();
            }
        }

        /* get config so we can detect data tables */
        let cfg = config::parse_config(CONFIG).unwrap();
        let mut all_user_cols = Vec::<String>::new();
        all_user_cols.push(cfg.user_table.id_col.clone());
        for dt in &cfg.data_tables{
            all_user_cols.extend(dt.user_cols.iter().cloned());
        }

        /* create ghost metadata table with boolean cols for each user id */
        let create_ghost_table_q = gen_create_ghost_metadata_query(&mut all_user_cols);
        // XXX temp: create a new ghost metadata table
        self.db.query_drop("DROP TABLE IF EXISTS ghost_metadata;").unwrap();
        self.db.query_drop(create_ghost_table_q).unwrap();
        
        /* create materialized view for all data tables */
        let dialect = MySqlDialect{};
        let mut sql = String::new();
        let mut mv_query : String;
        for line in SCHEMA.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !sql.is_empty() {
                sql.push_str(" ");
            }
            sql.push_str(line);
            if sql.ends_with(';') {
                sql.push_str("\n");
            }
        }

        // XXX hack because parser doesn't support these: note that we just need the table and col names
        // seems like mysql specific parsing hasn't yet been merge in official crate
        // - remove backticks
        // - remove autoincrement options
        // - remove unsigned options
        // - remove tinyint(1) 
        let re = Regex::new(r"`|(?i)AUTO_INCREMENT|unsigned|tiny|\(1\)")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        sql = re.replace_all(&sql, "").to_string();
        
        // sqlparser also doesn't support indexes in create table stmts for mysql
        let re_end = Regex::new(", (fulltext|UNIQUE)? INDEX .*")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        sql = re_end.replace_all(&sql, ");").to_string();

        let stmts = Parser::parse_sql(&dialect, &sql).unwrap();
        println!("parsed {} into {} statements!", sql, stmts.len());

        for stmt in stmts {
            match stmt {
                Statement::CreateTable {
                    name,
                    columns,
                    ..
                } => {
                    // construct query to create MV 
                    let mut col_names = Vec::<String>::new();
                    for col in columns {
                        col_names.push(col.name.to_string());
                    }
                    if name.to_string() == cfg.user_table.name {
                        mv_query = gen_create_user_mv_query(
                            &cfg.user_table.name, 
                            &cfg.user_table.id_col, 
                            &col_names);
                    } else {
                        let dtopt = cfg.data_tables.iter().find(|&dt| dt.name == name.to_string());
                        match dtopt {
                            Some(dt) => {
                                mv_query = gen_create_data_mv_query(
                                    &dt.name,
                                    &dt.id_col,
                                    &dt.user_cols,
                                    &col_names)
                            },
                            _ => continue,
                        }                     
                    }
                    // execute query
                    self.db.query_drop(mv_query).unwrap();
                },
                _ => continue, // we only handle create table stmts
            }
        }
        println!("done with init!");
        Ok(())
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        match self.db.query_iter(query) {
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
                    let vals = row.unwrap();
                    for (c, col) in cols.iter().enumerate() {
                        match col.coltype {
                            ColumnType::MYSQL_TYPE_DECIMAL => writer.write_col(vals.get::<f64, _>(c))?,
                            ColumnType::MYSQL_TYPE_TINY => writer.write_col(vals.get::<i16, _>(c))?,
                            ColumnType::MYSQL_TYPE_SHORT => writer.write_col(vals.get::<i16, _>(c))?,
                            ColumnType::MYSQL_TYPE_LONG => writer.write_col(vals.get::<i32, _>(c))?,
                            ColumnType::MYSQL_TYPE_FLOAT => writer.write_col(vals.get::<f32, _>(c))?,
                            ColumnType::MYSQL_TYPE_DOUBLE => writer.write_col(vals.get::<f64, _>(c))?,
                            ColumnType::MYSQL_TYPE_NULL => writer.write_col(vals.get::<i16, _>(c))?,
                            ColumnType::MYSQL_TYPE_LONGLONG => writer.write_col(vals.get::<i64, _>(c))?,
                            ColumnType::MYSQL_TYPE_INT24 => writer.write_col(vals.get::<i32, _>(c))?,
                            ColumnType::MYSQL_TYPE_VARCHAR => writer.write_col(vals.get::<String, _>(c))?,
                            ColumnType::MYSQL_TYPE_BIT => writer.write_col(vals.get::<i16, _>(c))?,
                            ColumnType::MYSQL_TYPE_TINY_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                            ColumnType::MYSQL_TYPE_MEDIUM_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                            ColumnType::MYSQL_TYPE_LONG_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                            ColumnType::MYSQL_TYPE_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                            ColumnType::MYSQL_TYPE_VAR_STRING => writer.write_col(vals.get::<String, _>(c))?,
                            ColumnType::MYSQL_TYPE_STRING => writer.write_col(vals.get::<String, _>(c))?,
                            ColumnType::MYSQL_TYPE_GEOMETRY => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_TIMESTAMP => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_DATE => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_TIME => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_DATETIME => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_YEAR => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_NEWDATE => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_TIMESTAMP2 => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_DATETIME2 => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_TIME2 => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_JSON => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_NEWDECIMAL => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_ENUM => writer.write_col(vals.get::<i16, _>(c))?,
                            //ColumnType::MYSQL_TYPE_SET => writer.write_col(vals.get::<i16, _>(c))?,
                            ct => unimplemented!("Cannot translate row type {:?} into value", ct),
                        }
                    }
                    writer.end_row()?;
                }
                writer.finish()?;
            }
            Err(e) => {
                results.error(ErrorKind::ER_BAD_SLAVE, format!("{:?}", e).as_bytes())?;
            }
        }
        println!("Performed query {}", query);
        Ok(())
    }
}

/// Convert a MySQL type to MySQL_svr type 
fn get_coltype(t: &mysql::consts::ColumnType) -> ColumnType {
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
