extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use regex::Regex;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::*;
use std::collections::HashMap;
use std::*;
mod config;

pub const SCHEMA : &'static str = include_str!("./schema.sql");
const DIALECT : sqlparser::dialect::MySqlDialect = MySqlDialect{};
const MV_SUFFIX : &'static str = "_mv"; 


fn gen_create_ghost_metadata_query() -> String {
    return r"CREATE TABLE IF NOT EXISTS `ghost_metadata` (
                `user_id` int unsigned,
                `ghost_id` int unsigned NOT NULL,
                UNIQUE INDEX `ghost_id` (`ghost_id`));".to_string();
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
    // XXX mysql doesn't support materialized views, so just create a new table
    r"CREATE TABLE {user_table_name}{suffix} AS (SELECT 
            COALESCE (ghost_metadata.user_id, {user_table_name}.{user_id_column}, ) as {user_id_column} 
            {other_columns}
        FROM ghost_metadata LEFT JOIN {user_table_name} 
        ON ghost_metadata.user_id = {user_table_name}.{user_id_column});",
        suffix = MV_SUFFIX,
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
                "COALESCE(ghost_metadata.user_id, {}.{}) as {}",
                    table_name, col, col));
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
    "CREATE TABLE {table_name}{suffix} AS (SELECT 
            {id_col_str} 
            {data_col_str}
        FROM {table_name} LEFT JOIN ghost_metadata
        ON 
            ghost_metadata.record_id = {table_name}.{record_id}
            AND ghost_metadata.table_name = '{table_name}';)",
        suffix = MV_SUFFIX,
        table_name = table_name,
        record_id = table_id_col,
        id_col_str = user_id_col_str,
        data_col_str = data_col_str);
    println!("Create data mv query: {}", query);
    return query;
}

struct Prepared {
    stmt: mysql::Statement,
    params: Vec<Column>,
}

pub struct Shim { 
    db: mysql::Conn,
    cfg: config::Config,
    table_names: Vec<String>,
    prepared: HashMap<u32, Prepared>,
}

impl Shim {
    pub fn new(db: mysql::Conn, cfg_json: &str) -> Self {
        let cfg = config::parse_config(cfg_json).unwrap();
        let mut table_names = Vec::<String>::new();
        table_names.push(cfg.user_table.name.clone());
        for dt in &cfg.data_tables{
            table_names.push(dt.name.clone());
        }
        let prepared = HashMap::new();
        Shim{db, cfg, table_names, prepared}
    }   

    fn query_using_mv_tables(&self, query: &str) -> String {
        // TODO handle insertions or updates --> need to modify ghost_metadata table
        let mut changed_q = query.to_string();
        let mut new_name : String; 
        for table_name in &self.table_names {
            new_name = table_name.clone();
            new_name.push_str(MV_SUFFIX);
            changed_q = changed_q.replace(table_name, &new_name);
        }
        changed_q
    }
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.prepared.clear();
        // drop the connection (implicitly done).
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = mysql::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        match self.db.prep(self.query_using_mv_tables(query)) {
            Ok(stmt) => {
                let params: Vec<_> = stmt
                    .params()
                    .into_iter()
                    .map(|p| {
                        Column {
                            table: p.table_str().to_string(),
                            column: p.name_str().to_string(),
                            coltype: get_coltype(&p.column_type()),
                            colflags: ColumnFlags::from_bits(p.flags().bits()).unwrap(),
                        }
                    })
                    .collect();
                let columns: Vec<_> = stmt
                    .columns()
                    .into_iter()
                    .map(|c| {
                        Column {
                            table: c.table_str().to_string(),
                            column: c.name_str().to_string(),
                            coltype: get_coltype(&c.column_type()),
                            colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                        }
                    })
                    .collect();
                info.reply(stmt.id(), &params, &columns)?;
                self.prepared.insert(stmt.id(), Prepared{stmt, params});
            },
            Err(e) => {
                match e {
                    mysql::Error::MySqlError(merr) => {
                        info.error(ErrorKind::ER_NO, merr.message.as_bytes())?;
                    },
                    _ => return Err(e),
                }
            }
        }
        Ok(())
    }
    
    fn on_execute(
        &mut self,
        id: u32,
        ps: ParamParser,
        results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        match self.prepared.get(&id) {
            None => return Ok(results.error(ErrorKind::ER_NO, b"no such prepared statement")?),
            Some(prepped) => {
                // parse params
                let args : Vec<mysql::Value> = ps
                    .into_iter()
                    .map(|p| match p.value.into_inner() {
                        NULL => {
                            mysql::Value::NULL
                        }
                        ValueInner::Bytes(bs) => {
                            mysql::Value::Bytes(bs.to_vec())
                        }
                        ValueInner::Int(v) => {
                            mysql::Value::Int(v)
                        }
                        ValueInner::UInt(v) => {
                            mysql::Value::UInt(v)
                        }
                        ValueInner::Double(v) => {
                            mysql::Value::Float(v)
                        }
                        ValueInner::Date(bs) => {
                            assert!(bs.len() == 7);
                            mysql::Value::Date(bs[0].into(), bs[1].into(), bs[2], bs[3], bs[4], bs[5], bs[6].into())
                        }
                        ValueInner::Time(bs) => {
                            assert!(bs.len() == 6);
                            mysql::Value::Time(bs[0] == 0, bs[1].into(), bs[2], bs[3], bs[4], bs[5].into())
                        }
                        ct => unimplemented!("no translation for param type {:?}", ct)
                }).collect();

                let res = self.db.exec_iter(
                    prepped.stmt, 
                    mysql::params::Params::Positional(args),
                );

                // TODO get response
                return Ok(());
                //answer_rows(results, self.db.query_iter(self.query_using_mv_tables("")))
            }
        }
    }
    
    fn on_close(&mut self, id: u32) {
        match self.prepared.get(&id) {
            None => return,
            Some(prepped) => {
                self.db.close(prepped.stmt);
                self.prepared.remove(&id); 
            }
        }
    }

    fn on_init(&mut self, schema: &str, w: InitWriter<W>) -> Result<(), Self::Error> {
        println!("On init called!");
        let res = self.db.select_db(schema);
        if !res {
            w.error(ErrorKind::ER_BAD_DB_ERROR, b"select db failed")?;
            return Ok(());
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

        /* create ghost metadata table with boolean cols for each user id */
        let create_ghost_table_q = gen_create_ghost_metadata_query();
        // XXX temp: create a new ghost metadata table
        self.db.query_drop("DROP TABLE IF EXISTS ghost_metadata;").unwrap();
        self.db.query_drop(create_ghost_table_q).unwrap();
        
        /* create materialized view for all data tables */
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

        let stmts = Parser::parse_sql(&DIALECT, &sql).unwrap();
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
                    if name.to_string() == self.cfg.user_table.name {
                        mv_query = gen_create_user_mv_query(
                            &self.cfg.user_table.name, 
                            &self.cfg.user_table.id_col, 
                            &col_names);
                    } else {
                        let dtopt = self.cfg.data_tables.iter().find(|&dt| dt.name == name.to_string());
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
        w.ok()?;
        Ok(())
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        // TODO support adding and modifying data tables
        /* Statement::CreateTable {} => {}
            Statement::CreateVirtualTable {} => {}
            Statement::CreateIndex {} => {}
            Statement::AlterTable { name, operation } => {}
            Statement::CreateSchema { schema_name } => {}*/
        answer_rows(results, self.db.query_iter(self.query_using_mv_tables(query)))
    }
}

fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    query: &str,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>,
) -> Result<(), mysql::Error> 
{
    let stmts = Parser::parse_sql(&DIALECT, &query).unwrap();
        println!("parsed {} into {} statements!", query, stmts.len());

        for stmt in stmts {
            match stmt {
                Statement::Insert {
                    table_name,
                    columns,
                    source,
                } => {
                    /*if table_name in self.table_names {
                        
                    }*/

                }
            }
        }


        
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
    Ok(())

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
