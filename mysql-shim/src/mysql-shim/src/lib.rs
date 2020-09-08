extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use sql_parser::parser::parse_statements;
use std::collections::HashMap;
use std::*;
mod helpers;
pub mod config;
pub mod datatable_transformer;
pub mod mv_transformer;

const GHOST_ID_START : u64 = 1<<20;
const GHOST_TABLE_NAME : &'static str = "ghosts";
const GHOST_USER_COL : &'static str = "user_id";

fn create_ghosts_query() -> String {
    return format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `ghost_id` int unsigned NOT NULL AUTO_INCREMENT = {},
            `user_id` int unsigned);", 
        GHOST_TABLE_NAME, GHOST_ID_START);
}

struct Prepared {
    stmt: mysql::Statement,
    params: Vec<Column>,
}

pub struct Shim { 
    db: mysql::Conn,
    prepared: HashMap<u32, Prepared>,
    
    mv_trans: mv_transformer::MVTransformer,
    dt_trans: datatable_transformer::DataTableTransformer,

    // NOTE: not *actually* static, but tied to our connection's lifetime.
    schema: &'static str,
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.prepared.clear();
        // drop the connection (implicitly done).
    }
}

impl Shim {
    pub fn new(db: mysql::Conn, cfg_json: &str, schema: &'static str) -> Self {
        let cfg = config::parse_config(cfg_json).unwrap();
        let prepared = HashMap::new();
        let mv_trans = mv_transformer::MVTransformer::new(cfg.clone());
        let dt_trans = datatable_transformer::DataTableTransformer::new(cfg.clone());
        Shim{db, mv_trans, dt_trans, prepared, schema}
    }   

    /* 
     * Set all user_ids in the ghosts table to NULL
     * refresh "materialized views"
     */
    pub fn unsubscribe() -> bool {
        false
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     */
    pub fn resubscribe() -> bool {
        false
    }

    /* 
     * Given schema in sql, issue queries to set up database.
     * Must be issued after select_db statement is issued.
     * */
    fn create_schema(&mut self) -> Result<(), mysql::Error> {
        let mut current_q = String::new();
        for line in self.schema.lines() {
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
        Ok(())
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = mysql::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        // TODO save prepared stmts modified for MVs and ghosts table
        /*match self.db.prep(self.query_using_mv_tables(query)) {
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
                self.prepared.insert(stmt.id(), Prepared{stmt: stmt.clone(), params});
            },
            Err(e) => {
                match e {
                    mysql::Error::MySqlError(merr) => {
                        info.error(ErrorKind::ER_NO, merr.message.as_bytes())?;
                    },
                    _ => return Err(e),
                }
            }
        }*/
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
                        msql_srv::ValueInner::NULL => {
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
                    prepped.stmt.clone(), 
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
                if let Err(e) = self.db.close(prepped.stmt.clone()){
                    eprintln!("{}", e);
                };
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
        
        self.create_schema().unwrap();

        /* create ghost metadata table with boolean cols for each user id */
        let create_ghost_table_q = create_ghosts_query();
        // XXX temp: create a new ghost metadata table
        self.db.query_drop("DROP TABLE IF EXISTS ghosts;").unwrap();
        self.db.query_drop(create_ghost_table_q).unwrap();
        
        /* issue schema statements */
        let mut sql = String::new();
        for line in self.schema.lines() {
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

        let stmts = parse_statements(sql);
        match stmts {
            Err(e) => 
                Ok(w.error(ErrorKind::ER_BAD_DB_ERROR, &format!("{}", e).as_bytes())?),
            Ok(stmts) => {
                for stmt in stmts {
                    // TODO wrap in txn
                    let (mv_stmt, is_write) = self.mv_trans.stmt_to_mv_stmt(&stmt);
                    if is_write {
                        // issue actual statement to datatables if they are writes (potentially creating ghost ID 
                        // entries as well)
                        if let Some(dt_stmt) = self.dt_trans.stmt_to_datatable_stmt(&stmt, &mut self.db)? {
                            self.db.query_drop(dt_stmt.to_string())?;
                        }
                    }
                    // issue statement to materialized views AFTER
                    // issuing to datatables (which may perform reads)
                    self.db.query_drop(mv_stmt.to_string())?;
                }
                Ok(w.ok()?)
            }
        }
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let stmts_res = parse_statements(query.to_string());
        match stmts_res {
            Err(e) => {
                results.error(ErrorKind::ER_PARSE_ERROR, format!("{:?}", e).as_bytes())?;
                return Ok(());
            }
            Ok(stmts) => {
                assert!(stmts.len()==1);
                // TODO wrap in txn
                let (mv_stmt, is_write) = self.mv_trans.stmt_to_mv_stmt(&stmts[0]);
                if is_write {
                    if let Some(dt_stmt) = self.dt_trans.stmt_to_datatable_stmt(&stmts[0], &mut self.db)? {
                        self.db.query_drop(dt_stmt.to_string())?;
                    }
                }
                return answer_rows(results, self.db.query_iter(format!("{}", mv_stmt)));
            }
        }
    }
}

fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>,
) -> Result<(), mysql::Error> 
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

