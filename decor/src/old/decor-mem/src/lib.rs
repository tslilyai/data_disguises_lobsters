extern crate ordered_float;
extern crate mysql;
extern crate crypto;
extern crate hex;

use msql_srv::*;
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::{self, BufReader, BufWriter};
use std::*;
use sql_parser::ast::*;
use log::{warn, error};

pub mod ghosts;
pub mod graph;
pub mod helpers;
pub mod policy;
pub mod query_transformer;
pub mod sqlparser_cache;
pub mod select;
pub mod subscriber;
pub mod views;

pub const INIT_CAPACITY: usize = 1000;
pub const ID_COL: &str = "id";

#[derive(Serialize, Deserialize, Hash, PartialOrd, Ord, PartialEq, Eq, Debug, Clone)]
pub struct EntityData {
    pub table: String, 
    pub row_strs: Vec<String>
}

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub translate: bool,
    pub parse: bool,
    pub in_memory: bool,
    pub prime: bool,
}

pub struct Shim { 
    db: mysql::Conn,

    qtrans: query_transformer::QueryTransformer,
    sqlcache: sqlparser_cache::ParserCache,

    // NOTE: not *actually* static, but tied to our connection's lifetime.
    schema: String,
    test_params: TestParams,
}

impl Drop for Shim {
    fn drop(&mut self) {
        helpers::stats::print_stats(&self.qtrans.stats, self.test_params.testname.clone());
        // drop the connection (implicitly done).
    }
}

impl Shim {
    pub fn new(db: mysql::Conn, schema: &'static str, policy: policy::ApplicationPolicy, test_params: TestParams) 
        -> Self 
    {
        let qtrans = query_transformer::QueryTransformer::new(policy, &test_params);
        let sqlcache = sqlparser_cache::ParserCache::new();
        let schema = schema.to_string();
        Shim{db, qtrans, sqlcache, schema, test_params}
    }   

    pub fn run_on_tcp(
        dbname: &str, 
        schema: &'static str, 
        policy: policy::ApplicationPolicy,
        test_params: TestParams, 
        s: net::TcpStream) 
        -> Result<(), mysql::Error> 
    {
        let mut db = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
        if test_params.prime {
            db.query_drop(&format!("DROP DATABASE IF EXISTS {};", dbname)).unwrap();
            db.query_drop(&format!("CREATE DATABASE {};", dbname)).unwrap();
        }
        assert_eq!(db.ping(), true);
        let rs = s.try_clone().unwrap();
        MysqlIntermediary::run_on(Shim::new(db, schema, policy, test_params), 
                                    BufReader::new(rs), BufWriter::new(s))
    }

    /* 
     * Given schema in sql, issue queries to set up database.
     * Must be issued after select_db statement is issued.
     * */
    fn create_schema(&mut self) -> Result<(), mysql::Error> {
        self.db.query_drop("SET max_heap_table_size = 4294967295;")?;

        /* issue schema statements but only if we're not priming and not decor */
        let mut stmt = String::new();
        for line in self.schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !stmt.is_empty() {
                stmt.push_str(" ");
            }
            stmt.push_str(line);
            if stmt.ends_with(';') {
                stmt = helpers::process_schema_stmt(&stmt, self.test_params.in_memory);
                let stmt_ast = self.sqlcache.get_single_parsed_stmt(&stmt)?;
                
                // if we're not priming, the table already exists!
                // add it as a MV
                if !self.test_params.prime {
                    match stmt_ast {
                        Statement::CreateTable(CreateTableStatement{
                            name,
                            columns,
                            constraints,
                            indexes,
                            ..
                        }) => {
                            self.qtrans.rebuild_view_with_all_rows(
                                &name.to_string(), columns, constraints, indexes, &mut self.db);
                        }
                        _ => (),
                    }
                } else {
                    self.qtrans.query_drop(&stmt_ast, &mut self.db)?;                
                }
                stmt = String::new();
            }
        }
        if !self.test_params.prime {
            self.qtrans.reupdate_with_ghost_mappings(&mut self.db);
        } 
        Ok(())
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = mysql::Error;

    /* 
     * Set all user_ids in the MV to ghost ids, insert ghost users into usersMV
     * TODO actually delete entries? 
     */
    fn on_unsubscribe(&mut self, uid: u64, w: QueryResultWriter<W>) -> Result<(), mysql::Error> {
        let start = time::Instant::now();
        let res = self.qtrans.unsubscribe(uid, &mut self.db, w);
        let dur = start.elapsed();
        self.qtrans.record_query_stats(helpers::stats::QueryType::Unsub, dur);
        res
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     * TODO check that user doesn't already exist
     */
    fn on_resubscribe(&mut self, 
                      uid: u64, 
                      gidshard: String, 
                      entity_data: String, 
                      w: QueryResultWriter<W>) 
        -> Result<(), Self::Error> 
    {
        let start = time::Instant::now();
        let gidshard = helpers::remove_escaped_chars(&gidshard);
        let entity_data = helpers::remove_escaped_chars(&entity_data);
        warn!("RESUB got data {}, {}", gidshard, entity_data);
        
        let gidshard = serde_json::from_str(&gidshard).unwrap();
        let entity_data = serde_json::from_str(&entity_data).unwrap();
 
        match self.qtrans.resubscribe(uid, &gidshard, &entity_data, &mut self.db) {
            Ok(()) => {
                let dur = start.elapsed();
                self.qtrans.record_query_stats(helpers::stats::QueryType::Resub, dur);
                Ok(w.completed(gidshard.len() as u64 + entity_data.len() as u64, 0)?)
            }
            Err(e) => {
                let dur = start.elapsed();
                self.qtrans.record_query_stats(helpers::stats::QueryType::Resub, dur);
                w.error(ErrorKind::ER_BAD_DB_ERROR, format!("b{}", e).as_bytes())?;
                Ok(())
            }
        }
    }

    fn on_prepare(&mut self, _query: &str, _info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        Ok(())
    }
    
    fn on_execute(
        &mut self,
        _id: u32,
        _ps: ParamParser,
        _results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        return Ok(());
    }
    
    fn on_close(&mut self, _id: u32) {}

    fn on_init(&mut self, schema: &str, w: InitWriter<W>) -> Result<(), Self::Error> {
        let res = self.db.select_db(schema);
        if !res {
            w.error(ErrorKind::ER_BAD_DB_ERROR, b"select db failed")?;
            return Ok(());
        }   
        
        self.qtrans.subscriber.init(&mut self.db, self.test_params.prime, self.test_params.in_memory)?;

        match self.create_schema() {
            Ok(_) => (),
            Err(e) => {
                warn!("Create schema failed with error {}", e);
                return Ok(w.error(ErrorKind::ER_BAD_DB_ERROR, &format!("{}", e).as_bytes())?);
            }
        }
        // TODO update autoinc value (if exists)
        Ok(w.ok()?)
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let res : Result<(), Self::Error>;
        let start = time::Instant::now();
        let dur: time::Duration;
        
        if !self.test_params.parse {
            self.qtrans.cur_stat.nqueries+=1;
            res = helpers::answer_rows(results, self.db.query_iter(query));
            dur = start.elapsed();
        } else {
            let parsestart = time::Instant::now();
            let stmt_ast = self.sqlcache.get_single_parsed_stmt(&query.to_string())?;
            let parsedur = parsestart.elapsed();
            warn!("parse {} duration is {}", query, parsedur.as_micros());
            
            if !self.test_params.translate {
                self.qtrans.cur_stat.nqueries+=1;
                res = helpers::answer_rows(results, self.db.query_iter(stmt_ast.to_string()));
                dur = start.elapsed();
            } else {
                res = self.qtrans.query(results, &stmt_ast, &mut self.db);
                dur = start.elapsed();
            }
        }
        /*if dur.as_micros() > 400 {
            error!("Long query: {}: {}us", query, dur.as_micros());
        }*/
        let qtype = helpers::stats::get_qtype(query)?;
        self.qtrans.record_query_stats(qtype, dur);
        res
    }
}
