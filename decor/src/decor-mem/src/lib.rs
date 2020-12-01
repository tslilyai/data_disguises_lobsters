extern crate ordered_float;
extern crate mysql;
extern crate crypto;
extern crate hex;

use msql_srv::*;
use mysql::prelude::*;
use std::io::{self, BufReader, BufWriter};
use std::*;
use log::{warn};

pub mod helpers;
pub mod ghosts_map;
pub mod query_transformer;
pub mod views;
pub mod sqlparser_cache;
pub mod stats;
pub mod select;
pub mod policy;

pub const INIT_CAPACITY: usize = 1000;
pub const ID_COL: &str = "id";

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub translate: bool,
    pub parse: bool,
    pub in_memory: bool,
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
        stats::print_stats(&self.qtrans.stats, self.test_params.testname.clone());
        // drop the connection (implicitly done).
    }
}

impl Shim {
    pub fn new(db: mysql::Conn, schema: &'static str, policy: policy::ApplicationPolicy<'static>, test_params: TestParams) 
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
        policy: policy::ApplicationPolicy<'static>,
        test_params: TestParams, 
        s: net::TcpStream) 
        -> Result<(), mysql::Error> 
    {
        let mut db = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", dbname)).unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", dbname)).unwrap();
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
        /* create ghost metadata table with boolean cols for each user id */
        ghosts_map::create_ghosts_table(&mut self.db, self.test_params.in_memory)?;
       
        /* issue schema statements */
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
                self.qtrans.query_drop(&stmt_ast, &mut self.db)?;                
                stmt = String::new();
            }
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
        self.qtrans.record_query_stats(stats::QueryType::Unsub, dur);
        res
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     * TODO check that user doesn't already exist
     */
    fn on_resubscribe(&mut self, uid: u64, gids: Vec<u64>, w: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let start = time::Instant::now();
        match self.qtrans.resubscribe(uid, &gids, &mut self.db) {
            Ok(()) => {
                let dur = start.elapsed();
                self.qtrans.record_query_stats(stats::QueryType::Resub, dur);
                Ok(w.completed(gids.len() as u64, 0)?)
            }
            Err(e) => {
                let dur = start.elapsed();
                self.qtrans.record_query_stats(stats::QueryType::Resub, dur);
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
        let qtype = stats::get_qtype(query)?;
        self.qtrans.record_query_stats(qtype, dur);
        res
    }
}
