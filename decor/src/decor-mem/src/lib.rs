extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter};
use std::*;
use log::{error, warn};
pub mod config;
pub mod helpers;
pub mod ghosts_cache;
pub mod query_transformer;
pub mod views;
pub mod sqlparser_cache;
pub mod stats;
pub mod select;

const GHOST_ID_START : u64 = 1<<20;
const GHOST_TABLE_NAME : &'static str = "ghosts";
const GHOST_USER_COL : &'static str = "user_id";
const GHOST_ID_COL: &'static str = "ghost_id";

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub translate: bool,
    pub parse: bool,
    pub in_memory: bool,
}

fn create_ghosts_query(in_memory: bool) -> String {
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` int unsigned)", 
        GHOST_TABLE_NAME, GHOST_ID_COL, GHOST_USER_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    q
}

fn set_initial_gid_query() -> String {
    format!(
        r"ALTER TABLE {} AUTO_INCREMENT={};",
        GHOST_TABLE_NAME, GHOST_ID_START)
}

struct Prepared {
    stmt: mysql::Statement,
    params: Vec<Column>,
}

pub struct Shim { 
    cfg: config::Config,
    db: mysql::Conn,
    prepared: HashMap<u32, Prepared>,

    qtrans: query_transformer::QueryTransformer,
    sqlcache: sqlparser_cache::ParserCache,

    // NOTE: not *actually* static, but tied to our connection's lifetime.
    schema: String,

    test_params: TestParams,
}

impl Drop for Shim {
    fn drop(&mut self) {
        stats::print_stats(&self.qtrans.stats, self.test_params.testname.clone());
        self.prepared.clear();
        // drop the connection (implicitly done).
    }
}

impl Shim {
    pub fn new(db: mysql::Conn, cfg_json: &str, schema: &'static str, test_params: TestParams) 
        -> Self 
    {
        let cfg = config::parse_config(cfg_json).unwrap();
        let prepared = HashMap::new();
        let qtrans = query_transformer::QueryTransformer::new(&cfg, &test_params);
        let sqlcache = sqlparser_cache::ParserCache::new();
        let schema = schema.to_string();
        Shim{cfg, db, qtrans, sqlcache, prepared, schema, test_params}
    }   

    pub fn run_on_tcp(
        dbname: &str, 
        cfg_json: &str, 
        schema: &'static str, 
        test_params: TestParams, 
        s: net::TcpStream) 
        -> Result<(), mysql::Error> 
    {

        let mut db = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", dbname)).unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", dbname)).unwrap();
        assert_eq!(db.ping(), true);
        let rs = s.try_clone().unwrap();
        MysqlIntermediary::run_on(Shim::new(db, cfg_json, schema, test_params), 
                                    BufReader::new(rs), BufWriter::new(s))
    }

    /* 
     * Given schema in sql, issue queries to set up database.
     * Must be issued after select_db statement is issued.
     * */
    fn create_schema(&mut self) -> Result<(), mysql::Error> {
        /* create ghost metadata table with boolean cols for each user id */
        // XXX temp: create a new ghost metadata table
        self.db.query_drop("DROP TABLE IF EXISTS ghosts;")?;
        self.db.query_drop(create_ghosts_query(self.test_params.in_memory))?;
        self.db.query_drop(set_initial_gid_query())?;
        warn!("drop/create/alter ghosts table");
       
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
    fn on_unsubscribe(&mut self, uid: u64, w: SubscribeWriter<W>) -> Result<(), mysql::Error> {
        match self.qtrans.unsubscribe(uid) {
            Ok(()) => Ok(w.ok()?),
            Err(_e) => {
                w.error(ErrorKind::ER_BAD_DB_ERROR, b"unsub failed")?;
                return Ok(());
            }
        }
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     * TODO check that user doesn't already exist
     */
    fn on_resubscribe(&mut self, uid: u64, w: SubscribeWriter<W>) -> Result<(), Self::Error> {
        match self.qtrans.resubscribe(uid, &mut self.db) {
            Ok(()) => Ok(w.ok()?),
            Err(_e) => {
                w.error(ErrorKind::ER_BAD_DB_ERROR, b"resub failed")?;
                return Ok(());
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
    
    fn on_close(&mut self, id: u32) {
        match self.prepared.get(&id) {
            None => return,
            Some(prepped) => {
                if let Err(e) = self.db.close(prepped.stmt.clone()){
                    warn!("close error {}", e);
                };
                self.prepared.remove(&id); 
            }
        }
    }

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
        // update autoinc value (if exists)
        if self.qtrans.cfg.user_table.is_autoinc {
            //TODO self.db.query_iter("")
        }
        Ok(w.ok()?)
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let res : Result<(), Self::Error>;
        let start = time::Instant::now();
        if !self.test_params.parse {
            warn!("on_query: {}", query);
            res = helpers::answer_rows(results, self.db.query_iter(query));
        } else {
            let stmt_ast = self.sqlcache.get_single_parsed_stmt(&query.to_string())?;
            if !self.test_params.translate {
                warn!("on_query: {}", stmt_ast);
                res = helpers::answer_rows(results, self.db.query_iter(stmt_ast.to_string()));
            } else {
                res = self.qtrans.query(results, &stmt_ast, &mut self.db);
            }
        }
        let qtype = stats::get_qtype(query)?;
        let dur = start.elapsed();
        if dur.as_secs() > 1 {
            error!("Long query: {}, {}s", query, dur.as_secs());
        }
        self.qtrans.record_query_stats(qtype, dur);
        res
    }
}