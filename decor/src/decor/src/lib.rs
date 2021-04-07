extern crate ordered_float;
extern crate mysql;
extern crate crypto;
extern crate hex;

use msql_srv::*;
use mysql::prelude::*;
use std::io::{self, BufReader, BufWriter};
use std::*;
use log::{warn};

pub mod disguises;
pub mod helpers;
pub mod querier;
pub mod subscriber;

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub testname: String,
    pub use_decor: bool,
    pub parse: bool,
    pub in_memory: bool,
    pub prime: bool,
}

pub struct Shim { 
    db: mysql::Conn,
    querier: querier::Querier,
    app: disguises::Application,
    test_params: TestParams,
}

impl Drop for Shim {
    fn drop(&mut self) {
        helpers::stats::print_stats(&self.querier.stats, self.test_params.testname.clone());
        // drop the connection (implicitly done).
    }
}

impl Shim {
    pub fn new(db: mysql::Conn, app: disguises::Application, test_params: TestParams) 
        -> Self 
    {
        let querier = querier::Querier::new(&test_params);
        Shim{
            db: db, 
            querier: querier, 
            app: app, 
            test_params: test_params
        }
    }   

    pub fn run_on_tcp(
        dbname: &str, 
        app: disguises::Application, 
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
        MysqlIntermediary::run_on(Shim::new(db, app, test_params), 
                                    BufReader::new(rs), BufWriter::new(s))
    }

    /* 
     * Given schema in sql, issue queries to set up database.
     * Must be issued after select_db statement is issued.
     * Also creates valut tables 
     * */
    fn create_schema(&mut self) -> Result<(), mysql::Error> {
        self.db.query_drop("SET max_heap_table_size = 4294967295;")?;

        /* issue schema statements but only if we're not priming and not decor */
        if self.test_params.prime {
            let mut stmt = String::new();
            for stmt in &self.app.schema {
                self.querier.query_drop(&stmt, &mut self.db)?;                
            }
            for stmt in &self.app.vault {
                self.querier.query_drop(&stmt, &mut self.db)?;                
            }
        }
        Ok(())
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = mysql::Error;

    fn on_unsubscribe(&mut self, uid: u64, w: QueryResultWriter<W>) -> Result<(), mysql::Error> {
        let start = time::Instant::now();
        let res = self.querier.unsubscribe(uid, &mut self.db, w);
        let dur = start.elapsed();
        self.querier.record_query_stats(helpers::stats::QueryType::Unsub, dur);
        res
    }

    fn on_resubscribe(&mut self, 
                      uid: u64, 
                      gidshard: String, 
                      object_data: String, 
                      w: QueryResultWriter<W>) 
        -> Result<(), Self::Error> 
    {
        let start = time::Instant::now();
        let gidshard = helpers::remove_escaped_chars(&gidshard);
        let object_data = helpers::remove_escaped_chars(&object_data);
        warn!("RESUB got data {}, {}", gidshard, object_data);
        
        /*let gidshard = serde_json::from_str(&gidshard).unwrap();
        let object_data = serde_json::from_str(&object_data).unwrap();*/
 
        match self.querier.resubscribe(uid, &mut self.db) {
            Ok(()) => {
                let dur = start.elapsed();
                self.querier.record_query_stats(helpers::stats::QueryType::Resub, dur);
                Ok(w.completed(gidshard.len() as u64 + object_data.len() as u64, 0)?)
            }
            Err(e) => {
                let dur = start.elapsed();
                self.querier.record_query_stats(helpers::stats::QueryType::Resub, dur);
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
            warn!("on init select db failed");
            w.error(ErrorKind::ER_BAD_DB_ERROR, b"select db failed")?;
            return Ok(());
        }   
        
        self.querier.subscriber.init(&mut self.db, self.test_params.prime, self.test_params.in_memory)?;

        match self.create_schema() {
            Ok(_) => (),
            Err(e) => {
                warn!("Create schema failed with error {}", e);
                return Ok(w.error(ErrorKind::ER_BAD_DB_ERROR, &format!("{}", e).as_bytes())?);
            }
        }
        Ok(w.ok()?)
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let res : Result<(), Self::Error>;
        let start = time::Instant::now();
        let dur: time::Duration;
        
        if !self.test_params.parse {
            self.querier.cur_stat.nqueries+=1;
            res = helpers::answer_rows(results, self.db.query_iter(query));
            dur = start.elapsed();
        } else {
            let parsestart = time::Instant::now();
            let stmt_ast = helpers::get_single_parsed_stmt(&query.to_string())?;
            let parsedur = parsestart.elapsed();
            warn!("parse {} duration is {}", query, parsedur.as_micros());
            
            if !self.test_params.use_decor {
                self.querier.cur_stat.nqueries+=1;
                res = helpers::answer_rows(results, self.db.query_iter(query));
                dur = start.elapsed();
            } else {
                res = self.querier.query(results, &stmt_ast, &mut self.db);
                dur = start.elapsed();
            }
        }
        warn!("on_query {} duration is {}", query, dur.as_micros());
        /*if dur.as_micros() > 400 {
            error!("Long query: {}: {}us", query, dur.as_micros());
        }*/
        let qtype = helpers::stats::get_qtype(query)?;
        self.querier.record_query_stats(qtype, dur);
        res
    }
}
