extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use std::collections::HashMap;
use sql_parser::*;
use sql_parser::ast::*;
use std::io::{self, BufReader, BufWriter};
use std::*;
use log::{debug, warn};
pub mod config;
pub mod helpers;
pub mod qtcache;
pub mod query_transformer;
pub mod sqlparser_cache;

const GHOST_ID_START : u64 = 1<<20;
const GHOST_TABLE_NAME : &'static str = "ghosts";
const GHOST_USER_COL : &'static str = "user_id";
const GHOST_ID_COL: &'static str = "ghost_id";
const MV_SUFFIX : &'static str = "mv"; 

#[derive(Debug, Clone, PartialEq)]
pub struct TestParams {
    pub translate: bool,
    pub parse: bool,
    pub in_memory: bool,
}

pub struct QTStats {
    pub nqueries : usize,
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
    //params: Vec<Column>,
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
        println!("\tSHIM performed {} queries", self.qtrans.stats.nqueries + self.qtrans.cache.stats.nqueries);
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

    pub fn get_num_queries(&self) -> usize {
        self.qtrans.stats.nqueries
    }
   
    pub fn run_on_tcp(
        db: mysql::Conn, 
        cfg_json: &str, 
        schema: &'static str, 
        test_params: TestParams, 
        s: net::TcpStream) 
        -> Result<(), mysql::Error> 
    {
        let rs = s.try_clone().unwrap();
        MysqlIntermediary::run_on(Shim::new(db, cfg_json, schema, test_params), 
                                    BufReader::new(rs), BufWriter::new(s))
    }

    /* 
     * Given schema in sql, issue queries to set up database.
     * Must be issued after select_db statement is issued.
     * */
    fn create_schema(&mut self) -> Result<(), mysql::Error> {
        let mut txn = self.db.start_transaction(mysql::TxOpts::default())?;

        /* create ghost metadata table with boolean cols for each user id */
        // XXX temp: create a new ghost metadata table
        txn.query_drop("DROP TABLE IF EXISTS ghosts;")?;
        txn.query_drop(create_ghosts_query(self.test_params.in_memory))?;
        txn.query_drop(set_initial_gid_query())?;
        warn!("drop/create/alter ghosts table");
        self.qtrans.stats.nqueries+=3;
        
        /* issue schema statements */
        let mut sql = String::new();
        let mut stmt = String::new();
        for line in self.schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !sql.is_empty() {
                sql.push_str(" ");
                stmt.push_str(" ");
            }
            stmt.push_str(line);
            if stmt.ends_with(';') {
                stmt = helpers::process_schema_stmt(&stmt, self.test_params.in_memory);
                
                let stmt_ast = self.sqlcache.get_single_parsed_stmt(&stmt)?;
                warn!("stmt: {}", stmt_ast);
                let mv_stmt = self.qtrans.get_mv_stmt(&stmt_ast, &mut txn)?;
                warn!("mv_stmt: {}", mv_stmt);
                txn.query_drop(mv_stmt.to_string())?;
                self.qtrans.stats.nqueries+=1;
                
                stmt = String::new();
            }
        }
        Ok(())
    }

    fn prep_statement(&mut self, query: &str) -> Result<(), Self::Error> {
        match self.db.prep(query) {
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
    fn on_unsubscribe(&mut self, uid: u64, w: SubscribeWriter<W>) -> Result<(), Self::Error> {
        let mut txn = self.db.start_transaction(mysql::TxOpts::default())?;
        let uid_val = ast::Value::Number(uid.to_string());
                    
        let vals_vec : Vec<Vec<Expr>> = self.qtrans.cache.get_gids_for_uid(uid, &mut txn)?
            .iter()
            .map(|g| vec![Expr::Value(ast::Value::Number(g.to_string()))])
            .collect();
        let gid_source_q = Query {
            ctes: vec![],
            body: SetExpr::Values(Values(vals_vec)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        };
        let user_table_name = helpers::string_to_objname(&self.cfg.user_table.name);
        let mv_table_name = self.qtrans.objname_to_mv_objname(&user_table_name);
 
        /* 
         * 1. update the users MV to have an entry for all the users' GIDs
         */
        let insert_gids_as_users_stmt = Statement::Insert(InsertStatement{
            table_name: mv_table_name.clone(),
            columns: vec![Ident::new(&self.cfg.user_table.id_col)],
            source: InsertSource::Query(Box::new(gid_source_q)),
        });
        warn!("unsub: {}", insert_gids_as_users_stmt);
        txn.query_drop(format!("{}", insert_gids_as_users_stmt.to_string()))?;
        self.qtrans.stats.nqueries+=1;
        
        /*
         * 2. delete UID from users MV and users (only one table, so delete from either)
         */
        let delete_uid_from_users = Statement::Delete(DeleteStatement {
            table_name: user_table_name,
            selection: Some(Expr::BinaryOp{
                left: Box::new(Expr::Identifier(helpers::string_to_idents(&self.cfg.user_table.id_col))),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(uid_val.clone())), 
            }),
        });
        warn!("unsub: {}", delete_uid_from_users);
        txn.query_drop(format!("{}", delete_uid_from_users.to_string()))?;
        self.qtrans.stats.nqueries+=1;
 
        /* 
         * 3. Change all entries with this UID to use the correct GID in the MV
         */
        for dt in &self.cfg.data_tables {
            let dtobjname = helpers::string_to_objname(&dt.name);
            let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &dtobjname);

            let mut assignments : Vec<String> = vec![];
            for uc in ucols {
                let uc_dt_ids = helpers::string_to_idents(&uc);
                let uc_mv_ids = self.qtrans.idents_to_mv_idents(&uc_dt_ids);
                let mut astr = String::new();
                astr.push_str(&format!(
                        "{} = {}", 
                        ObjectName(uc_mv_ids.clone()),
                        Expr::Case{
                            operand: None, 
                            // check usercol_mv = UID
                            conditions: vec![Expr::BinaryOp{
                                left: Box::new(Expr::Identifier(uc_mv_ids.clone())),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(uid_val.clone())),
                            }],
                            // then assign to ghost ucol value
                            results: vec![Expr::Identifier(uc_dt_ids)],
                            // otherwise keep as the uid in the MV
                            else_result: Some(Box::new(Expr::Identifier(uc_mv_ids.clone()))),
                        }));
                assignments.push(astr);
            }
           
            let mut select_constraint = Expr::Value(ast::Value::Boolean(true));
            // add constraint on non-user columns to be identical (performing a "JOIN" on the DT
            // and the MV so the correct rows are joined together)
            // XXX could put a constraint selecting rows only with the UID in a ucol
            // but the assignment CASE should already handle this?
            for col in &dt.data_cols {
                let mut fullname = dt.name.clone();
                fullname.push_str(".");
                fullname.push_str(&col);
                let dt_ids = helpers::string_to_idents(&fullname);
                let mv_ids = self.qtrans.idents_to_mv_idents(&dt_ids);

                select_constraint = Expr::BinaryOp {
                    left: Box::new(select_constraint),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(mv_ids)),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Identifier(dt_ids)),
                    }),             
                };
            }
                
            // UPDATE corresponding MV
            // SET MV.usercols = (MV.usercol = uid) ? dt.usercol : MV.usercol 
            // WHERE dtMV = dt ON [all other rows equivalent]
            let mut astr = String::new();
            astr.push_str(&assignments[0]);
            for i in 1..assignments.len() {
                astr.push_str(", ");
                astr.push_str(&assignments[i]);
            }
                
            let update_dt_stmt = format!("UPDATE {}, {} SET {} WHERE {};", 
                self.qtrans.objname_to_mv_objname(&dtobjname).to_string(),
                dtobjname.to_string(),
                astr,
                select_constraint.to_string(),
            );
                
            warn!("unsub: {}", update_dt_stmt);
            txn.query_drop(format!("{}", update_dt_stmt))?;
            self.qtrans.stats.nqueries+=1;
        }
        
        // TODO return some type of auth token?
        txn.commit()?;
        Ok(w.ok()?)
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     * TODO check that user doesn't already exist
     */
    fn on_resubscribe(&mut self, uid: u64, w: SubscribeWriter<W>) -> Result<(), Self::Error> {
        // TODO check auth token?
        let mut txn = self.db.start_transaction(mysql::TxOpts::default())?;
        let uid_val = ast::Value::Number(uid.to_string());
        
        let gid_exprs : Vec<Expr> = self.qtrans.cache.get_gids_for_uid(uid, &mut txn)?
            .iter()
            .map(|g| Expr::Value(ast::Value::Number(g.to_string())))
            .collect();
        let user_table_name = helpers::string_to_objname(&self.cfg.user_table.name);
        let mv_table_name = self.qtrans.objname_to_mv_objname(&user_table_name);

        /*
         * 1. drop all GIDs from users table 
         */
        let delete_gids_as_users_stmt = Statement::Delete(DeleteStatement {
            table_name: mv_table_name.clone(),
            selection: Some(Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(&self.cfg.user_table.id_col))),
                list: gid_exprs.clone(),
                negated: false, 
            }),
        });
        warn!("resub: {}", delete_gids_as_users_stmt);
        txn.query_drop(format!("{}", delete_gids_as_users_stmt.to_string()))?;
        self.qtrans.stats.nqueries+=1;

        /*
         * 2. Add user to users and usersmv (only one table)
         * TODO should also add back all of the user data????
         */
        let insert_uid_as_user_stmt = Statement::Insert(InsertStatement{
            table_name: user_table_name,
            columns: vec![Ident::new(&self.cfg.user_table.id_col)],
            source: InsertSource::Query(Box::new(Query{
                ctes: vec![],
                body: SetExpr::Values(Values(vec![vec![Expr::Value(uid_val.clone())]])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            })),
        });
        warn!("resub: {}", insert_uid_as_user_stmt.to_string());
        txn.query_drop(format!("{}", insert_uid_as_user_stmt.to_string()))?;
        self.qtrans.stats.nqueries+=1;
 
        /* 
         * 3. update assignments in MV to use UID again
         */
        for dt in &self.cfg.data_tables {
            let dtobjname = helpers::string_to_objname(&dt.name);
            let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &dtobjname);
            
            let mut assignments : Vec<String> = vec![];
            for uc in ucols {
                let uc_dt_ids = helpers::string_to_idents(&uc);
                let uc_mv_ids = self.qtrans.idents_to_mv_idents(&uc_dt_ids);
                let mut astr = String::new();
                astr.push_str(&format!(
                        "{} = {}", 
                        ObjectName(uc_mv_ids.clone()),
                        Expr::Case{
                            operand: None, 
                            // check usercol_mv IN gids
                            conditions: vec![Expr::InList{
                                expr: Box::new(Expr::Identifier(uc_mv_ids.clone())),
                                list: gid_exprs.clone(),
                                negated: false,
                            }],
                            // then assign UID value
                            results: vec![Expr::Value(uid_val.clone())],
                            // otherwise keep as the current value in the MV
                            else_result: Some(Box::new(Expr::Identifier(uc_mv_ids.clone()))),
                        }));
                assignments.push(astr);
            }
           
            let mut select_constraint = Expr::Value(ast::Value::Boolean(true));
            // add constraint on non-user columns to be identical (performing a "JOIN" on the DT
            // and the MV so the correct rows are joined together)
            // XXX could put a constraint selecting rows only with the GIDs in a ucol
            // but the assignment CASE should already handle this?
            for col in &dt.data_cols {
                let mut fullname = dt.name.clone();
                fullname.push_str(".");
                fullname.push_str(&col);
                let dt_ids = helpers::string_to_idents(&fullname);
                let mv_ids = self.qtrans.idents_to_mv_idents(&dt_ids);

                select_constraint = Expr::BinaryOp {
                    left: Box::new(select_constraint),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(mv_ids)),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Identifier(dt_ids)),
                    }),             
                };
            }
                
            // UPDATE corresponding MV
            // SET MV.usercols = (MV.usercol = dt.usercol) ? uid : MV.usercol
            // WHERE dtMV = dt ON [all other rows equivalent]
            let mut astr = String::new();
            astr.push_str(&assignments[0]);
            for i in 1..assignments.len() {
                astr.push_str(", ");
                astr.push_str(&assignments[i]);
            }
            let update_dt_stmt = format!("UPDATE {}, {} SET {} WHERE {};", 
                self.qtrans.objname_to_mv_objname(&dtobjname).to_string(),
                dtobjname.to_string(),
                astr,
                select_constraint.to_string(),
            );
            warn!("resub: {}", update_dt_stmt);
            txn.query_drop(format!("{}", update_dt_stmt))?;
            self.qtrans.stats.nqueries+=1;
        }    
        txn.commit()?;
        Ok(w.ok()?)
    }

    fn on_prepare(&mut self, query: &str, _info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        if !self.test_params.parse{
            return self.prep_statement(query);
        }
        
        let stmt_ast = self.sqlcache.get_single_parsed_stmt(&query.to_string())?;
        if !self.test_params.translate {
            self.qtrans.stats.nqueries+=1;
            warn!("on_query: {}", stmt_ast);
            return self.prep_statement(format!("{}", stmt_ast));
        }
        // wrap in txn to ensure that all reads are consistent if any are performed
        let mut txn = self.db.start_transaction(mysql::TxOpts::default())?;
        let mv_stmt = self.qtrans.prep_mv_stmt(&stmt_ast, &mut txn)?;
        txn.commit()?;
        // TODO add prepped statement
        Ok(())
    }
    
    fn on_execute(
        &mut self,
        _id: u32,
        _ps: ParamParser,
        _results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        /*match self.prepared.get(&id) {
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
                //answer_rows(results, self.db.query_iter(self.query_using_mv_tables("")))
            }
        }*/
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
                debug!("Create schema failed with error {}", e);
                return Ok(w.error(ErrorKind::ER_BAD_DB_ERROR, &format!("{}", e).as_bytes())?);
            }
        }
        // update autoinc value (if exists)
        if self.qtrans.cfg.user_table.is_autoinc {
            //TODO self.db.query_iter("")
        }

        // initialize columns of DT
        for dt in &mut self.cfg.data_tables {
            warn!("Initializing columns of table: {}", dt.name);
            let res = self.db.query_iter(format!("SHOW COLUMNS FROM {dt_name}", dt_name=dt.name))?;
            self.qtrans.stats.nqueries+=1;
            for row in res {
                let vals = row.unwrap().unwrap();
                if vals.len() < 1 {
                    return Ok(w.error(ErrorKind::ER_BAD_DB_ERROR, &"No columns in table".as_bytes())?)
                }
                let colname = helpers::mysql_val_to_parser_val(&vals[0]).to_string(); 
                if !dt.user_cols.iter().any(|uc| *uc == helpers::trim_quotes(&colname)) {
                    dt.data_cols.push(colname);
                }
            }
        }
        Ok(w.ok()?)
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        if !self.test_params.parse {
            self.qtrans.stats.nqueries+=1;
            warn!("on_query: {}", query);
            return answer_rows(results, self.db.query_iter(query));
        }

        let stmt_ast = self.sqlcache.get_single_parsed_stmt(&query.to_string())?;
        if !self.test_params.translate {
            self.qtrans.stats.nqueries+=1;
            warn!("on_query: {}", stmt_ast);
            return answer_rows(results, self.db.query_iter(stmt_ast.to_string()));
        }

        let mut txn = self.db.start_transaction(mysql::TxOpts::default())?;
        let mv_stmt = self.qtrans.get_mv_stmt(&stmt_ast, &mut txn)?;
        self.qtrans.stats.nqueries+=1;
        warn!("on_query: {}", mv_stmt);
        let res = answer_rows(results, txn.query_iter(mv_stmt.to_string()));
        match res {
            Err(e) => return Err(e),
            Ok(()) => {
                txn.commit()?;
                return Ok(());
            }
        }
    }
}

fn answer_rows<W: io::Write>(
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

fn mysql_val_to_common_val(val: &mysql::Value) -> mysql_common::value::Value {
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
