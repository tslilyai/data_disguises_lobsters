use mysql::prelude::*;
use sql_parser::ast::*;
use super::{helpers, ghosts_cache, config, stats, views};
use std::*;
use std::time::Duration;
use std::sync::atomic::{AtomicU64};
use msql_srv::{QueryResultWriter};
use log::{warn, debug};

pub struct QueryTransformer {
    pub cfg: config::Config,
    pub ghosts_cache: ghosts_cache::GhostsCache,
    
    latest_uid: AtomicU64,
    views: views::Views,
    
    // for tests
    params: super::TestParams,
    cur_stat: stats::QueryStat,
    pub stats: Vec<stats::QueryStat>,
}

impl QueryTransformer {
    pub fn new(cfg: &config::Config, params: &super::TestParams) -> Self {
        QueryTransformer{
            views: views::Views::new(),
            cfg: cfg.clone(),
            ghosts_cache: ghosts_cache::GhostsCache::new(),
            latest_uid: AtomicU64::new(0),
            params: params.clone(),
            cur_stat: stats::QueryStat::new(),
            stats: vec![],
        }
    }   

    fn issue_stmt (
        &mut self, 
        stmt: &Statement, 
        txn: &mut mysql::Transaction) 
        -> Result<Statement, io::Error>
    {
        // TODO consistency?
        let (results, is_write) = self.views.query_view(stmt)?;
        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
            }
            Statement::CreateView(CreateViewStatement{
                name,
                columns,
                with_options,
                query,
                if_exists,
                temporary,
                materialized,
            }) => {
            }
            Statement::CreateTable(CreateTableStatement{
                name,
                columns,
                constraints,
                indexes,
                with_options,
                if_not_exists,
                engine,
            }) => {
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
            }
            Statement::ShowObjects(ShowObjectsStatement{
                object_type,
                from,
                extended,
                full,
                materialized,
                filter,
            }) => {
            }
            /* TODO Handle Statement::Explain(stmt) => f.write_node(stmt)
             *
             * TODO Currently don't support alterations that reset autoincrement counters
             * Assume that deletions leave autoincrement counters as monotonically increasing
             *
             * Don't handle CreateSink, CreateSource, Copy,
             *  ShowCreateSource, ShowCreateSink, Tail, Explain
             * 
             * Don't modify queries for CreateSchema, CreateDatabase, 
             * ShowDatabases, ShowCreateTable, DropDatabase, Transactions,
             * ShowColumns, SetVariable (mysql exprs in set var not supported yet)
             *
             * XXX: ShowVariable, ShowCreateView and ShowCreateIndex will return 
             *  queries that used the materialized views, rather than the 
             *  application-issued tables. This is probably not a big issue, 
             *  since these queries are used to create the table again?
             *
             * XXX: SHOW * from users will not return any ghost users in ghostusersMV
             * */
            _ => {
            }
        }
        Ok(stmt.clone())
    }

    pub fn record_query_stats(&mut self, qtype: stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries+=self.ghosts_cache.nqueries;
        self.cur_stat.duration = dur;
        self.cur_stat.qtype = qtype;
        self.stats.push(self.cur_stat.clone());
        self.cur_stat.clear();
        self.ghosts_cache.nqueries = 0;
    }

    pub fn query<W: io::Write>(
        &mut self, 
        writer: QueryResultWriter<W>, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error>
    {
        helpers::answer_rows(writer, db.query_iter(stmt.to_string()))
    }

    pub fn query_drop(
        &mut self, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        db.query_drop(stmt.to_string())
    }

    pub fn unsubscribe(&mut self, uid: u64, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        self.cur_stat.qtype = stats::QueryType::Unsub;

        // check if already unsubscribed
        if !self.ghosts_cache.unsubscribe(uid) {
            return Ok(())
        }
        /*let mut txn = db.start_transaction(mysql::TxOpts::default())?;
        let uid_val = Value::Number(uid.to_string());
                    
        let vals_vec : Vec<Vec<Expr>> = self.ghosts_cache.get_gids_for_uid(uid, &mut txn)?
            .iter()
            .map(|g| vec![Expr::Value(Value::Number(g.to_string()))])
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
        let mv_table_name = self.mvtrans.objname_to_mv_objname(&user_table_name);
 
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
        self.cur_stat.nqueries+=1;
        
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
        self.cur_stat.nqueries+=1;
 
        /* 
         * 3. Change all entries with this UID to use the correct GID in the MV
         */
        for dt in &self.cfg.data_tables {
            let dtobjname = helpers::string_to_objname(&dt.name);
            let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &dtobjname);

            let mut assignments : Vec<String> = vec![];
            for uc in ucols {
                let uc_dt_ids = helpers::string_to_idents(&uc);
                let uc_mv_ids = self.mvtrans.idents_to_mv_idents(&uc_dt_ids);
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
           
            let mut select_constraint = Expr::Value(Value::Boolean(true));
            // add constraint on non-user columns to be identical (performing a "JOIN" on the DT
            // and the MV so the correct rows are joined together)
            // XXX could put a constraint selecting rows only with the UID in a ucol
            // but the assignment CASE should already handle this?
            for col in &dt.data_cols {
                let mut fullname = dt.name.clone();
                fullname.push_str(".");
                fullname.push_str(&col);
                let dt_ids = helpers::string_to_idents(&fullname);
                let mv_ids = self.mvtrans.idents_to_mv_idents(&dt_ids);

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
                self.mvtrans.objname_to_mv_objname(&dtobjname).to_string(),
                dtobjname.to_string(),
                astr,
                select_constraint.to_string(),
            );
                
            warn!("unsub: {}", update_dt_stmt);
            txn.query_drop(format!("{}", update_dt_stmt))?;
            self.cur_stat.nqueries+=1;
        }
        
        // TODO return some type of auth token?
        txn.commit()?;
        */
        Ok(())
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     * TODO check that user doesn't already exist
     */
    pub fn resubscribe(&mut self, uid: u64, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        // TODO check auth token?
        self.cur_stat.qtype = stats::QueryType::Resub;

        // check if already resubscribed
        if !self.ghosts_cache.resubscribe(uid) {
            return Ok(())
        }

        /*
        let mut txn = db.start_transaction(mysql::TxOpts::default())?;
        let uid_val = Value::Number(uid.to_string());

        let gid_exprs : Vec<Expr> = self.ghosts_cache.get_gids_for_uid(uid, &mut txn)?
            .iter()
            .map(|g| Expr::Value(Value::Number(g.to_string())))
            .collect();

        let user_table_name = helpers::string_to_objname(&self.cfg.user_table.name);
        let mv_table_name = self.mvtrans.objname_to_mv_objname(&user_table_name);

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
        self.cur_stat.nqueries+=1;

        /*
         * 2. Add user to users/usersmv (only one table)
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
        self.cur_stat.nqueries+=1;
 
        /* 
         * 3. update assignments in MV to use UID again
         */
        for dt in &self.cfg.data_tables {
            let dtobjname = helpers::string_to_objname(&dt.name);
            let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &dtobjname);
            
            let mut assignments : Vec<String> = vec![];
            for uc in ucols {
                let uc_dt_ids = helpers::string_to_idents(&uc);
                let uc_mv_ids = self.mvtrans.idents_to_mv_idents(&uc_dt_ids);
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
           
            let mut select_constraint = Expr::Value(Value::Boolean(true));
            // add constraint on non-user columns to be identical (performing a "JOIN" on the DT
            // and the MV so the correct rows are joined together)
            // XXX could put a constraint selecting rows only with the GIDs in a ucol
            // but the assignment CASE should already handle this?
            for col in &dt.data_cols {
                let mut fullname = dt.name.clone();
                fullname.push_str(".");
                fullname.push_str(&col);
                let dt_ids = helpers::string_to_idents(&fullname);
                let mv_ids = self.mvtrans.idents_to_mv_idents(&dt_ids);

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
                self.mvtrans.objname_to_mv_objname(&dtobjname).to_string(),
                dtobjname.to_string(),
                astr,
                select_constraint.to_string(),
            );
            warn!("resub: {}", update_dt_stmt);
            txn.query_drop(format!("{}", update_dt_stmt))?;
            self.cur_stat.nqueries+=1;
        }    
        txn.commit()?;*/
        Ok(())
    }
} 
