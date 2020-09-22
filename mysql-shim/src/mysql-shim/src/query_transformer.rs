use mysql::prelude::*;
use std::collections::HashMap;
use sql_parser::ast::*;
use super::config;
use super::helpers;
use super::mv_transformer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::*;

static LATEST_GID : AtomicU64 = AtomicU64::new(super::GHOST_ID_START);

pub struct QueryTransformer {
    cfg: config::Config,
    mv_trans: mv_transformer::MVTransformer,
}

impl QueryTransformer {
    pub fn new(cfg: config::Config) -> Self {
        // better way than simply replicating?
        let mv_trans = mv_transformer::MVTransformer::new(&cfg);
        QueryTransformer{cfg, mv_trans}
    }   

    /* 
     * WRAPPERS AROUND MV FUNCTIONS INVOKED BY SUPER 
     */
    pub fn objname_to_mv_string(&self, obj: &ObjectName) -> String {
        self.mv_trans.objname_to_mv_string(obj)
    }

    pub fn objname_to_mv_objname(&self, obj: &ObjectName) -> ObjectName {
        self.mv_trans.objname_to_mv_objname(obj)
    }
 
    pub fn idents_to_mv_idents(&self, obj: &Vec<Ident>) -> Vec<Ident> {
        self.mv_trans.idents_to_mv_idents(obj)
    }

    pub fn query_to_mv_query(&self, query: &Query) -> Query {
        self.mv_trans.query_to_mv_query(query)
    }
 
    pub fn expr_to_mv_expr(&self, expr: &Expr) -> Expr {
        self.mv_trans.expr_to_mv_expr(expr)
    }
     
    pub fn stmt_to_mv_stmt(&mut self, stmt: &Statement, db: &mut mysql::Conn) -> Result<(Statement, bool /*is_write*/), mysql::Error> {
        self.mv_trans.stmt_to_mv_stmt(stmt, db)
    }
      
    /*
     * DATATABLE QUERY TRANSFORMER FUNCTIONS
     */
    fn get_uid2gids_for_uids(&self, uids_to_match: Vec<Expr>, db: &mut mysql::Conn)
        -> Result<HashMap<Value, Vec<Expr>>, mysql::Error> 
    {
        let get_gids_stmt_from_ghosts = Query::select(Select{
            distinct: true,
            projection: vec![
                SelectItem::Expr{
                    expr: Expr::Identifier(helpers::string_to_objname(&super::GHOST_USER_COL).0),
                    alias: None,
                },
                SelectItem::Expr{
                    expr: Expr::Identifier(helpers::string_to_objname(&super::GHOST_ID_COL).0),
                    alias: None,
                }
            ],
            from: vec![TableWithJoins{
                relation: TableFactor::Table{
                    name: helpers::string_to_objname(&super::GHOST_TABLE_NAME),
                    alias: None,
                },
                joins: vec![],
            }],
            selection: Some(Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(&super::GHOST_USER_COL))),
                list: uids_to_match,
                negated: false,
            }),
            group_by: vec![],
            having: None,
        });

        let mut uid_to_gids : HashMap<Value, Vec<Expr>> = HashMap::new();
        let res = db.query_iter(format!("{}", get_gids_stmt_from_ghosts.to_string()))?;
        for row in res {
            let vals : Vec<Value> = row.unwrap().unwrap()
                .iter()
                .map(|v| helpers::mysql_val_to_parser_val(&v))
                .collect();
            match uid_to_gids.get_mut(&vals[0]) {
                Some(gids) => (*gids).push(Expr::Value(vals[1].clone())),
                None => {
                    uid_to_gids.insert(vals[0].clone(), vec![Expr::Value(vals[1].clone())]);
                }
            }
        }
        Ok(uid_to_gids)
    }

    fn insert_gid_for_uid(&self, uid: &Expr, db: &mut mysql::Conn) -> Result<u64, mysql::Error> {
        // user ids are always ints
        let res = db.query_iter(&format!("INSERT INTO {} ({}) VALUES ({});", 
                                         super::GHOST_TABLE_NAME, super::GHOST_USER_COL, uid))?;
        // we want to insert the GID in place
        // of the UID
        let gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
        
        // update the last known GID
        LATEST_GID.fetch_max(gid, Ordering::SeqCst);
        Ok(gid)
    }
    
    /* 
     * This issues the specified query to the MVs, and returns a VALUES query that
     * represents the values retrieved by the query to the MVs.
     * NOTE: queries are read-only operations (whereas statements may be writes)
     */
    fn query_to_datatable_query(&mut self, query: &Query, db: &mut mysql::Conn) -> Result<Query, mysql::Error> {
        let mv_q = self.mv_trans.query_to_mv_query(query);
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        let res = db.query_iter(&mv_q.to_string())?;
        for row in res {
            let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
            vals_vec.push(mysql_vals
                .iter()
                .map(|val| Expr::Value(helpers::mysql_val_to_parser_val(&val)))
                .collect());
        }
        Ok(Query {
            ctes: vec![],
            body: SetExpr::Values(Values(vals_vec)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        })
    }

    /*
     * This changes any nested queries to the corresponding VALUE 
     * (read from the MVs), if any exist.
     *
     * If ucols_to_replace is nonempty, the function sets whether 
     * any of these cols are contained within the query 
     */
    fn expr_to_datatable_expr(&mut self, expr: &Expr, db: &mut mysql::Conn, 
                              contains_ucol_id: &mut bool, ucols_to_replace: &Vec<String>) 
        -> Result<Expr, mysql::Error> 
    {
        let new_expr = match expr {
            Expr::Identifier(_ids) => {
                *contains_ucol_id |= ucols_to_replace.iter().any(|uc| uc.contains(&expr.to_string()));
                expr.clone()
            }
            Expr::QualifiedWildcard(_ids) => {
                *contains_ucol_id |= ucols_to_replace.iter().any(|uc| uc.contains(&expr.to_string()));
                expr.clone()
            }
            Expr::FieldAccess {
                expr,
                field,
            } => {
                // XXX we might be returning TRUE for contains_ucol_id if only the expr matches,
                // but not the field
                Expr::FieldAccess {
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_query = self.query_to_datatable_query(&subquery, db)?;
                // otherwise just return table column IN subquery
                Expr::InSubquery {
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    subquery: Box::new(new_query),
                    negated: *negated,
                }                
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let new_low = self.expr_to_datatable_expr(&low, db, contains_ucol_id, ucols_to_replace)?;
                let new_high = self.expr_to_datatable_expr(&high, db, contains_ucol_id, ucols_to_replace)?;
                Expr::Between {
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    negated: *negated,
                    low: Box::new(new_low),
                    high: Box::new(new_high),
                }
            }
            Expr::BinaryOp{
                left,
                op,
                right
            } => {
                let new_left = self.expr_to_datatable_expr(&left, db, contains_ucol_id, ucols_to_replace)?;
                let new_right = self.expr_to_datatable_expr(&right, db, contains_ucol_id, ucols_to_replace)?;
                Expr::BinaryOp{
                    left: Box::new(new_left),
                    op: op.clone(),
                    right: Box::new(new_right),
                }
            }
            Expr::UnaryOp{
                op,
                expr,
            } => Expr::UnaryOp{
                op: op.clone(),
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::Row{
                    exprs: new_exprs,
                }
            }
            Expr::Function(f) => Expr::Function(Function{
                name: f.name.clone(),
                args: match &f.args {
                    FunctionArgs::Star => FunctionArgs::Star,
                    FunctionArgs::Args(exprs) => {
                        let mut new_exprs = vec![];
                        for e in exprs {
                            new_exprs.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_datatable_expr(&filt, db, contains_ucol_id, ucols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_datatable_expr(&obe.expr, db, contains_ucol_id, ucols_to_replace)?,
                                asc: obe.asc.clone(),
                            });
                        }
                        Some(WindowSpec{
                            partition_by: new_pb,
                            order_by: new_ob,
                            window_frame: ws.window_frame.clone(),
                        })
                    }
                    None => None,
                },
                distinct: f.distinct,
            }),
            Expr::Case{
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut new_cond = vec![];
                for e in conditions {
                    new_cond.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                }
            }
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_datatable_query(&q, db)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_datatable_query(&q, db)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_datatable_expr(&left, db, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right, db)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_datatable_expr(&left, db, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right, db)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                subscript: Box::new(self.expr_to_datatable_expr(&subscript, db, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_datatable_expr(&e, db, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    positions: new_pos,
                }
            }
            _ => expr.clone(),
        };
        Ok(new_expr)
    }

    fn vals_vec_to_datatable_vals(&mut self, vals_vec: &Vec<Vec<Expr>>, ucol_indices: &Vec<usize>, db: &mut mysql::Conn) 
        -> Result<Option<Vec<Vec<Expr>>>, mysql::Error>
    {
        if ucol_indices.is_empty() {
            return Ok(Some(vals_vec.to_vec()));
        }         
        let mut parser_val_tuples = vec![];
        for row in vals_vec {
            let mut parser_vals : Vec<Expr> = vec![];
            for i in 0..row.len() {
                let mut val = row[i].clone();
                // add entry to ghosts table
                if ucol_indices.contains(&i) {
                    // NULL check: don't add ghosts entry if new UID value is NULL
                    if val != Expr::Value(Value::Null) {
                        let gid = self.insert_gid_for_uid(&row[i], db)?;
                        val = Expr::Value(Value::Number(format!("{}", gid)));
                    }
                }
                // add to vector of values for this row
                parser_vals.push(val);
            }
            parser_val_tuples.push(parser_vals);
        }
        Ok(Some(parser_val_tuples))
    }

    fn selection_to_datatable_selection(&mut self, selection: &Option<Expr>, db: &mut mysql::Conn, 
                                        table_name: &ObjectName, ucols: &Vec<String>) 
        -> Result<Option<Expr>, mysql::Error>
    {
        let mut qt_selection = None;
        let mut contains_ucol_id = false;
        if let Some(s) = selection {
            // check if the expr contains any conditions on user columns
            qt_selection = Some(self.expr_to_datatable_expr(&s, db, &mut contains_ucol_id, &ucols)?);

            // if a user column is being used as a selection criteria, first perform a 
            // select of all UIDs of matching rows in the MVs
            if contains_ucol_id {
                // get the matching rows from the MVs 
                let mv_select_stmt = Statement::Select(SelectStatement {
                        query: Box::new(Query::select(Select{
                        distinct: true,
                        projection: vec![SelectItem::Wildcard],
                        from: vec![TableWithJoins{
                            relation: TableFactor::Table{
                                name: self.mv_trans.objname_to_mv_objname(table_name),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: Some(self.mv_trans.expr_to_mv_expr(&s)),
                        group_by: vec![],
                        having: None,
                    })),
                    as_of: None,
                });

                // collect row results from MV
                let mut uids = vec![];
                let mut rows : Vec<Vec<mysql::Value>> = vec![];
                let mut cols = vec![];
                let res = db.query_iter(format!("{}", mv_select_stmt.to_string()))?;
                for row in res {
                    let row = row.unwrap();
                    cols = row.columns_ref().to_vec();
                    
                    let mut row_vals = vec![];
                    for i in 0..cols.len() {
                        // if it's a user column, add restriction on GID
                        let colname = cols[i].name_str().to_string();
 
                        // Add condition on user column to be within relevant GIDs mapped
                        // to by the UID value
                        // However, only update with GIDs if UID value is NOT NULL
                        if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) && row[i] != mysql::Value::NULL {
                            uids.push(row[i].clone());
                        }
                        row_vals.push(row[i].clone());
                    }
                    rows.push(row_vals);
                }

                // get all the gid rows corresponding to uids
                // TODO deal with potential GIDs in user_cols due to
                // unsubscriptions/resubscriptions
                let uid_to_gids = self.get_uid2gids_for_uids(
                    uids.iter()
                        .map(|uid| Expr::Value(helpers::mysql_val_to_parser_val(&uid)))
                        .collect(), 
                    db)?;

                // expr to constrain to select a particular row
                let mut or_row_constraint_expr = Expr::Value(Value::Boolean(false));
                for row in rows {
                    let mut and_col_constraint_expr = Expr::Value(Value::Boolean(true));
                    for i in 0..cols.len() {
                        // if it's a user column, add restriction on GID
                        let colname = cols[i].name_str().to_string();
                        let parser_val = helpers::mysql_val_to_parser_val(&row[i]);
 
                        // Add condition on user column to be within relevant GIDs mapped
                        // to by the UID value
                        // However, only update with GIDs if UID value is NOT NULL
                        if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) && row[i] != mysql::Value::NULL {
                            // add condition on user column to be within the relevant GIDs
                            and_col_constraint_expr = Expr::BinaryOp {
                                left: Box::new(and_col_constraint_expr),
                                op: BinaryOperator::And,
                                right: Box::new(Expr::InList {
                                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                    list: match uid_to_gids.get(&parser_val) {
                                        Some(gids) => gids.clone(),
                                        None => vec![],
                                    },
                                    negated: false,
                                }),
                            };

                        } else {

                            // otherwise, we just want to constrain the row value to the
                            // actual value returned
                            and_col_constraint_expr = Expr::BinaryOp {
                                left: Box::new(and_col_constraint_expr),
                                op: BinaryOperator::And,
                                right: Box::new(Expr::BinaryOp{
                                    left: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(Expr::Value(parser_val)),
                                }),             
                            };
                        }
                    }

                    // we allow the selection to match ANY returned row
                    or_row_constraint_expr = Expr::BinaryOp {
                        left: Box::new(or_row_constraint_expr),
                        op: BinaryOperator::Or,
                        right: Box::new(and_col_constraint_expr),
                    };
                }
                qt_selection = Some(or_row_constraint_expr);
            } 
        } 
        return Ok(qt_selection);
    }

    pub fn stmt_to_datatable_stmt(&mut self, stmt: &Statement, db: &mut mysql::Conn) -> Result<Option<Statement>, mysql::Error> {
        let mut qt_stmt = stmt.clone();

        match stmt {
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                /* note that if the table is the users table,
                 * we just want to insert like usual; we only care about
                 * adding ghost ids for data tables, but we don't add ghosts to
                 * the user table
                 */

                /* For all columns that are user columns, generate a new ghost_id and insert
                     into ghosts table with appropriate user_id value
                     those as the values instead for those columns.
                    This will be empty if the table is the user table, or not a datatable
                 */
                let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &table_name);
                let mut ucol_indices = vec![];
                // get indices of columns corresponding to user vals
                if !ucols.is_empty() {
                    for (i, c) in columns.into_iter().enumerate() {
                        // XXX this may overcount if a non-user column is a suffix of a user
                        // column
                        if ucols.iter().any(|uc| helpers::str_ident_match(&c.to_string(), uc)) {
                            ucol_indices.push(i);
                        }
                    }
                }

                // update sources
                let mut qt_source = source.clone();
                /* if no user columns, change sources to use MV
                 * otherwise, we need to insert new GID->UID mappings 
                 * with the values of the usercol value as the UID
                 * and then set the GID as the new source value of the usercol 
                 * */
                match source {
                    InsertSource::Query(q) => {
                        let mut contains_ucol_id = false;
                        match &q.body {
                            SetExpr::Values(Values(expr_vals)) => {
                                // NOTE: only need to modify values if we're dealing with a DT,
                                // could perform check here rather than calling vals_vec
                                let mut vals_vec : Vec<Vec<Expr>>= vec![];
                                for row in expr_vals {
                                    let mut vals_row : Vec<Expr>= vec![];
                                    for val in row {
                                        let query_val = match self.expr_to_datatable_expr(&val, db, &mut contains_ucol_id, &vec![])? {
                                            Expr::Subquery(q) => {
                                                match q.body {
                                                    SetExpr::Values(Values(subq_exprs)) => {
                                                        assert_eq!(subq_exprs.len(), 1);
                                                        assert_eq!(subq_exprs[0].len(), 1);
                                                        subq_exprs[0][0].clone()
                                                    }
                                                    _ => unimplemented!("query_to_data_query should only return a Value"),
                                                }
                                            }
                                            _ => val.clone(),
                                        };
                                        vals_row.push(self.expr_to_datatable_expr(&query_val, db, &mut contains_ucol_id, &vec![])?);
                                    }
                                    vals_vec.push(vals_row);
                                }
                                if let Some(vv) = self.vals_vec_to_datatable_vals(&vals_vec, &ucol_indices, db)? {
                                    let mut new_q = q.clone();
                                    new_q.body = SetExpr::Values(Values(vv));
                                    qt_source = InsertSource::Query(new_q);
                                } else {
                                    return Ok(None);
                                }
                            }
                            _ => {
                                // we need to issue q to MVs to get rows that will be set as values
                                // regardless of whether this is a DT or not (because query needs
                                // to read from MV, rather than initially specified tables)
                                let mv_q = self.mv_trans.query_to_mv_query(q);
                                let mut vals_vec : Vec<Vec<Expr>>= vec![];
                                let mut res = db.query_iter(&mv_q.to_string());
                                match res {
                                    Ok(ref mut rows) => {
                                        for row in rows {
                                            let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                                            vals_vec.push(mysql_vals
                                                .iter()
                                                .map(|val| Expr::Value(helpers::mysql_val_to_parser_val(&val)))
                                                .collect());
                                        }
                                    }
                                    Err(_e) => {
                                        return Ok(None);
                                    }
                                }
                                drop(res);

                                if let Some(vv) = self.vals_vec_to_datatable_vals(&vals_vec, &ucol_indices, db)? {
                                    let mut new_q = q.clone();
                                    new_q.body = SetExpr::Values(Values(vv));
                                    qt_source = InsertSource::Query(new_q);
                                } else {
                                    return Ok(None);
                                }
                            }    
                        }
                    }
                    InsertSource::DefaultValues => (), // TODO might have to get rid of this
                }
                qt_stmt = Statement::Insert(InsertStatement{
                    table_name: table_name.clone(),
                    columns : columns.clone(),
                    source : qt_source, 
                });
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &table_name);
                let mut contains_ucol_id = false;
                let mut ucol_assigns = vec![];
                let mut ucol_selectitems_assn = vec![];
                let mut qt_assn = vec![];

                for a in assignments {
                    // we still want to perform the update BUT we need to make sure that the updated value, if a 
                    // expr with a query, reads from the MV rather than the datatables

                    let new_val = self.expr_to_datatable_expr(&a.value, db, &mut contains_ucol_id, &vec![])?;
                                        
                    // we won't replace any UIDs when converting assignments to values, but
                    // we also want to update any usercol value to NULL if the UID is being set to NULL, so we put it
                    // in qt_assn too (rather than only updating the GID)
                    let is_ucol = ucols.iter().any(|uc| helpers::str_ident_match(&a.id.to_string(), uc));
                    if !is_ucol || new_val == Expr::Value(Value::Null) {
                        qt_assn.push(Assignment{
                            id: a.id.clone(),
                            value: new_val.clone(),
                        });
                    } 
                    // if we have an assignment to a UID, we need to update the GID->UID mapping
                    // instead of updating the actual data table record
                    // note that we still include NULL entries so we know to delete this GID
                    if is_ucol {
                        ucol_assigns.push(Assignment {
                            id: a.id.clone(),
                            value: new_val.clone(),
                        });
                        ucol_selectitems_assn.push(SelectItem::Expr{
                            expr: Expr::Identifier(vec![a.id.clone()]),
                            alias: None,
                        });
                    }
                }

                let qt_selection = self.selection_to_datatable_selection(selection, db, &table_name, &ucols)?;
             
                // if usercols are being updated, query DT to get the relevant
                // GIDs and update these GID->UID mappings in the ghosts table
                if !ucol_assigns.is_empty() {
                    let get_gids_stmt_from_dt = Statement::Select(SelectStatement {
                        query: Box::new(Query::select(Select{
                            distinct: true,
                            projection: ucol_selectitems_assn,
                            from: vec![TableWithJoins{
                                relation: TableFactor::Table{
                                    name: table_name.clone(),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: qt_selection.clone(),
                            group_by: vec![],
                            having: None,
                        })),
                        as_of: None,
                    });
                    // get the user_col GIDs from the datatable
                    let res = db.query_iter(format!("{}", get_gids_stmt_from_dt.to_string()))?;
                    let mut ghost_update_stmts = vec![];
                    for row in res {
                        let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                        for (i, uc_val) in ucol_assigns.iter().enumerate() {
                            let gid = helpers::mysql_val_to_parser_val(&mysql_vals[i]);
                            // delete the GID entry if it is being set to NULL
                            if uc_val.value == Expr::Value(Value::Null) {
                                ghost_update_stmts.push(Statement::Delete(DeleteStatement {
                                    table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                                    selection: Some(Expr::BinaryOp{
                                        left: Box::new(Expr::Identifier(helpers::string_to_idents(super::GHOST_ID_COL))),
                                        op: BinaryOperator::Eq,
                                        right: Box::new(Expr::Value(Value::Number(format!("{}", gid)))),
                                    }),
                                }));
                            } else {
                                // otherwise, update GID entry with new UID value
                                // XXX what if the value IS a GID??? should we just remove this GID?
                                ghost_update_stmts.push(Statement::Update(UpdateStatement {
                                    table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                                    assignments: vec![Assignment{
                                        id: Ident::new(super::GHOST_USER_COL),
                                        value: uc_val.value.clone(),
                                    }],
                                    selection: Some(Expr::BinaryOp{
                                        left: Box::new(Expr::Identifier(helpers::string_to_idents(super::GHOST_ID_COL))),
                                        op: BinaryOperator::Eq,
                                        right: Box::new(Expr::Value(Value::Number(format!("{}", gid)))),
                                    }),
                                }));
                            }
                        }
                    }
                    for gstmt in ghost_update_stmts {
                        db.query_drop(format!("{}", gstmt.to_string()))?;
                    }
                }
                qt_stmt = Statement::Update(UpdateStatement{
                    table_name: table_name.clone(),
                    assignments : qt_assn,
                    selection : qt_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &table_name);
                let qt_selection = self.selection_to_datatable_selection(selection, db, &table_name, &ucols)?;

                let ucol_selectitems = ucols.iter()
                    .map(|uc| SelectItem::Expr{
                        expr: Expr::Identifier(helpers::string_to_idents(uc)),
                        alias: None,
                    })
                    .collect();
               
                // get the list of GIDs to delete from the ghosts table 
                let select_gids_stmt = Statement::Select(SelectStatement {
                        query: Box::new(Query::select(Select{
                                distinct: true,
                                projection: ucol_selectitems,
                                from: vec![TableWithJoins{
                                    relation: TableFactor::Table{
                                        name: table_name.clone(),
                                        alias: None,
                                    },
                                    joins: vec![],
                                }],
                                selection: qt_selection.clone(),
                                group_by: vec![],
                                having: None,
                            })),        
                        as_of: None,
                });
                let res = db.query_iter(format!("{}", select_gids_stmt.to_string()))?;
                let mut gids_list : Vec<Expr>= vec![];
                for row in res {
                    for val in row.unwrap().unwrap() {
                        gids_list.push(Expr::Value(helpers::mysql_val_to_parser_val(&val)));
                    }
                }

                // delete from ghosts table if GIDs are removed
                // TODO we might want to keep this around if data is "restorable"
                if !ucols.is_empty() {
                    let ghosts_delete_statement = Statement::Delete(DeleteStatement{
                        table_name: helpers::string_to_objname(&super::GHOST_TABLE_NAME),
                        selection: Some(Expr::InList{
                            expr: Box::new(Expr::Identifier(helpers::string_to_idents(&super::GHOST_ID_COL))),
                            list: gids_list,
                            negated: false,
                        }),
                    });
                    db.query_drop(&ghosts_delete_statement.to_string())?;
                }

                qt_stmt = Statement::Delete(DeleteStatement{
                    table_name: table_name.clone(),
                    selection : qt_selection,
                });
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
                let qt_query = self.query_to_datatable_query(&query, db)?;
                qt_stmt = Statement::CreateView(CreateViewStatement{
                    name: name.clone(),
                    columns: columns.clone(),
                    with_options: with_options.clone(),
                    query : Box::new(qt_query),
                    if_exists: if_exists.clone(),
                    temporary: temporary.clone(),
                    materialized: materialized.clone(),
                });
            }
            _ => ()
        }
        return Ok(Some(qt_stmt));
    }
}
