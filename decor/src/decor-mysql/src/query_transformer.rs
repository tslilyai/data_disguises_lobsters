use mysql::prelude::*;
use sql_parser::ast::*;
use super::{helpers, ghosts_cache, config, mv_transformer, stats};
use std::sync::atomic::Ordering;
use std::*;
use std::time::Duration;
use std::sync::atomic::{AtomicU64};
use msql_srv::{QueryResultWriter};
use log::{warn, debug};

pub struct QueryTransformer {
    pub cfg: config::Config,
    pub cache: ghosts_cache::GhostsCache,
    
    mvtrans: mv_transformer::MVTransformer,
    latest_uid: AtomicU64,
    
    // for tests
    params: super::TestParams,
    cur_stat: stats::QueryStat,
    pub stats: Vec<stats::QueryStat>,
}

impl QueryTransformer {
    pub fn new(cfg: &config::Config, params: &super::TestParams) -> Self {
        QueryTransformer{
            cfg: cfg.clone(),
            mvtrans: mv_transformer::MVTransformer::new(cfg),
            cache: ghosts_cache::GhostsCache::new(),
            latest_uid: AtomicU64::new(0),
            params: params.clone(),
            cur_stat: stats::QueryStat::new(),
            stats: vec![],
        }
    }   

    /**************************************** 
     **** Converts Queries/Exprs to Values **
     ****************************************/
    /* 
     * This issues the specified query to the MVs, and returns a VALUES query that
     * represents the values retrieved by the query to the MVs.
     * NOTE: queries are read-only operations (whereas statements may be writes)
     */
    fn query_to_value_query(&mut self, query: &Query, txn: &mut mysql::Transaction) -> Result<Query, mysql::Error> {
        let mv_q = self.mvtrans.query_to_mv_query(query);
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        
        warn!("query_to_value_query: {}", mv_q);
        let res = txn.query_iter(&mv_q.to_string())?;
        self.cur_stat.nqueries+=1;

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
     * This issues the specified query to the MVs, and returns a VALUES row 
     */
    fn query_to_value_rows(&mut self, query: &Query, is_single_column: bool, txn: &mut mysql::Transaction) -> Result<Vec<Expr>, mysql::Error> {
        let mv_q = self.mvtrans.query_to_mv_query(query);
        let mut vals_vec : Vec<Expr>= vec![];
        
        warn!("query_to_value_rows: {}", mv_q);
        let res = txn.query_iter(&mv_q.to_string())?;
        self.cur_stat.nqueries+=1;

        for row in res {
            let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
            if is_single_column {
                if mysql_vals.len() != 1 {
                    return Err(mysql::Error::IoError(io::Error::new(io::ErrorKind::Other, format!("Query should only select one column"))));
                }
                vals_vec.push(Expr::Value(helpers::mysql_val_to_parser_val(&mysql_vals[0])));
            } else {
                vals_vec.push(Expr::Row{exprs:
                    mysql_vals
                    .iter()
                    .map(|v| Expr::Value(helpers::mysql_val_to_parser_val(v)))
                    .collect()
                });
            }
        }
        Ok(vals_vec)
    }

    /*
     * This changes any nested queries to the corresponding VALUE 
     * (read from the MVs), if any exist.
     */
    fn expr_to_value_expr(&mut self, expr: &Expr, txn: &mut mysql::Transaction, 
                              contains_ucol_id: &mut bool, 
                              ucols_to_replace: &Vec<String>) 
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
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_query = self.query_to_value_query(&subquery, txn)?;
                // otherwise just return table column IN subquery
                Expr::InSubquery {
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
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
                let new_low = self.expr_to_value_expr(&low, txn, contains_ucol_id, ucols_to_replace)?;
                let new_high = self.expr_to_value_expr(&high, txn, contains_ucol_id, ucols_to_replace)?;
                Expr::Between {
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
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
                let new_left = self.expr_to_value_expr(&left, txn, contains_ucol_id, ucols_to_replace)?;
                let new_right = self.expr_to_value_expr(&right, txn, contains_ucol_id, ucols_to_replace)?;
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
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(
                    self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
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
                            new_exprs.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_value_expr(&filt, txn, contains_ucol_id, ucols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_value_expr(&obe.expr, txn, contains_ucol_id, ucols_to_replace)?,
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
                    new_cond.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                }
            }
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_value_query(&q, txn)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_value_query(&q, txn)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_value_expr(&left, txn, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right, txn)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_value_expr(&left, txn, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right, txn)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                subscript: Box::new(self.expr_to_value_expr(&subscript, txn, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                    positions: new_pos,
                }
            }
            _ => expr.clone(),
        };
        Ok(new_expr)
    }

    /*
     * Try and convert to expr that uses GIDs instead of UIDs If expression matches the subset of
     * expressions accepted, returns Some(expr) else returns None 
     *
     * Note that expressions where we can guarantee that the constraint is on a non-user column are
     * also acceptable; nested or more complex expressions, however, are not.
     */
    fn fastpath_expr_to_gid_expr(&mut self, e: &Expr, txn: &mut mysql::Transaction, ucols_to_replace: &Vec<String>) 
        -> Result<Option<Expr>, mysql::Error> 
    {
        // if it's just an identifier, we can return if it's not a ucol
        debug!("\tFastpath expr: looking at {}", e);
        if helpers::expr_is_col(&e) && !helpers::expr_is_ucol(&e, ucols_to_replace) {
            return Ok(Some(e.clone()));
        }
        let mut new_expr = None;
        match e {
            // values are always ok!
            Expr::Value(_) => {new_expr = Some(e.clone());}
            
            // these are ok as long as the expr is a column
            Expr::Cast{expr, ..} | Expr::Collate{expr, ..} | Expr::IsNull{expr,..} => {
                if helpers::expr_is_col(&expr) {
                    new_expr = Some(e.clone());
                } 
            }
            
            // col [NOT] IN (values list)
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if helpers::expr_is_col(&expr) {
                    if helpers::expr_is_ucol(&expr, ucols_to_replace) {
                        // get all uids in the list
                        let mut uid_vals = vec![];
                        for e in list {
                            // values must be u64 for ucols
                            match helpers::parser_expr_to_u64(&e) {
                                Ok(v) => uid_vals.push(v),
                                Err(_) => return Ok(None),
                            }
                        }

                        // now change the uids to gids
                        let mut gid_exprs = vec![];
                        for (_uid, gids) in self.cache.get_gids_for_uids(&uid_vals, txn)? {
                            for g in gids {
                                gid_exprs.push(Expr::Value(Value::Number(g.to_string())));
                            }
                        }
                        new_expr = Some(Expr::InList {
                            expr: expr.clone(),
                            list: gid_exprs,
                            negated: *negated,
                        });
                    } else {
                       new_expr = Some(e.clone());
                    }
                }
            }
            
            // col IN subquery 
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if helpers::expr_is_col(expr) {
                    let vals_vec = self.query_to_value_rows(&subquery, true, txn)?;
                    if helpers::expr_is_ucol(expr, ucols_to_replace) {
                        // get all uids in the list
                        let mut uid_vals = vec![];
                        for e in vals_vec {
                            // values must be u64 for ucols
                            match helpers::parser_expr_to_u64(&e) {
                                Ok(v) => uid_vals.push(v),
                                Err(_) => return Ok(None),
                            }
                        }

                        // now change the uids to gids
                        let mut gid_exprs = vec![];
                        for (_uid, gids) in self.cache.get_gids_for_uids(&uid_vals, txn)? {
                            for g in gids {
                                gid_exprs.push(Expr::Value(Value::Number(g.to_string())));
                            }
                        }
                        new_expr = Some(Expr::InList {
                            expr: expr.clone(),
                            list: gid_exprs,
                            negated: *negated,
                        });
                    } else {
                        new_expr = Some(Expr::InList {
                            expr: expr.clone(),
                            list: vals_vec,
                            negated: *negated,
                        });
                    }
                }
            }
            
            // col BETWEEN numval1 numval2
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if helpers::expr_is_col(&expr) {
                    let lowu64 : u64;
                    let highu64 : u64;
                    match helpers::parser_expr_to_u64(&low) {
                        Ok(v) => {lowu64 = v;}
                        Err(_) => {return Ok(None);}
                    }
                    match helpers::parser_expr_to_u64(&high) {
                        Ok(v) => {highu64 = v;}
                        Err(_) => {return Ok(None);}
                    }
                    if helpers::expr_is_ucol(&expr, ucols_to_replace) {
                        // change the uids in the range to gids
                        let uid_vals : Vec<u64> = ops::RangeInclusive::new(lowu64, highu64).collect();
                        let mut gid_exprs = vec![];
                        for (_uid, gids) in self.cache.get_gids_for_uids(&uid_vals, txn)? {
                            for g in gids {
                                gid_exprs.push(Expr::Value(Value::Number(g.to_string())));
                            }
                        }
                        new_expr = Some(Expr::InList {
                            expr: expr.clone(),
                            list: gid_exprs,
                            negated: *negated,
                        });
                    } else {
                        new_expr = Some(e.clone());
                    }
                } 
            }
            Expr::BinaryOp{
                left,
                op,
                right
            } => {
                match op {
                    BinaryOperator::Eq | BinaryOperator::NotEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
                        if helpers::expr_is_col(&left) {
                            // ucol OP u64 val
                            if helpers::expr_is_ucol(&left, ucols_to_replace) {
                                if let Ok(v) = helpers::parser_expr_to_u64(&right) {
                                    let uid_vals : Vec<u64> = match op {
                                        BinaryOperator::Lt => (0..v).collect(),
                                        BinaryOperator::LtEq => ops::RangeInclusive::new(0, v).collect(),
                                        _ => vec![v],
                                    };
                                    let mut gid_exprs = vec![];
                                    for (_uid, gids) in self.cache.get_gids_for_uids(&uid_vals, txn)? {
                                        for g in gids {
                                            gid_exprs.push(Expr::Value(Value::Number(g.to_string())));
                                        }
                                    }
                                    new_expr = Some(Expr::InList{
                                        expr: left.clone(),
                                        list: gid_exprs, 
                                        negated: (*op == BinaryOperator::NotEq),
                                    });
                                }
                            // col OP val or non-ucol column
                            } else if !helpers::expr_is_ucol(&right, ucols_to_replace) 
                                && (helpers::expr_is_col(&right) 
                                    || helpers::expr_is_value(&right)) 
                            {
                                new_expr = Some(e.clone());
                            }
                        }
                    }
                    // col > / >= col or val 
                    //  XXX ucols not supported because potentially unbounded
                    BinaryOperator::Gt | BinaryOperator::GtEq => {
                        if !helpers::expr_is_ucol(&left, ucols_to_replace) 
                            && !helpers::expr_is_ucol(&right, ucols_to_replace) 
                            && (helpers::expr_is_col(&left) || helpers::expr_is_value(&left)) 
                            && (helpers::expr_is_col(&right) || helpers::expr_is_value(&right)) 
                        {
                                new_expr = Some(e.clone());
                        } 
                    }
                    _ => {
                        // all other ops are ops on nested (non-primitive) exprs
                        // NOTE: just column names won't pass fastpath because then there may be
                        // constraints supported like "ucol * col", which would lead to inaccurate results
                        // when ucol contains GIDs
                        let newleft = self.fastpath_expr_to_gid_expr(&left, txn, ucols_to_replace)?;
                        let newright = self.fastpath_expr_to_gid_expr(&right, txn, ucols_to_replace)?;
                        if newleft.is_some() && newright.is_some() {
                            new_expr = Some(Expr::BinaryOp{
                                left: Box::new(newleft.unwrap()),
                                op: op.clone(),
                                right: Box::new(newright.unwrap()),
                            });
                        }
                    }
                }
            }
            
            // +/-/not (valid fastpath expr)
            Expr::UnaryOp{
                op,
                expr,
            } => {
                if let Some(expr) = self.fastpath_expr_to_gid_expr(&expr, txn, ucols_to_replace)? {
                    new_expr = Some(Expr::UnaryOp{
                        op : op.clone(),
                        expr: Box::new(expr),
                    });
                }
            },

            // nested (valid fastpath expr)
            Expr::Nested(expr) => {
                if let Some(expr) = self.fastpath_expr_to_gid_expr(&expr, txn, ucols_to_replace)? {
                    new_expr = Some(Expr::Nested(Box::new(expr)));
                } 
            },
            
            // Row or List(valid fastpath exprs)
            Expr::Row{ exprs } | Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, txn, ucols_to_replace)? {
                        new_exprs.push(newe);
                    } else {
                        return Ok(None);
                    }
                }
                if let Expr::Row{..} = e {
                    new_expr = Some(Expr::Row{exprs: new_exprs});
                } else {
                    new_expr = Some(Expr::List(new_exprs));
                }
            }

            // query contains more than one value
            Expr::Exists(q) => {
                let vals = self.query_to_value_rows(&q, false, txn)?;
                if vals.len() > 0 {
                    new_expr = Some(Expr::Value(Value::Boolean(true)));
                } else {
                    new_expr = Some(Expr::Value(Value::Boolean(false)));
                }
            }
            
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let new_op = match operand {
                    Some(e) => self.fastpath_expr_to_gid_expr(&e, txn, ucols_to_replace)?,
                    None => None,
                };

                let mut new_cond = vec![];
                for e in conditions {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, txn, ucols_to_replace)? {
                        new_cond.push(newe);
                    }
                }
                let mut new_res= vec![];
                for e in results {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, txn, ucols_to_replace)? {
                        new_res.push(newe);
                    }
                }
                let new_end_res = match else_result {
                    Some(e) => self.fastpath_expr_to_gid_expr(&e, txn, ucols_to_replace)?,
                    None => None,
                };

                new_expr = Some(Expr::Case{
                    operand: match new_op {
                        Some(e) => Some(Box::new(e)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match new_end_res {
                        Some(e) => Some(Box::new(e)),
                        None => None,
                    },
                });
            }
            
            Expr::Subquery(q) => {
                new_expr = Some(Expr::Subquery(Box::new(self.query_to_value_query(&q, txn)?)));
            }
            
            Expr::Any {
                left,
                op,
                right,
            } => {
                if let Some(newleft) = self.fastpath_expr_to_gid_expr(&left, txn, ucols_to_replace)? {
                    new_expr = Some(Expr::Any{
                        left: Box::new(newleft),
                        op: op.clone(),
                        right: Box::new(self.query_to_value_query(&right, txn)?),
                    });
                }
            }
            
            Expr::All{
                left,
                op,
                right,
            } => {
                if let Some(newleft) = self.fastpath_expr_to_gid_expr(&left, txn, ucols_to_replace)? {
                    new_expr = Some(Expr::Any{
                        left: Box::new(newleft),
                        op: op.clone(),
                        right: Box::new(self.query_to_value_query(&right, txn)?),
                    });
                }
            }
            _ => (),
        }
        Ok(new_expr)
    }

    /* 
     * Convert all expressions to insert to primitive values
     */
    fn insert_source_query_to_values(&mut self, q: &Query, txn: &mut mysql::Transaction) 
        -> Result<Vec<Vec<Expr>>, mysql::Error> 
    {
        let mut contains_ucol_id = false;
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        match &q.body {
            SetExpr::Values(Values(expr_vals)) => {
                // NOTE: only need to modify values if we're dealing with a DT,
                // could perform check here rather than calling vals_vec
                for row in expr_vals {
                    let mut vals_row : Vec<Expr> = vec![];
                    for val in row {
                        let value_expr = self.expr_to_value_expr(&val, txn, &mut contains_ucol_id, &vec![])?;
                        match value_expr {
                            Expr::Subquery(q) => {
                                match q.body {
                                    SetExpr::Values(Values(subq_exprs)) => {
                                        assert_eq!(subq_exprs.len(), 1);
                                        assert_eq!(subq_exprs[0].len(), 1);
                                        vals_row.push(subq_exprs[0][0].clone());
                                    }
                                    _ => unimplemented!("query_to_data_query should only return a Value"),
                                }
                            }
                            _ => vals_row.push(value_expr),
                        }
                    }
                    vals_vec.push(vals_row);
                }
            }
            _ => {
                // we need to issue q to MVs to get rows that will be set as values
                // regardless of whether this is a DT or not (because query needs
                // to read from MV, rather than initially specified tables)
                let mv_q = self.mvtrans.query_to_mv_query(q);
                warn!("insert_source_q_to_values: {}", mv_q);
                let rows = txn.query_iter(&mv_q.to_string())?;
                self.cur_stat.nqueries+=1;
                for row in rows {
                    let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                    vals_vec.push(mysql_vals
                        .iter()
                        .map(|val| Expr::Value(helpers::mysql_val_to_parser_val(&val)))
                        .collect());
                }
            }
        }    
        Ok(vals_vec)
    }
      
    /*
     * DATATABLE QUERY TRANSFORMER FUNCTIONS
     */
    fn selection_to_datatable_selection(
        &mut self, 
        selection: &Option<Expr>, 
        txn: &mut mysql::Transaction, 
        table_name: &ObjectName, 
        ucols: &Vec<String>) 
        -> Result<Option<Expr>, mysql::Error>
    {
        let mut contains_ucol_id = false;
        let mut qt_selection = None;
        if let Some(s) = selection {
            // check if the expr can be fast-pathed
            if let Some(fastpath_expr) = self.fastpath_expr_to_gid_expr(&s, txn, &ucols)? {
                qt_selection = Some(fastpath_expr);
            } else {
                // check if the expr contains any conditions on user columns
                qt_selection = Some(self.expr_to_value_expr(&s, txn, &mut contains_ucol_id, &ucols)?);

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
                                    name: self.mvtrans.objname_to_mv_objname(table_name),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: Some(self.mvtrans.expr_to_mv_expr(&s)),
                            group_by: vec![],
                            having: None,
                        })),
                        as_of: None,
                    });

                    // collect row results from MV
                    let mut uids = vec![];
                    let mut rows : Vec<Vec<mysql::Value>> = vec![];
                    let mut cols = vec![];
                    warn!("selection_to_dt_selection: {}", mv_select_stmt);
                    let res = txn.query_iter(format!("{}", mv_select_stmt.to_string()))?;
                    self.cur_stat.nqueries+=1;
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
                            if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) 
                                && row[i] != mysql::Value::NULL 
                            {
                                uids.push(helpers::mysql_val_to_u64(&row[i])?);
                            }
                            row_vals.push(row[i].clone());
                        }
                        rows.push(row_vals);
                    }

                    // get all the gid rows corresponding to uids
                    // TODO deal with potential GIDs in user_cols due to
                    // unsubscriptions/resubscriptions
                    self.cache.cache_uid2gids_for_uids(&uids, txn)?;

                    // expr to constrain to select a particular row
                    let mut or_row_constraint_expr = Expr::Value(Value::Boolean(false));
                    for row in rows {
                        let mut and_col_constraint_expr = Expr::Value(Value::Boolean(true));
                        for i in 0..cols.len() {
                            // if it's a user column, add restriction on GID
                            let colname = cols[i].name_str().to_string();
                            // Add condition on user column to be within relevant GIDs mapped
                            // to by the UID value
                            // However, only update with GIDs if UID value is NOT NULL
                            if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) 
                                && row[i] != mysql::Value::NULL 
                            {
                                let uid = helpers::mysql_val_to_u64(&row[i])?;
                                // add condition on user column to be within the relevant GIDs
                                and_col_constraint_expr = Expr::BinaryOp {
                                    left: Box::new(and_col_constraint_expr),
                                    op: BinaryOperator::And,
                                    right: Box::new(Expr::InList {
                                        expr: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                        list: self.cache.get_gids_for_uid(uid, txn)?.iter()
                                                .map(|g| Expr::Value(Value::Number(g.to_string())))
                                                .collect(),
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
                                        right: Box::new(Expr::Value(helpers::mysql_val_to_parser_val(&row[i]))),
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
        } 
        return Ok(qt_selection);
    }
    
    fn issue_insert_datatable_stmt(&mut self, values: &mut Vec<Vec<Expr>>, stmt: InsertStatement, txn: &mut mysql::Transaction) 
        -> Result<(), mysql::Error> 
    {
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
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let mut ucol_indices = vec![];
        // get indices of columns corresponding to user vals
        if !ucols.is_empty() {
            for (i, c) in (&stmt.columns).into_iter().enumerate() {
                // XXX this may overcount if a non-user column is a suffix of a user
                // column
                if ucols.iter().any(|uc| helpers::str_ident_match(&c.to_string(), uc)) {
                    ucol_indices.push(i);
                }
            }
        }

        // update sources
        let mut qt_source = stmt.source.clone();
        /* if no user columns, change sources to use MV
         * otherwise, we need to insert new GID->UID mappings 
         * with the values of the usercol value as the UID
         * and then set the GID as the new source value of the usercol 
         * */
        match stmt.source {
            InsertSource::Query(q) => {
                self.cache.insert_uid2gids_for_values(values, &ucol_indices, txn)?;
                let mut new_q = q.clone();
                new_q.body = SetExpr::Values(Values(values.to_vec()));
                qt_source = InsertSource::Query(new_q);
            }
            InsertSource::DefaultValues => (),
        }
       
        let dt_stmt = Statement::Insert(InsertStatement{
            table_name: stmt.table_name.clone(),
            columns : stmt.columns.clone(),
            source : qt_source, 
        });
 
        warn!("issue_insert_dt_stmt: {}", dt_stmt);
        txn.query_drop(dt_stmt.to_string())?;
        self.cur_stat.nqueries+=1;

        Ok(())
    }
       
    fn issue_update_datatable_stmt(&mut self, assign_vals: &Vec<Expr>, stmt: UpdateStatement, txn: &mut mysql::Transaction)
        -> Result<(), mysql::Error> 
    {
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let mut ucol_assigns = vec![];
        let mut ucol_selectitems_assn = vec![];
        let mut qt_assn = vec![];

        for (i, a) in stmt.assignments.iter().enumerate() {
            // we still want to perform the update BUT we need to make sure that the updated value, if a 
            // expr with a query, reads from the MV rather than the datatables
                                
            // we won't replace any UIDs when converting assignments to values, but
            // we also want to update any usercol value to NULL if the UID is being set to NULL, so we put it
            // in qt_assn too (rather than only updating the GID)
            let is_ucol = ucols.iter().any(|uc| helpers::str_ident_match(&a.id.to_string(), uc));
            if !is_ucol || assign_vals[i] == Expr::Value(Value::Null) {
                qt_assn.push(Assignment{
                    id: a.id.clone(),
                    value: assign_vals[i].clone(),
                });
            } 
            // if we have an assignment to a UID, we need to update the GID->UID mapping
            // instead of updating the actual data table record
            // note that we still include NULL entries so we know to delete this GID
            if is_ucol {
                ucol_assigns.push(Assignment {
                    id: a.id.clone(),
                    value: assign_vals[i].clone(),
                });
                ucol_selectitems_assn.push(SelectItem::Expr{
                    expr: Expr::Identifier(vec![a.id.clone()]),
                    alias: None,
                });
            }
        }

        let qt_selection = self.selection_to_datatable_selection(
            &stmt.selection, txn, &stmt.table_name, &ucols)?;
     
        // if usercols are being updated, query DT to get the relevant
        // GIDs and update these GID->UID mappings in the ghosts table
        if !ucol_assigns.is_empty() {
            let get_gids_stmt_from_dt = Statement::Select(SelectStatement {
                query: Box::new(Query::select(Select{
                    distinct: true,
                    projection: ucol_selectitems_assn,
                    from: vec![TableWithJoins{
                        relation: TableFactor::Table{
                            name: stmt.table_name.clone(),
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
            warn!("issue_update_datatable_stmt: {}", get_gids_stmt_from_dt);
            let res = txn.query_iter(format!("{}", get_gids_stmt_from_dt.to_string()))?;
            self.cur_stat.nqueries+=1;

            let mut ghost_update_stmts = vec![];
            let mut ghost_update_pairs = vec![];
            for row in res {
                let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                for (i, uc_val) in ucol_assigns.iter().enumerate() {
                    let gid = helpers::mysql_val_to_parser_val(&mysql_vals[i]);
                    // delete the GID entry if it is being set to NULL
                    if uc_val.value == Expr::Value(Value::Null) {
                        ghost_update_stmts.push(Statement::Delete(DeleteStatement {
                            table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                            selection: Some(Expr::BinaryOp{
                                left: Box::new(Expr::Identifier(
                                              helpers::string_to_idents(super::GHOST_ID_COL))),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(gid)),
                            }),
                        }));
                        ghost_update_pairs.push((None, helpers::mysql_val_to_u64(&mysql_vals[i])?));
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
                                left: Box::new(Expr::Identifier(
                                              helpers::string_to_idents(super::GHOST_ID_COL))),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(gid)),
                            }),
                        }));
                        ghost_update_pairs.push(
                            (Some(helpers::parser_expr_to_u64(&uc_val.value)?), 
                             helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    }
                }
            }
            for gstmt in ghost_update_stmts {
                warn!("issue_update_dt_stmt: {}", gstmt);
                txn.query_drop(format!("{}", gstmt.to_string()))?;
                self.cur_stat.nqueries+=1;
            }
            self.cache.update_uid2gids_with(&ghost_update_pairs)?;
        }
        let update_stmt = Statement::Update(UpdateStatement{
            table_name: stmt.table_name.clone(),
            assignments : qt_assn,
            selection : qt_selection,
        });
        warn!("issue_update_dt_stmt: {}", update_stmt);
        txn.query_drop(update_stmt.to_string())?;
        self.cur_stat.nqueries+=1;
        Ok(())
    }
    
    fn issue_delete_datatable_stmt(&mut self, stmt: DeleteStatement, txn: &mut mysql::Transaction)
        -> Result<(), mysql::Error> 
    {        
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let qt_selection = self.selection_to_datatable_selection(&stmt.selection, txn, &stmt.table_name, &ucols)?;

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
                                name: stmt.table_name.clone(),
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
        warn!("issue_delete_dt_stmt: {}", select_gids_stmt);
        let res = txn.query_iter(format!("{}", select_gids_stmt.to_string()))?;
        self.cur_stat.nqueries+=1;

        let mut gids_list : Vec<Expr>= vec![];
        let mut ghost_update_pairs: Vec<(Option<u64>, u64)>= vec![];
        for row in res {
            for val in row.unwrap().unwrap() {
                gids_list.push(Expr::Value(helpers::mysql_val_to_parser_val(&val)));
                ghost_update_pairs.push((None, helpers::mysql_val_to_u64(&val)?));
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
            warn!("issue_delete_dt_stmt: {}", ghosts_delete_statement);
            txn.query_drop(&ghosts_delete_statement.to_string())?;
            self.cur_stat.nqueries+=1;
        }
        self.cache.update_uid2gids_with(&ghost_update_pairs)?;

        let delete_stmt = Statement::Delete(DeleteStatement{
            table_name: stmt.table_name.clone(),
            selection : qt_selection,
        });
        warn!("issue_delete_dt_stmt: {}", delete_stmt);
        txn.query_drop(delete_stmt.to_string())?;
        self.cur_stat.nqueries+=1;
        Ok(())
    }


    fn issue_to_dt_and_get_mv_stmt (
        &mut self, 
        stmt: &Statement, 
        txn: &mut mysql::Transaction) 
        -> Result<Statement, mysql::Error>
    {
        let mv_stmt : Statement;
        let mut is_dt_write = false;
        let mv_table_name : String;

        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                let new_q = self.mvtrans.query_to_mv_query(&query);
                mv_stmt = Statement::Select(SelectStatement{
                    query: Box::new(new_q), 
                    as_of: as_of.clone(),
                })
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                mv_table_name = self.mvtrans.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();
                let mut new_source = source.clone();
                let mut mv_cols = columns.clone();
                let mut new_q = None;
                
                // update sources if is a datatable
                let mut values = vec![];
                if is_dt_write || table_name.to_string() == self.cfg.user_table.name {
                    match source {
                        InsertSource::Query(q) => {
                            values = self.insert_source_query_to_values(&q, txn)?;
                            new_q = Some(q.clone());
                        }
                        InsertSource::DefaultValues => (),
                    }
                }

                // issue to datatable with vals_vec BEFORE we modify vals_vec to include the
                // user_id column
                if is_dt_write {
                    self.issue_insert_datatable_stmt(&mut values.clone(), InsertStatement{
                        table_name: table_name.clone(), 
                        columns: columns.clone(), 
                        source: source.clone(),
                    }, txn)?;
                }
                                       
                // if the user table has an autoincrement column, we should 
                // (1) see if the table is actually inserting a value for that column (found) 
                // (2) update the self.latest_uid appropriately and insert the value for that column
                if table_name.to_string() == self.cfg.user_table.name && self.cfg.user_table.is_autoinc {
                    let mut found = false;
                    for (i, col) in columns.iter().enumerate() {
                        if col.to_string() == self.cfg.user_table.id_col {
                            // get the values of the uid col being inserted and update as
                            // appropriate
                            let mut max = self.latest_uid.load(Ordering::SeqCst);
                            for vv in &values{
                                match &vv[i] {
                                    Expr::Value(Value::Number(n)) => {
                                        let n = n.parse::<u64>().map_err(|e| mysql::Error::IoError(io::Error::new(
                                                        io::ErrorKind::Other, format!("{}", e))))?;
                                        max = cmp::max(max, n);
                                    }
                                    _ => (),
                                }
                            }
                            // TODO ensure self.latest_uid never goes above GID_START
                            self.latest_uid.fetch_max(max, Ordering::SeqCst);
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        // put self.latest_uid + N as the id col values 
                        let cur_uid = self.latest_uid.fetch_add(values.len() as u64, Ordering::SeqCst);
                        for i in 0..values.len() {
                            values[i].push(Expr::Value(Value::Number(format!("{}", cur_uid + (i as u64) + 1))));
                        }
                        // add id column to update
                        mv_cols.push(Ident::new(self.cfg.user_table.id_col.clone()));
                    }
                    // update source with new vals_vec
                    if let Some(mut nq) = new_q {
                        nq.body = SetExpr::Values(Values(values.clone()));
                        new_source = InsertSource::Query(nq);
                    }
                }
                
                mv_stmt = Statement::Insert(InsertStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    columns : mv_cols,
                    source : new_source, 
                });
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                mv_table_name = self.mvtrans.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();

                let mut assign_vals = vec![];
                if is_dt_write || table_name.to_string() == self.cfg.user_table.name {
                    let mut contains_ucol_id = false;
                    for a in assignments {
                        assign_vals.push(self.expr_to_value_expr(&a.value, txn, &mut contains_ucol_id, &vec![])?);
                    }
                }
                    
                if is_dt_write {
                    self.issue_update_datatable_stmt(
                        &assign_vals,
                        UpdateStatement{
                            table_name: table_name.clone(), 
                            assignments: assignments.clone(), 
                            selection: selection.clone()
                        }, 
                        txn)?;
                }
 
                // if the user table has an autoincrement column, we should 
                // (1) see if the table is actually updating a value for that column and
                // (2) update the self.latest_uid appropriately 
                if table_name.to_string() == self.cfg.user_table.name && self.cfg.user_table.is_autoinc {
                    for i in 0..assignments.len() {
                        if assignments[i].id.to_string() == self.cfg.user_table.id_col {
                            match &assign_vals[i] {
                                Expr::Value(Value::Number(n)) => {
                                    let n = n.parse::<u64>().map_err(|e| mysql::Error::IoError(io::Error::new(
                                                    io::ErrorKind::Other, format!("{}", e))))?;
                                    self.latest_uid.fetch_max(n, Ordering::SeqCst);
                                }
                                _ => (),
                            }
                        }
                    }
                }

                let mut mv_assn = Vec::<Assignment>::new();
                let mut mv_selection = selection.clone();
                // update assignments
                for a in assignments {
                    mv_assn.push(Assignment{
                        id : a.id.clone(),
                        value: self.mvtrans.expr_to_mv_expr(&a.value),
                    });
                }
                // update selection 
                match selection {
                    None => (),
                    Some(s) => mv_selection = Some(self.mvtrans.expr_to_mv_expr(&s)),
                }
               
                mv_stmt = Statement::Update(UpdateStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    assignments : mv_assn,
                    selection : mv_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                mv_table_name = self.mvtrans.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();

                if is_dt_write {
                    self.issue_delete_datatable_stmt(DeleteStatement{
                        table_name: table_name.clone(), 
                        selection: selection.clone(),
                    }, txn)?;
                }

                let mut mv_selection = selection.clone();
                // update selection 
                match selection {
                    None => (),
                    Some(s) => mv_selection = Some(self.mvtrans.expr_to_mv_expr(&s)),
                }
               
                mv_stmt = Statement::Delete(DeleteStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    selection : mv_selection,
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
                let mv_query = self.mvtrans.query_to_mv_query(&query);
                mv_stmt = Statement::CreateView(CreateViewStatement{
                    name: name.clone(),
                    columns: columns.clone(),
                    with_options: with_options.clone(),
                    query : Box::new(mv_query),
                    if_exists: if_exists.clone(),
                    temporary: temporary.clone(),
                    materialized: materialized.clone(),
                });
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
                mv_table_name = self.mvtrans.objname_to_mv_string(&name);
                is_dt_write = mv_table_name != name.to_string();
                let mut new_engine = engine.clone();
                if self.params.in_memory {
                    new_engine = Some(Engine::Memory);
                }

                if is_dt_write {
                    // create the original table as well if we're going to
                    // create a MV for this table
                    let dtstmt = CreateTableStatement {
                        name: name.clone(),
                        columns: columns.clone(),
                        constraints: constraints.clone(),
                        indexes: indexes.clone(),
                        with_options: with_options.clone(),
                        if_not_exists: *if_not_exists,
                        engine: new_engine.clone(),
                    };

                    warn!("get_mv stmt: {}", dtstmt);
                    txn.query_drop(dtstmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }

                let mv_constraints : Vec<TableConstraint> = constraints
                    .iter()
                    .map(|c| match c {
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                        } => {
                            let foreign_table = self.mvtrans.objname_to_mv_string(foreign_table);
                            TableConstraint::ForeignKey{
                                name: name.clone(),
                                columns: columns.clone(),
                                foreign_table: helpers::string_to_objname(&foreign_table),
                                referred_columns: referred_columns.clone(),
                            }
                        }
                        _ => c.clone(),
                    })
                    .collect(); 

                let mut mv_cols = columns.clone();
                // if we're creating the user table, remove any autoinc column
                if name.to_string() == self.cfg.user_table.name {
                    for col in &mut mv_cols{
                        if col.name.to_string() != self.cfg.user_table.id_col {
                            continue;
                        }

                        // if this is the user id column and it is autoincremented,
                        // remove autoincrement in materialized view
                        if col.options.iter().any(|cod| cod.option == ColumnOption::AutoIncrement) {
                            self.cfg.user_table.is_autoinc = true;
                            col.options.retain(|x| x.option != ColumnOption::AutoIncrement);
                        }
                        break;
                    }
                }
                
                mv_stmt = Statement::CreateTable(CreateTableStatement{
                    name: helpers::string_to_objname(&mv_table_name),
                    columns: mv_cols,
                    constraints: mv_constraints,
                    indexes: indexes.clone(),
                    with_options: with_options.clone(),
                    if_not_exists: *if_not_exists,
                    engine: new_engine,
                });
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
                mv_table_name = self.mvtrans.objname_to_mv_string(&on_name);
                is_dt_write = mv_table_name != on_name.to_string();

                if is_dt_write {
                    // create the original index as well if we're going to
                    // create a MV index 
                    warn!("get_mv: {}", stmt);
                    txn.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }

                mv_stmt = Statement::CreateIndex(CreateIndexStatement{
                    name: name.clone(),
                    on_name: helpers::string_to_objname(&mv_table_name),
                    key_parts: key_parts.clone(),
                    if_not_exists: if_not_exists.clone(),
                });
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
                let mut to_item_mv_name = to_item_name.to_string();
                mv_table_name= self.mvtrans.objname_to_mv_string(&name);
                is_dt_write = mv_table_name != name.to_string();

                if is_dt_write {
                    // alter the original table as well if we're going to
                    // alter a MV table
                    warn!("get_mv: {}", stmt);
                    txn.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }
                
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        if mv_table_name != name.to_string() {
                            // change config to reflect new table name
                            // TODO change config as table names are updated
                            /*if self.cfg.user_table.name == name.to_string() {
                                self.cfg.user_table.name = to_item_name.to_string();
                            } else {
                                for tab in &mut self.cfg.data_tables {
                                    if tab.name == name.to_string() {
                                        tab.name = to_item_name.to_string();
                                    }
                                }
                            }*/
                            to_item_mv_name = format!("{}{}", to_item_name, super::MV_SUFFIX);
                        }
                    }
                    _ => (),
                }
                mv_stmt = Statement::AlterObjectRename(AlterObjectRenameStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    name: helpers::string_to_objname(&mv_table_name),
                    to_item_name: Ident::new(to_item_mv_name),
                });
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
                let mut mv_names = names.clone();
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        for name in &mut mv_names {
                            let newname = self.mvtrans.objname_to_mv_string(&name);
                            is_dt_write |= newname != name.to_string();

                            if is_dt_write {
                                // alter the original table as well if we're going to
                                // alter a MV table
                                warn!("get_mv: {}", stmt);
                                txn.query_drop(stmt.to_string())?;
                                self.cur_stat.nqueries+=1;
                            }

                            *name = helpers::string_to_objname(&newname);
                        }
                    }
                    _ => (),
                }
                mv_stmt = Statement::DropObjects(DropObjectsStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    names: mv_names,
                    cascade: *cascade,
                });
            }
            Statement::ShowObjects(ShowObjectsStatement{
                object_type,
                from,
                extended,
                full,
                materialized,
                filter,
            }) => {
                let mut mv_from = from.clone();
                if let Some(f) = from {
                    mv_from = Some(helpers::string_to_objname(&self.mvtrans.objname_to_mv_string(&f)));
                }

                let mut mv_filter = filter.clone();
                if let Some(f) = filter {
                    match f {
                        ShowStatementFilter::Like(_s) => (),
                        ShowStatementFilter::Where(expr) => {
                            mv_filter = Some(ShowStatementFilter::Where(self.mvtrans.expr_to_mv_expr(&expr)));
                        }
                    }
                }
                mv_stmt = Statement::ShowObjects(ShowObjectsStatement{
                    object_type: object_type.clone(),
                    from: mv_from,
                    extended: *extended,
                    full: *full,
                    materialized: *materialized,
                    filter: mv_filter,
                });
            }
            Statement::ShowIndexes(ShowIndexesStatement{
                table_name,
                extended,
                filter,
            }) => {
                mv_table_name = self.mvtrans.objname_to_mv_string(&table_name);
                let mut mv_filter = filter.clone();
                if let Some(f) = filter {
                    match f {
                        ShowStatementFilter::Like(_s) => (),
                        ShowStatementFilter::Where(expr) => {
                            mv_filter = Some(ShowStatementFilter::Where(self.mvtrans.expr_to_mv_expr(&expr)));
                        }
                    }
                }
                mv_stmt = Statement::ShowIndexes(ShowIndexesStatement {
                    table_name: helpers::string_to_objname(&mv_table_name),
                    extended: *extended,
                    filter: mv_filter,
                });
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
                mv_stmt = stmt.clone()
            }
        }
        Ok(mv_stmt)
    }

    pub fn record_query_stats(&mut self, qtype: stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries+=self.cache.nqueries;
        self.cur_stat.duration = dur;
        self.cur_stat.qtype = qtype;
        self.stats.push(self.cur_stat.clone());
        self.cur_stat.clear();
        self.cache.nqueries = 0;
    }

    pub fn query<W: io::Write>(
        &mut self, 
        writer: QueryResultWriter<W>, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error>
    {
        let res : Result<(), mysql::Error>;
        if let Some(mv_stmt) = self.mvtrans.try_get_simple_mv_stmt(self.params.in_memory, stmt)? {
            warn!("query_iter nontxnal mv_stmt: {}", mv_stmt);
            res = helpers::answer_rows(writer, db.query_iter(mv_stmt.to_string()));
        } else {
            let mut txn = db.start_transaction(mysql::TxOpts::default())?;
            let mv_stmt = self.issue_to_dt_and_get_mv_stmt(stmt, &mut txn)?;
            warn!("query_iter txnal mv_stmt: {}", mv_stmt);
            res = helpers::answer_rows(writer, txn.query_iter(mv_stmt.to_string()));
            txn.commit()?;
        }
        res
    }

    pub fn query_drop(
        &mut self, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        if let Some(mv_stmt) = self.mvtrans.try_get_simple_mv_stmt(self.params.in_memory, stmt)? {
            warn!("query_drop nontxnal mv_stmt: {}", mv_stmt);
            db.query_drop(mv_stmt.to_string())?;
        } else {
            let mut txn = db.start_transaction(mysql::TxOpts::default())?;
            let mv_stmt = self.issue_to_dt_and_get_mv_stmt(stmt, &mut txn)?;
            warn!("query_drop txnal mv_stmt: {}", mv_stmt);
            txn.query_drop(mv_stmt.to_string())?;
            txn.commit()?;
        }
        Ok(())
    }

    pub fn unsubscribe(&mut self, uid: u64, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        self.cur_stat.qtype = stats::QueryType::Unsub;

        // check if already unsubscribed
        if !self.cache.unsubscribe(uid) {
            return Ok(())
        }
        let mut txn = db.start_transaction(mysql::TxOpts::default())?;
        let uid_val = Value::Number(uid.to_string());
                    
        let vals_vec : Vec<Vec<Expr>> = self.cache.get_gids_for_uid(uid, &mut txn)?
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
        if !self.cache.resubscribe(uid) {
            return Ok(())
        }
        let mut txn = db.start_transaction(mysql::TxOpts::default())?;
        let uid_val = Value::Number(uid.to_string());

        let gid_exprs : Vec<Expr> = self.cache.get_gids_for_uid(uid, &mut txn)?
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
        txn.commit()?;
        Ok(())
    }
} 
