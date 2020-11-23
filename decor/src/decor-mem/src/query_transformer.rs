use mysql::prelude::*;
use sql_parser::ast::*;
use super::{helpers, ghosts_map, config, stats, views, select};
use std::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use msql_srv::{QueryResultWriter};
use log::{warn, debug};

pub struct QueryTransformer {
    pub cfg: config::Config,
    pub ghosts_map: ghosts_map::GhostsMap,
    
    views: views::Views,
    
    // for tests
    params: super::TestParams,
    pub cur_stat: stats::QueryStat,
    pub stats: Vec<stats::QueryStat>,
}

impl QueryTransformer {
    pub fn new(cfg: &config::Config, params: &super::TestParams) -> Self {
        QueryTransformer{
            views: views::Views::new(),
            cfg: cfg.clone(),
            ghosts_map: ghosts_map::GhostsMap::new(),
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
    fn query_to_value_query(&mut self, query: &Query) -> Result<Query, mysql::Error> {
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        for row in self.views.query_iter(query)?.1 {
            vals_vec.push(row.borrow().iter().map(|v| Expr::Value(v.clone())).collect());
        }
        self.cur_stat.nqueries_mv+=1;
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
    fn query_to_value_rows(&mut self, query: &Query, is_single_column: bool) -> Result<Vec<Expr>, mysql::Error> {
        let mut vals_vec : Vec<Expr>= vec![];
        self.cur_stat.nqueries_mv+=1;
        for row in self.views.query_iter(query)?.1 {
            let row = row.borrow();
            if is_single_column {
                if row.len() != 1 {
                    return Err(mysql::Error::IoError(io::Error::new(io::ErrorKind::Other, format!("Query should only select one column"))));
                }
                vals_vec.push(Expr::Value(row[0].clone()));
            } else {
                vals_vec.push(Expr::Row{exprs:
                    row 
                    .iter()
                    .map(|v| Expr::Value(v.clone()))
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
    fn expr_to_value_expr(&mut self, expr: &Expr, contains_ucol_id: &mut bool, ucols_to_replace: &Vec<String>) 
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
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_query = self.query_to_value_query(&subquery)?;
                // otherwise just return table column IN subquery
                Expr::InSubquery {
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
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
                let new_low = self.expr_to_value_expr(&low, contains_ucol_id, ucols_to_replace)?;
                let new_high = self.expr_to_value_expr(&high, contains_ucol_id, ucols_to_replace)?;
                Expr::Between {
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
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
                let new_left = self.expr_to_value_expr(&left, contains_ucol_id, ucols_to_replace)?;
                let new_right = self.expr_to_value_expr(&right, contains_ucol_id, ucols_to_replace)?;
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
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(
                    self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
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
                            new_exprs.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_value_expr(&filt, contains_ucol_id, ucols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_value_expr(&obe.expr, contains_ucol_id, ucols_to_replace)?,
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
                    new_cond.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                }
            }
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_value_query(&q)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_value_query(&q)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_value_expr(&left, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_value_expr(&left, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
                subscript: Box::new(self.expr_to_value_expr(&subscript, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_value_expr(&e, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ucol_id, ucols_to_replace)?),
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
    fn fastpath_expr_to_gid_expr(&mut self, e: &Expr, ucols_to_replace: &Vec<String>) 
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
                        for (_uid, gids) in self.ghosts_map.get_gids_for_uids(&uid_vals)? {
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
                    let vals_vec = self.query_to_value_rows(&subquery, true)?;
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
                        for (_uid, gids) in self.ghosts_map.get_gids_for_uids(&uid_vals)? {
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
                        for (_uid, gids) in self.ghosts_map.get_gids_for_uids(&uid_vals)? {
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
                                    for (_uid, gids) in self.ghosts_map.get_gids_for_uids(&uid_vals)? {
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
                        let newleft = self.fastpath_expr_to_gid_expr(&left, ucols_to_replace)?;
                        let newright = self.fastpath_expr_to_gid_expr(&right, ucols_to_replace)?;
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
                if let Some(expr) = self.fastpath_expr_to_gid_expr(&expr, ucols_to_replace)? {
                    new_expr = Some(Expr::UnaryOp{
                        op : op.clone(),
                        expr: Box::new(expr),
                    });
                }
            },

            // nested (valid fastpath expr)
            Expr::Nested(expr) => {
                if let Some(expr) = self.fastpath_expr_to_gid_expr(&expr, ucols_to_replace)? {
                    new_expr = Some(Expr::Nested(Box::new(expr)));
                } 
            },
            
            // Row or List(valid fastpath exprs)
            Expr::Row{ exprs } | Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, ucols_to_replace)? {
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
                let vals = self.query_to_value_rows(&q, false)?;
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
                    Some(e) => self.fastpath_expr_to_gid_expr(&e, ucols_to_replace)?,
                    None => None,
                };

                let mut new_cond = vec![];
                for e in conditions {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, ucols_to_replace)? {
                        new_cond.push(newe);
                    }
                }
                let mut new_res= vec![];
                for e in results {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, ucols_to_replace)? {
                        new_res.push(newe);
                    }
                }
                let new_end_res = match else_result {
                    Some(e) => self.fastpath_expr_to_gid_expr(&e, ucols_to_replace)?,
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
                new_expr = Some(Expr::Subquery(Box::new(self.query_to_value_query(&q)?)));
            }
            
            Expr::Any {
                left,
                op,
                right,
            } => {
                if let Some(newleft) = self.fastpath_expr_to_gid_expr(&left, ucols_to_replace)? {
                    new_expr = Some(Expr::Any{
                        left: Box::new(newleft),
                        op: op.clone(),
                        right: Box::new(self.query_to_value_query(&right)?),
                    });
                }
            }
            
            Expr::All{
                left,
                op,
                right,
            } => {
                if let Some(newleft) = self.fastpath_expr_to_gid_expr(&left, ucols_to_replace)? {
                    new_expr = Some(Expr::Any{
                        left: Box::new(newleft),
                        op: op.clone(),
                        right: Box::new(self.query_to_value_query(&right)?),
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
    fn insert_source_query_to_rptrs(&mut self, q: &Query) 
        -> Result<views::RowPtrs, mysql::Error> 
    {
        let mut contains_ucol_id = false;
        let mut vals_vec : views::RowPtrs = vec![];
        match &q.body {
            SetExpr::Values(Values(expr_vals)) => {
                // NOTE: only need to modify values if we're dealing with a DT,
                // could perform check here rather than calling vals_vec
                for row in expr_vals {
                    let mut vals_row : views::Row = vec![];
                    for val in row {
                        let value_expr = self.expr_to_value_expr(&val, &mut contains_ucol_id, &vec![])?;
                        match value_expr {
                            Expr::Subquery(q) => {
                                match q.body {
                                    SetExpr::Values(Values(subq_exprs)) => {
                                        assert_eq!(subq_exprs.len(), 1);
                                        assert_eq!(subq_exprs[0].len(), 1);
                                        match &subq_exprs[0][0] {
                                            Expr::Value(v) => vals_row.push(v.clone()),
                                            _ => unimplemented!("Bad value? q"),
                                        }
                                    }
                                    _ => unimplemented!("query_to_data_query should only return a Value"),
                                }
                            }
                            Expr::Value(v) => vals_row.push(v),
                            Expr::UnaryOp{
                                op, 
                                expr
                            } => {
                                if let Expr::Value(ref v) = *expr {
                                    match op {
                                        UnaryOperator::Minus => {
                                            let n = -1.0 * helpers::parser_val_to_f64(&v);
                                            vals_row.push(Value::Number(n.to_string()));
                                        }
                                        _ => unimplemented!("Unary op not supported! {:?}", expr),
                                    }
                                } else {
                                    unimplemented!("Unary op not supported! {:?}", expr);
                                }
                            }
                            _ => unimplemented!("Bad value expression: {:?}", value_expr),
                        }
                    }
                    vals_vec.push(Rc::new(RefCell::new(vals_row)));
                }
            }
            _ => {
                // we need to issue q to MVs to get rows that will be set as values
                // regardless of whether this is a DT or not (because query needs
                // to read from MV, rather than initially specified tables)
                vals_vec = self.views.query_iter(q)?.1;
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
        table_name: &ObjectName, 
        ucols: &Vec<String>) 
        -> Result<Option<Expr>, mysql::Error>
    {
        let mut contains_ucol_id = false;
        let mut qt_selection = None;
        if let Some(s) = selection {
            // check if the expr can be fast-pathed
            if let Some(fastpath_expr) = self.fastpath_expr_to_gid_expr(&s, &ucols)? {
                qt_selection = Some(fastpath_expr);
            } else {
                // check if the expr contains any conditions on user columns
                qt_selection = Some(self.expr_to_value_expr(&s, &mut contains_ucol_id, &ucols)?);

                // if a user column is being used as a selection criteria, first perform a 
                // select of all UIDs of matching rows in the MVs
                if contains_ucol_id {
                    let query = Query::select(Select{
                        distinct: true,
                        projection: vec![SelectItem::Wildcard],
                        from: vec![TableWithJoins{
                            relation: TableFactor::Table{
                                name: table_name.clone(),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: selection.clone(),
                        group_by: vec![],
                        having: None,
                    });

                    // collect row results from MV
                    let mut uids = vec![];
                    let (cols, rows, _cols_to_keep) = self.views.query_iter(&query)?;
                    self.cur_stat.nqueries_mv+=1;
                    for rptr in &rows {
                        let row = rptr.borrow();
                        for ci in 0..cols.len() {
                            // if it's a user column, add restriction on GID
                            let colname = cols[ci].name();
     
                            // Add condition on user column to be within relevant GIDs mapped
                            // to by the UID value
                            // However, only update with GIDs if UID value is NOT NULL
                            if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) 
                                && row[ci] != Value::Null
                            {
                                uids.push(helpers::parser_val_to_u64(&row[ci]));
                            }
                        }
                    }

                    // get all the gid rows corresponding to uids
                    // TODO deal with potential GIDs in user_cols due to
                    // unsubscriptions/resubscriptions
                    self.ghosts_map.cache_uid2gids_for_uids(&uids)?;

                    // expr to constrain to select a particular row
                    let mut or_row_constraint_expr = Expr::Value(Value::Boolean(false));
                    for rptr in &rows {
                        let row = rptr.borrow();
                        let mut and_col_constraint_expr = Expr::Value(Value::Boolean(true));
                        for ci in 0..cols.len() {
                            // Add condition on user column to be within relevant GIDs mapped
                            // to by the UID value
                            // However, only update with GIDs if UID value is NOT NULL
                            let colname = cols[ci].name();
                            if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) 
                                && row[ci] != Value::Null
                            {
                                let uid = helpers::parser_val_to_u64(&row[ci]);
                                // add condition on user column to be within the relevant GIDs
                                and_col_constraint_expr = Expr::BinaryOp {
                                    left: Box::new(and_col_constraint_expr),
                                    op: BinaryOperator::And,
                                    right: Box::new(Expr::InList {
                                        expr: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                        list: self.ghosts_map.get_gids_for_uid(uid)?.iter()
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
                                        right: Box::new(Expr::Value(row[ci].clone())),
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
    
    fn issue_insert_datatable_stmt(&mut self, values: &views::RowPtrs, stmt: InsertStatement, db: &mut mysql::Conn) 
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
        
        /* 
         * if there are user columns, we need to insert new GID->UID mappings 
         * with the values of the usercol value as the UID
         * and then set the GID as the new source value of the usercol 
         * (thus why values is mutable)
         * */
        match stmt.source {
            InsertSource::Query(q) => {
                let vals_with_gids = self.ghosts_map.insert_uid2gids_for_values(&values, &ucol_indices, db)?;
                let mut new_q = q.clone();
                new_q.body = SetExpr::Values(Values(vals_with_gids));
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
        db.query_drop(dt_stmt.to_string())?;
        self.cur_stat.nqueries+=1;

        Ok(())
    }
       
    fn issue_update_datatable_stmt(&mut self, assign_vals: &Vec<Expr>, stmt: UpdateStatement, db: &mut mysql::Conn)
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
            &stmt.selection, &stmt.table_name, &ucols)?;
     
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
            let res = db.query_iter(format!("{}", get_gids_stmt_from_dt.to_string()))?;
            self.cur_stat.nqueries+=1;

            let mut ghost_update_pairs = vec![];
            for row in res {
                let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                for (i, uc_val) in ucol_assigns.iter().enumerate() {
                    if uc_val.value == Expr::Value(Value::Null) {
                        ghost_update_pairs.push((None, helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    } else {
                        ghost_update_pairs.push(
                            (Some(helpers::parser_expr_to_u64(&uc_val.value)?), 
                             helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    }
                }
            }
            self.ghosts_map.update_uid2gids_with(&ghost_update_pairs, db)?;
        }
        let update_stmt = Statement::Update(UpdateStatement{
            table_name: stmt.table_name.clone(),
            assignments : qt_assn,
            selection : qt_selection,
        });
        warn!("issue_update_dt_stmt: {}", update_stmt);
        db.query_drop(update_stmt.to_string())?;
        self.cur_stat.nqueries+=1;
        Ok(())
    }
    
    fn issue_delete_datatable_stmt(&mut self, stmt: DeleteStatement, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {        
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let qt_selection = self.selection_to_datatable_selection(&stmt.selection, &stmt.table_name, &ucols)?;

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
        let res = db.query_iter(format!("{}", select_gids_stmt.to_string()))?;
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
        self.ghosts_map.update_uid2gids_with(&ghost_update_pairs, db)?;

        // delete from the data table
        let delete_stmt = Statement::Delete(DeleteStatement{
            table_name: stmt.table_name.clone(),
            selection : qt_selection,
        });
        warn!("issue_delete_dt_stmt: {}", delete_stmt);
        db.query_drop(delete_stmt.to_string())?;
        self.cur_stat.nqueries+=1;
        Ok(())
    }

    fn issue_statement (
            &mut self, 
            stmt: &Statement,
            db: &mut mysql::Conn) 
        -> Result<(Vec<views::TableColumnDef>, views::RowPtrs, Vec<usize>), mysql::Error>
    {
        warn!("issue statement: {:?}", stmt);
        let mut view_res : (Vec<views::TableColumnDef>, views::RowPtrs, Vec<usize>) = (vec![], vec![], vec![]);
        
        // TODO consistency?
        match stmt {
            Statement::Select(SelectStatement{query, ..}) => {
                view_res = self.views.query_iter(&query)?;
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                let is_dt_write = helpers::is_datatable(&self.cfg, &table_name);
                
                // update sources if is a datatable
                let mut values = vec![];
                if is_dt_write || table_name.to_string() == self.cfg.user_table.name {
                    match source {
                        InsertSource::Query(q) => {
                            values = self.insert_source_query_to_rptrs(&q)?;
                        }
                        InsertSource::DefaultValues => (),
                    }
                }

                // issue to datatable with vals_vec BEFORE we modify vals_vec to include the
                // user_id column
                if is_dt_write {
                    self.issue_insert_datatable_stmt(
                        &values, 
                        InsertStatement{
                            table_name: table_name.clone(), 
                            columns: columns.clone(), 
                            source: source.clone(),
                        },
                        db
                    )?;
                }

                // insert into views
                self.views.insert(&table_name, &columns, &values)?;
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                let is_dt_write = helpers::is_datatable(&self.cfg, &table_name);

                let mut assign_vals = vec![];
                let mut contains_ucol_id = false;
                if is_dt_write || table_name.to_string() == self.cfg.user_table.name {
                    for a in assignments {
                        assign_vals.push(self.expr_to_value_expr(&a.value, &mut contains_ucol_id, &vec![])?);
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
                        db)?;
                } else {
                    db.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }

                // update views
                self.views.update(&table_name, &assignments, &selection, &assign_vals)?;
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                let is_dt_write = helpers::is_datatable(&self.cfg, &table_name);
                if is_dt_write {
                    self.issue_delete_datatable_stmt(DeleteStatement{
                        table_name: table_name.clone(), 
                        selection: selection.clone(),
                    }, db)?;
                } else {
                    db.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }
                self.views.delete(&table_name, &selection)?;
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
                let mut new_engine = engine.clone();
                if self.params.in_memory {
                    new_engine = Some(Engine::Memory);
                }

                let dtstmt = CreateTableStatement {
                    name: name.clone(),
                    columns: columns.clone(),
                    constraints: constraints.clone(),
                    indexes: indexes.clone(),
                    with_options: with_options.clone(),
                    if_not_exists: *if_not_exists,
                    engine: new_engine.clone(),
                };
                db.query_drop(dtstmt.to_string())?;
                self.cur_stat.nqueries+=1;

                // create view for this table
                self.views.add_view(
                    name.to_string(), 
                    columns,
                    &indexes,
                    &constraints,
                );
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                names,
                ..
            }) => {
                match object_type {
                    ObjectType::Table => {
                        // alter the data table
                        db.query_drop(stmt.to_string())?;
                        self.cur_stat.nqueries+=1;

                        // remove view
                        self.views.remove_views(names);
                    }
                    _ => unimplemented!("Cannot drop object {}", stmt),
                }
            }
            _ => unimplemented!("stmt not supported: {}", stmt),
        }
        Ok(view_res)
    }

    pub fn record_query_stats(&mut self, qtype: stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries+=self.ghosts_map.nqueries;
        self.cur_stat.duration = dur;
        self.cur_stat.qtype = qtype;
        self.stats.push(self.cur_stat.clone());
        self.cur_stat.clear();
        self.ghosts_map.nqueries = 0;
    }

    pub fn query<W: io::Write>(
        &mut self, 
        writer: QueryResultWriter<W>, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error>
    {
        let view_res = self.issue_statement(stmt, db)?;
        views::view_cols_rows_to_answer_rows(&view_res.0, view_res.1, &view_res.2, writer)
    }

    pub fn query_drop(
        &mut self, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        self.issue_statement(stmt, db)?; 
        Ok(())
    }

    pub fn unsubscribe<W: io::Write>(&mut self, uid: u64, db: &mut mysql::Conn, writer: QueryResultWriter<W>) -> Result<(), mysql::Error> {
        warn!("Unsubscribing {}", uid);
        self.cur_stat.qtype = stats::QueryType::Unsub;

        let uid_val = Value::Number(uid.to_string());
        let user_table_name = helpers::string_to_objname(&self.cfg.user_table.name);

        // check if already unsubscribed
        // get list of ghosts to return otherwise
        let gids : Vec<u64>;
        let gid_values : views::RowPtrs;
        match self.ghosts_map.unsubscribe(uid, db)? {
            None => {
                writer.error(msql_srv::ErrorKind::ER_BAD_SLAVE, format!("{:?}", "user already unsubscribed").as_bytes())?;
                return Ok(());
            }
            Some(ghosts) => {
                gid_values = ghosts.iter().map(|g| Rc::new(RefCell::new(vec![Value::Number(g.to_string())]))).collect();
                gids = ghosts;
            }
        }
 
        /* 
         * 1. update the users MV to have an entry for all the users' GIDs
         */
        warn!("UNSUB: inserting into user view {:?}", gid_values);
        self.views.insert(&user_table_name, &vec![Ident::new(&self.cfg.user_table.id_col)], &gid_values)?;
        
        /*
         * 2. delete UID from users MV and users data table
         */
        let uid_col = Expr::Identifier(helpers::string_to_idents(&self.cfg.user_table.id_col));
        let selection = Some(Expr::BinaryOp{
                left: Box::new(uid_col),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(uid_val.clone())), 
        });
        // delete from user mv  
        warn!("UNSUB: deleting from user view {:?}", selection);
        self.views.delete(&user_table_name, &selection)?;

        let delete_uid_from_users = Statement::Delete(DeleteStatement {
            table_name: user_table_name.clone(),
            selection: selection.clone(),
        });
        warn!("UNSUB: {}", delete_uid_from_users);
        db.query_drop(format!("{}", delete_uid_from_users.to_string()))?;
        self.cur_stat.nqueries+=1;
 
        /* 
         * 3. Change all entries with this UID to use the correct GID in the MV
         */
        let mut gid_index = 0;
        for dt in &self.cfg.data_tables {
            let dtobjname = helpers::string_to_objname(&dt.name);
            let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &dtobjname);

            let view_ptr = self.views.get_view(&dt.name).unwrap();
            let mut view = view_ptr.borrow_mut();
            let mut select_constraint = Expr::Value(Value::Boolean(false));
            let mut cis = vec![];
            for col in &ucols {
                cis.push(view.columns.iter().position(|c| select::tablecolumn_matches_col(c, &col)).unwrap());
                select_constraint = Expr::BinaryOp {
                    left: Box::new(select_constraint),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(helpers::string_to_idents(&col))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(uid_val.clone())),
                    }),             
                };
            }            

            let (neg, mut rptrs_to_update) = select::get_rptrs_matching_constraint(&select_constraint, &view, None, None);
            if neg {
                let mut all_rptrs : views::RowPtrs = view.rows.borrow().iter().map(|(_pk, rptr)| rptr.clone()).collect();
                rptrs_to_update = view.minus_rptrs(&mut all_rptrs, &mut rptrs_to_update);
            } 
            let mut updated_cis = vec![];
            for rptr in &rptrs_to_update {
                let mut row = rptr.borrow_mut();
                for ci in &cis {
                    if row[*ci].to_string() == uid_val.to_string() {
                        assert!(gid_index < gid_values.len());
                        let val = &gid_values[gid_index].borrow()[0];
                        warn!("UNSUB: updating {:?} with {}", row, val);
                        row[*ci] = val.clone();
                        updated_cis.push(*ci);
                        gid_index += 1;
                    }
                }
            }
            for rptr in &rptrs_to_update {
                for ci in &updated_cis {
                    let val = &rptr.borrow()[*ci];
                    warn!("UNSUB: updating {:?} with {}", rptr, val);
                    view.update_index(rptr.clone(), *ci, Some(&val));
                }
            }
        }
        warn!("gid_index is {}, gid_values has len {}", gid_index, gid_values.len());
        assert!(gid_index == gid_values.len());  
        ghosts_map::answer_rows(writer, &gids)
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     * TODO check that user doesn't already exist
     */
    pub fn resubscribe(&mut self, uid: u64, gids: &Vec<u64>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        // TODO check auth token?
        self.cur_stat.qtype = stats::QueryType::Resub;

        if !self.ghosts_map.resubscribe(uid, gids, db)? {
            return Ok(());
        }

        let user_table_name = helpers::string_to_objname(&self.cfg.user_table.name);
        let uid_val = Value::Number(uid.to_string());

        let gid_exprs : Vec<Expr> = gids
            .iter()
            .map(|g| Expr::Value(Value::Number(g.to_string())))
            .collect();

        /*
         * 1. drop all GIDs from users table 
         */
        let selection =  Some(Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(&self.cfg.user_table.id_col))),
                list: gid_exprs.clone(),
                negated: false, 
        });
        // delete from users MV
        self.views.delete(&user_table_name, &selection)?;

        let delete_gids_as_users_stmt = Statement::Delete(DeleteStatement {
            table_name: user_table_name.clone(),
            selection: selection.clone(),
        });
        warn!("resub: {}", delete_gids_as_users_stmt);
        db.query_drop(format!("{}", delete_gids_as_users_stmt.to_string()))?;
        self.cur_stat.nqueries+=1;

        /*
         * 2. Add user to users
         * TODO should also add back all of the user data????
         */
        self.views.insert(&user_table_name, &vec![Ident::new(&self.cfg.user_table.id_col)], 
                          &vec![Rc::new(RefCell::new(vec![uid_val.clone()]))])?;

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
        db.query_drop(format!("{}", insert_uid_as_user_stmt.to_string()))?;
        self.cur_stat.nqueries+=1;
        
 
        /* 
         * 3. update assignments in MV to use UID again
         */
        for dt in &self.cfg.data_tables {
            let dtobjname = helpers::string_to_objname(&dt.name);
            let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &dtobjname);

            let view_ptr = self.views.get_view(&dt.name).unwrap();
            let mut view = view_ptr.borrow_mut();
            let mut select_constraint = Expr::Value(Value::Boolean(false));
            let mut cis = vec![];
            for col in &ucols {
                // push all user columns, even if some of them might not "belong" to us
                cis.push(view.columns.iter().position(|c| select::tablecolumn_matches_col(c, &col)).unwrap());
                select_constraint = Expr::BinaryOp {
                    left: Box::new(select_constraint),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::InList{
                        expr: Box::new(Expr::Identifier(helpers::string_to_idents(&col))),
                        list: gid_exprs.clone(),
                        negated: false,
                    }),             
                };
            }            

            let (negated, mut rptrs_to_update) = select::get_rptrs_matching_constraint(&select_constraint, &view, None, None);
            if negated {
                let mut all_rptrs : views::RowPtrs = view.rows.borrow().iter().map(|(_pk, rptr)| rptr.clone()).collect();
                rptrs_to_update = view.minus_rptrs(&mut all_rptrs, &mut rptrs_to_update);
            } 
            let mut updated_cis = vec![];
            for rptr in &rptrs_to_update {
                let mut row = rptr.borrow_mut();
                for ci in &cis {
                    // update the columns to use the uid
                    if gids.iter().any(|g| g.to_string() == row[*ci].to_string()) {
                        updated_cis.push(*ci);
                        row[*ci] = uid_val.clone();
                    }
                }
            }
            for rptr in &rptrs_to_update {
                for ci in &updated_cis {
                    warn!("RESUB: updating {:?} with {}", rptr, uid_val);
                    view.update_index(rptr.clone(), *ci, Some(&uid_val));
                }
            }
        }
        Ok(())
    }
} 
