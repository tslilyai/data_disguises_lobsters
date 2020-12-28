use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::*;
use msql_srv::{QueryResultWriter};
use log::{debug, warn, error};
use ordered_float::*;

use crate::{helpers, ghosts::GhostMaps, ghosts, policy, views, ID_COL, EntityData, graph::EntityTypeRows, subscriber};
use crate::views::{TableColumnDef, Views, Row, RowPtr, RowPtrs, HashedRowPtr};
use crate::ghosts::{TemplateEntity, GhostEidMapping, TableGhostEntities, GhostFamily};


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TraversedEntity {
    pub table_name: String,
    pub eid : u64,
    pub vals : HashedRowPtr,

    pub from_table: String,
    pub from_col_index: usize,
    pub sensitivity: OrderedFloat<f64>,
}

pub struct QueryTransformer {
    views: Views,
    
    // map from table names to columns with ghost parent IDs
    policy_config: policy::Config,
    ghost_maps: GhostMaps,
    pub subscriber: subscriber::Subscriber,
    
    // for tests
    params: super::TestParams,
    pub cur_stat: helpers::stats::QueryStat,
    pub stats: Vec<helpers::stats::QueryStat>,
}

impl QueryTransformer {
    pub fn new(policy: policy::ApplicationPolicy, params: &super::TestParams) -> Self {
        let policy_config = policy::policy_to_config(&policy);
        QueryTransformer{
            views: Views::new(),
            policy_config: policy_config,
            ghost_maps: GhostMaps::new(),
            subscriber: subscriber::Subscriber::new(),
            params: params.clone(),
            cur_stat: helpers::stats::QueryStat::new(),
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
    fn expr_to_value_expr(&mut self, expr: &Expr, contains_ghosted_col_id: &mut bool, ghosted_cols_to_replace: &Vec<(String, String)>) 
        -> Result<Expr, mysql::Error> 
    {
        let new_expr = match expr {
            Expr::Identifier(_ids) => {
                *contains_ghosted_col_id |= helpers::expr_to_ghosted_col(expr, ghosted_cols_to_replace).is_some();
                expr.clone()
            }
            Expr::QualifiedWildcard(_ids) => {
                *contains_ghosted_col_id |= helpers::expr_to_ghosted_col(expr, ghosted_cols_to_replace).is_some();
                expr.clone()
            }
            Expr::FieldAccess {
                expr,
                field,
            } => {
                // XXX we might be returning TRUE for contains_ghosted_col_id if only the expr matches,
                // but not the field
                Expr::FieldAccess {
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
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
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
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
                let new_low = self.expr_to_value_expr(&low, contains_ghosted_col_id, ghosted_cols_to_replace)?;
                let new_high = self.expr_to_value_expr(&high, contains_ghosted_col_id, ghosted_cols_to_replace)?;
                Expr::Between {
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
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
                let new_left = self.expr_to_value_expr(&left, contains_ghosted_col_id, ghosted_cols_to_replace)?;
                let new_right = self.expr_to_value_expr(&right, contains_ghosted_col_id, ghosted_cols_to_replace)?;
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
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(
                    self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
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
                            new_exprs.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_value_expr(&filt, contains_ghosted_col_id, ghosted_cols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_value_expr(&obe.expr, contains_ghosted_col_id, ghosted_cols_to_replace)?,
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
                    new_cond.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?)),
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
                left: Box::new(self.expr_to_value_expr(&left, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_value_expr(&left, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                subscript: Box::new(self.expr_to_value_expr(&subscript, contains_ghosted_col_id, ghosted_cols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_value_expr(&e, contains_ghosted_col_id, ghosted_cols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_value_expr(&expr, contains_ghosted_col_id, ghosted_cols_to_replace)?),
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
    fn fastpath_expr_to_gid_expr(&mut self, e: &Expr, ghosted_cols_to_replace: &Vec<(String, String)>) 
        -> Result<Option<Expr>, mysql::Error> 
    {
        // if it's just an identifier, we can return if it's not a ghosted_col
        debug!("\tFastpath expr: looking at {}", e);
        if helpers::expr_is_col(&e) && helpers::expr_to_ghosted_col(&e, ghosted_cols_to_replace).is_none() {
            warn!("Fastpath found non-ghost col {}, {:?}", e, ghosted_cols_to_replace);
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
                    if let Some((_gc, parent)) = helpers::expr_to_ghosted_col(&expr, ghosted_cols_to_replace) {
                        // get all eids in the list
                        let mut eid_vals = vec![];
                        for e in list {
                            // values must be u64 for ghosted cols
                            match helpers::parser_expr_to_u64(&e) {
                                Ok(v) => eid_vals.push(v),
                                Err(_) => return Ok(None),
                            }
                        }

                        // now change the eids to gids
                        let mut gid_exprs = vec![];
                        for (_eid, gids) in self.ghost_maps.get_gids_for_eids(&eid_vals, &parent)? {
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
                    if let Some((_gc, parent)) = helpers::expr_to_ghosted_col(expr, ghosted_cols_to_replace) {
                        // get all uids in the list
                        let mut eid_vals = vec![];
                        for e in vals_vec {
                            // values must be u64 for ghosted_cols
                            match helpers::parser_expr_to_u64(&e) {
                                Ok(v) => eid_vals.push(v),
                                Err(_) => return Ok(None),
                            }
                        }

                        // now change the uids to gids
                        let mut gid_exprs = vec![];
                        for (_eid, gids) in self.ghost_maps.get_gids_for_eids(&eid_vals, &parent)? {
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
                    if let Some((_gc, parent)) = helpers::expr_to_ghosted_col(&expr, ghosted_cols_to_replace) {
                        // change the eids in the range to gids
                        let eid_vals : Vec<u64> = ops::RangeInclusive::new(lowu64, highu64).collect();
                        let mut gid_exprs = vec![];
                        for (_eid, gids) in self.ghost_maps.get_gids_for_eids(&eid_vals, &parent)? {
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
                            // ghosted_col OP u64 val
                            if let Some((_gc, parent)) = helpers::expr_to_ghosted_col(&left, ghosted_cols_to_replace) {
                                if let Ok(v) = helpers::parser_expr_to_u64(&right) {
                                    let eid_vals : Vec<u64> = match op {
                                        BinaryOperator::Lt => (0..v).collect(),
                                        BinaryOperator::LtEq => ops::RangeInclusive::new(0, v).collect(),
                                        _ => vec![v],
                                    };
                                    let mut gid_exprs = vec![];
                                    for (_eid, gids) in self.ghost_maps.get_gids_for_eids(&eid_vals, &parent)? {
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
                            // col OP val or non-ghosted_col column
                            } else if helpers::expr_is_value(&right) 
                                || (helpers::expr_to_ghosted_col(&right, ghosted_cols_to_replace).is_none()
                                    && helpers::expr_is_col(&right))
                            {
                                warn!("Fastpath found non-ghost col {:?}, {:?}", left, ghosted_cols_to_replace);
                                new_expr = Some(e.clone());
                            }
                        }
                    }
                    // col > / >= col or val 
                    //  XXX ghosted_cols not supported because potentially unbounded
                    BinaryOperator::Gt | BinaryOperator::GtEq => {
                        if helpers::expr_to_ghosted_col(&left, ghosted_cols_to_replace).is_none()
                            && helpers::expr_to_ghosted_col(&right, ghosted_cols_to_replace).is_none()
                            && (helpers::expr_is_col(&left) || helpers::expr_is_value(&left)) 
                            && (helpers::expr_is_col(&right) || helpers::expr_is_value(&right)) 
                        {
                                new_expr = Some(e.clone());
                        } 
                    }
                    _ => {
                        // all other ops are ops on nested (non-primitive) exprs
                        // NOTE: just column names won't pass fastpath because then there may be
                        // constraints supported like "ghosted_col * col", which would lead to inaccurate results
                        // when ghosted_col contains GIDs
                        let newleft = self.fastpath_expr_to_gid_expr(&left, ghosted_cols_to_replace)?;
                        let newright = self.fastpath_expr_to_gid_expr(&right, ghosted_cols_to_replace)?;
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
                if let Some(expr) = self.fastpath_expr_to_gid_expr(&expr, ghosted_cols_to_replace)? {
                    new_expr = Some(Expr::UnaryOp{
                        op : op.clone(),
                        expr: Box::new(expr),
                    });
                }
            },

            // nested (valid fastpath expr)
            Expr::Nested(expr) => {
                if let Some(expr) = self.fastpath_expr_to_gid_expr(&expr, ghosted_cols_to_replace)? {
                    new_expr = Some(Expr::Nested(Box::new(expr)));
                } 
            },
            
            // Row or List(valid fastpath exprs)
            Expr::Row{ exprs } | Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, ghosted_cols_to_replace)? {
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
                    Some(e) => self.fastpath_expr_to_gid_expr(&e, ghosted_cols_to_replace)?,
                    None => None,
                };

                let mut new_cond = vec![];
                for e in conditions {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, ghosted_cols_to_replace)? {
                        new_cond.push(newe);
                    }
                }
                let mut new_res= vec![];
                for e in results {
                    if let Some(newe) = self.fastpath_expr_to_gid_expr(&e, ghosted_cols_to_replace)? {
                        new_res.push(newe);
                    }
                }
                let new_end_res = match else_result {
                    Some(e) => self.fastpath_expr_to_gid_expr(&e, ghosted_cols_to_replace)?,
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
                if let Some(newleft) = self.fastpath_expr_to_gid_expr(&left, ghosted_cols_to_replace)? {
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
                if let Some(newleft) = self.fastpath_expr_to_gid_expr(&left, ghosted_cols_to_replace)? {
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
        -> Result<RowPtrs, mysql::Error> 
    {
        let mut contains_ghosted_col_id = false;
        let mut vals_vec : RowPtrs = vec![];
        match &q.body {
            SetExpr::Values(Values(expr_vals)) => {
                // NOTE: only need to modify values if we're dealing with a DT,
                // could perform check here rather than calling vals_vec
                for row in expr_vals {
                    let mut vals_row : Row = vec![];
                    for val in row {
                        let value_expr = self.expr_to_value_expr(&val, &mut contains_ghosted_col_id, &vec![])?;
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
        ghosted_cols: &Vec<(String, String)>) 
        -> Result<Option<Expr>, mysql::Error>
    {
        let mut contains_ghosted_col_id = false;
        let mut qt_selection = None;
        if let Some(s) = selection {
            // check if the expr can be fast-pathed
            if let Some(fastpath_expr) = self.fastpath_expr_to_gid_expr(&s, &ghosted_cols)? {
                qt_selection = Some(fastpath_expr);
            } else {
                // check if the expr contains any conditions on user columns
                qt_selection = Some(self.expr_to_value_expr(&s, &mut contains_ghosted_col_id, &ghosted_cols)?);

                // if a user column is being used as a selection criteria, first perform a 
                // select of all EIDs of matching rows in the MVs
                if contains_ghosted_col_id {
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
                    let mut eids = vec![];
                    let (cols, rows, _cols_to_keep) = self.views.query_iter(&query)?;
                    self.cur_stat.nqueries_mv+=1;
                    for ci in 0..cols.len() {
                        let colname = &cols[ci].fullname;
                        if let Some(_i) = ghosted_cols.iter().position(|gc| helpers::str_ident_match(&colname, &gc.0)) {
                            for rptr in &rows {
                                let row = rptr.borrow();
         
                                // Add condition on ghosted column to be within relevant GIDs mapped
                                // to by the EID value
                                // However, only update with GIDs if EID value is NOT NULL
                                if row[ci] != Value::Null
                                {
                                    eids.push(helpers::parser_val_to_u64(&row[ci]));
                                }
                            }
                            // get all the gid rows corresponding to eids
                            // TODO deal with potential GIDs in ghosted_cols due to
                            // unsubscriptions/resubscriptions
                            //let parent = &ghosted_cols[i].1;
                        }
                    }

                    // expr to constrain to select a particular row
                    let mut or_row_constraint_expr = Expr::Value(Value::Boolean(false));
                    for rptr in &rows {
                        let row = rptr.borrow();
                        let mut and_col_constraint_expr = Expr::Value(Value::Boolean(true));
                        for ci in 0..cols.len() {
                            // Add condition on user column to be within relevant GIDs mapped
                            // to by the EID value
                            // However, only update with GIDs if EID value is NOT NULL
                            let colname = &cols[ci].fullname;
                            if row[ci] == Value::Null {
                                continue;
                            }
                            if let Some(i) = ghosted_cols.iter().position(|gc| helpers::str_ident_match(&colname, &gc.0)) {
                                let eid = helpers::parser_val_to_u64(&row[ci]);
                                let parent = &ghosted_cols[i].1;
                                // add condition on user column to be within the relevant GIDs
                                and_col_constraint_expr = Expr::BinaryOp {
                                    left: Box::new(and_col_constraint_expr),
                                    op: BinaryOperator::And,
                                    right: Box::new(Expr::InList {
                                        expr: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                        list: self.ghost_maps.get_gids_for_eid(eid, &parent)?.iter()
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
    
    fn issue_insert_datatable_stmt(&mut self, values: &RowPtrs, stmt: InsertStatement, db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();
        /* For all columns that are ghosted columns, generate a new ghost_id and insert
             into ghosts table with appropriate actual id value
             those as the values instead for those columns.
            This will be empty if the table is not a table with ghost columns
         */
        let ghost_cols = helpers::get_ghosted_cols_of_datatable(&self.policy_config, &stmt.table_name);
        let mut ghost_col_indices = vec![];
        // get indices of columns corresponding to ghosted vals
        if !ghost_cols.is_empty() {
            for (i, c) in (&stmt.columns).into_iter().enumerate() {
                // XXX this may overcount if a non-ghost column is a suffix of a ghost col
                if let Some(ghost_i) = ghost_cols.iter().position(|gc| helpers::str_ident_match(&c.to_string(), &gc.0)) {
                    ghost_col_indices.push((i, ghost_cols[ghost_i].1.clone()));
                }
            }
        }
        // update sources
        let mut qt_source = stmt.source.clone();
        
        /* 
         * if there are ghosted columns, we need to insert new GID->EID mappings 
         * with the values of the ghostedcol as the EID
         * and then set the GID as the new source value of the ghostedcol 
         * */
        match stmt.source {
            InsertSource::Query(q) => {
                let mut gid_rows = vec![];
                for row in 0..values.len() {
                    let mut gid_vals = vec![];
                    let valrow = values[row].borrow();
                    for col in 0..valrow.len() {
                        // add entry to ghosts table but not if new value is null
                        let mut found = false;
                        if valrow[col] != Value::Null {
                            // TODO could make this more efficient
                            for (i, parent_table) in &ghost_col_indices {
                                if col == *i {
                                    let eid = helpers::parser_val_to_u64(&valrow[col]);
                                    let gid = self.insert_ghost_parent(parent_table, eid, db)?;
                                    gid_vals.push(Expr::Value(Value::Number(gid.to_string())));
                                    found = true;
                                    break;
                                }
                            }
                        } 
                        if !found {
                            gid_vals.push(Expr::Value(valrow[col].clone()));
                        }
                    }
                    gid_rows.push(gid_vals);
                }
                let mut new_q = q.clone();
                new_q.body = SetExpr::Values(Values(gid_rows));
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
        let dur = start.elapsed();
        debug!("issue_insert_datatable_stmt prepare {}", dur.as_micros());
        db.query_drop(dt_stmt.to_string())?;
        self.cur_stat.nqueries+=1;

        let dur = start.elapsed();
        warn!("issue_insert_datatable_stmt issued {}, {}", dt_stmt, dur.as_micros());
        Ok(())
    }

    fn insert_ghost_parent(&mut self, parent_table: &str, eid: u64, db: &mut mysql::Conn) -> Result<u64, mysql::Error> {
        let view_ptr = self.views.get_view(parent_table).unwrap();
        // NOTE : this assumes that the parent must exist in the datatable!
        let vals = view_ptr.borrow().get_row_of_id(eid);
        // NOTE: this may create a *chain* of ghost parents, but only the mapping to the first
        // ghost is held in the ghost table (since the other ghost->ghost mappings don't really
        // matter)
        self.ghost_maps.insert_gid_for_eid(&self.views, &self.policy_config.ghost_policies, vals.clone(), eid, db, parent_table)
    }
       
    fn issue_update_datatable_stmt(&mut self, assign_vals: &Vec<Expr>, stmt: UpdateStatement, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();
        let ghosted_cols = helpers::get_ghosted_cols_of_datatable(&self.policy_config, &stmt.table_name);
        let mut ghosted_col_assigns = vec![];
        let mut ghosted_col_selectitems_assn = vec![];
        let mut qt_assn = vec![];

        for (i, a) in stmt.assignments.iter().enumerate() {
            // we still want to perform the update BUT we need to make sure that the updated value, if a 
            // expr with a query, reads from the MV rather than the datatables
                                
            // we won't replace any EIDs when converting assignments to values, but
            // we also want to update any ghosted col value to NULL if the EID is being set to NULL, so we put it
            // in qt_assn too (rather than only updating the GID)
            let ghosted_col_pos = ghosted_cols.iter().position(|uc| helpers::str_ident_match(&a.id.to_string(), &uc.0));
            if ghosted_col_pos.is_none() || assign_vals[i] == Expr::Value(Value::Null) {
                qt_assn.push(Assignment{
                    id: a.id.clone(),
                    value: assign_vals[i].clone(),
                });
            } 
            // if we have an assignment to a EID, we need to update the GID->EID mapping
            // instead of updating the actual data table record
            // note that we still include NULL entries so we know to delete this GID
            if let Some(ghost_pos) = ghosted_col_pos {
                ghosted_col_assigns.push((Assignment {
                    id: a.id.clone(),
                    value: assign_vals[i].clone(),
                }, ghosted_cols[ghost_pos].1.clone()));
                ghosted_col_selectitems_assn.push(SelectItem::Expr{
                    expr: Expr::Identifier(vec![a.id.clone()]),
                    alias: None,
                });
            }
        }
        let dur = start.elapsed();
        warn!("issue_update_datatable_stmt assigns dur {}us", dur.as_micros());
 
        let qt_selection = self.selection_to_datatable_selection(
            &stmt.selection, &stmt.table_name, &ghosted_cols)?;
        let dur = start.elapsed();
        warn!("issue_update_datatable_stmt selection dur {}us", dur.as_micros());
 
        // if ghosted cols are being updated, query DT to get the relevant
        // GIDs and update these GID->EID mappings in the ghosts table
        if !ghosted_col_assigns.is_empty() {
            let get_gids_stmt_from_dt = Statement::Select(SelectStatement {
                query: Box::new(Query::select(Select{
                    distinct: true,
                    projection: ghosted_col_selectitems_assn,
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
            // get the ghosted_col GIDs from the datatable
            warn!("issue_update_datatable_stmt: {}", get_gids_stmt_from_dt);
            let res = db.query_iter(format!("{}", get_gids_stmt_from_dt.to_string()))?;
            self.cur_stat.nqueries+=1;
            
            let dur = start.elapsed();
            warn!("update datatable stmt getgids dur {}us", dur.as_micros());

            let mut ghost_update_pairs = vec![vec![]; ghosted_col_assigns.len()];
            for row in res {
                let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                for i in 0..ghosted_col_assigns.len() {
                    let uc_val = &ghosted_col_assigns[i].0;
                    if uc_val.value == Expr::Value(Value::Null) {
                        ghost_update_pairs[i].push((None, helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    } else {
                        ghost_update_pairs[i].push(
                            (Some(helpers::parser_expr_to_u64(&uc_val.value)?), 
                             helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    }
                }
            }
            for (i, (_, parent)) in ghosted_col_assigns.iter().enumerate() {
                self.ghost_maps.update_eid2gids_with(&ghost_update_pairs[i], db, &parent)?;
                warn!("update datatable stmt updating gids map with {:?}", ghost_update_pairs);
            }
        }
        let update_stmt = Statement::Update(UpdateStatement{
            table_name: stmt.table_name.clone(),
            assignments : qt_assn,
            selection : qt_selection,
        });
        warn!("issue_update_dt_stmt: {}", update_stmt);
        db.query_drop(update_stmt.to_string())?;
        self.cur_stat.nqueries+=1;
        
        let dur = start.elapsed();
        warn!("issue_insert_datatable_stmt done {}", dur.as_micros());
        Ok(())
    }
    
    fn issue_delete_datatable_stmt(&mut self, stmt: DeleteStatement, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {        
        let ghosted_cols = helpers::get_ghosted_cols_of_datatable(&self.policy_config, &stmt.table_name);
        let qt_selection = self.selection_to_datatable_selection(&stmt.selection, &stmt.table_name, &ghosted_cols)?;

        let ghosted_col_selectitems = ghosted_cols.iter()
            .map(|gc| SelectItem::Expr{
                expr: Expr::Identifier(helpers::string_to_idents(&gc.0)),
                alias: None,
            })
            .collect();
       
        // get the list of GIDs to delete from the ghosts table 
        let select_gids_stmt = Statement::Select(SelectStatement {
                query: Box::new(Query::select(Select{
                        distinct: true,
                        projection: ghosted_col_selectitems,
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

        let mut ghost_update_pairs = vec![vec![]; ghosted_cols.len()];
        for row in res {
            let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
            for i in 0..ghosted_cols.len() {
                ghost_update_pairs[i].push((None, helpers::mysql_val_to_u64(&mysql_vals[i])?));
            }
        }
        for (i, (_, parent)) in ghosted_cols.iter().enumerate() {
            self.ghost_maps.update_eid2gids_with(&ghost_update_pairs[i], db, &parent)?;
            warn!("delete datatable stmt updating gids map with {:?}", ghost_update_pairs);
        }

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
        -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), mysql::Error>
    {
        warn!("issue statement: {}", stmt);
        let mut view_res : (Vec<TableColumnDef>, RowPtrs, Vec<usize>) = (vec![], vec![], vec![]);
        
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
                let writing_gids = helpers::contains_ghosted_columns(&self.policy_config, &table_name.to_string());
                
                // update sources to only be values (note: we don't actually need to do this for tables that
                // are never either ghosted or contain ghost column keys)
                let mut values = vec![];
                match source {
                    InsertSource::Query(q) => {
                        values = self.insert_source_query_to_rptrs(&q)?;
                    }
                    InsertSource::DefaultValues => (),
                }

                // issue to datatable with vals_vec BEFORE we modify vals_vec to include the
                // user_id column
                if writing_gids {
                    self.issue_insert_datatable_stmt(
                        &values, 
                        InsertStatement{
                            table_name: table_name.clone(), 
                            columns: columns.clone(), 
                            source: source.clone(),
                        },
                        db
                    )?;
                } else {
                    warn!("Issuing {}", stmt);
                    db.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }

                // insert into views
                self.views.insert(&table_name.to_string(), Some(&columns), &values)?;
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                let start = time::Instant::now();
                let writing_gids = helpers::contains_ghosted_columns(&self.policy_config, &table_name.to_string());

                let mut assign_vals = vec![];
                let mut contains_ghosted_col_id = false;
                // update all assignments to use only values
                for a in assignments {
                    assign_vals.push(self.expr_to_value_expr(&a.value, &mut contains_ghosted_col_id, &vec![])?);
                }
                let dur = start.elapsed();
                warn!("update mysql time get_assign_values: {}us", dur.as_micros());

                if writing_gids {
                    self.issue_update_datatable_stmt(
                        &assign_vals,
                        UpdateStatement{
                            table_name: table_name.clone(), 
                            assignments: assignments.clone(), 
                            selection: selection.clone()
                        }, 
                        db)?;
                    let dur = start.elapsed();
                    warn!("update mysql time issue update datatable: {}us", dur.as_micros());
                } else {
                    db.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                    let dur = start.elapsed();
                    warn!("update mysql time issue update not datatable: {}us", dur.as_micros());
                }
                // update views
                self.views.update(&table_name.to_string(), &assignments, &selection, &assign_vals)?;
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                let writing_gids = helpers::contains_ghosted_columns(&self.policy_config, &table_name.to_string());
                if writing_gids {
                    self.issue_delete_datatable_stmt(DeleteStatement{
                        table_name: table_name.clone(), 
                        selection: selection.clone(),
                    }, db)?;
                } else {
                    db.query_drop(stmt.to_string())?;
                    self.cur_stat.nqueries+=1;
                }
                self.views.delete(&table_name.to_string(), &selection)?;
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

                // create ghost maps
                self.ghost_maps.new_ghost_map(name.to_string(), db, self.params.in_memory);

                // get parent columns so that we can keep track of the graph 
                let parent_cols_of_table = helpers::get_parent_col_indices_of_datatable(&self.policy_config, &name, columns);

                // create view for this table
                self.views.add_view(
                    name.to_string(), 
                    columns,
                    &indexes,
                    &constraints,
                    &parent_cols_of_table,
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

    pub fn record_query_stats(&mut self, qtype: helpers::stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries += self.ghost_maps.get_nqueries();
        self.cur_stat.nqueries += self.subscriber.get_nqueries();
        self.cur_stat.duration = dur;
        self.cur_stat.qtype = qtype;
        self.stats.push(self.cur_stat.clone());
        self.cur_stat.clear();
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

    /*******************************************************
     ****************** UNSUBSCRIPTION *********************
     *******************************************************/
    pub fn unsubscribe<W: io::Write>(&mut self, uid: u64, db: &mut mysql::Conn, writer: QueryResultWriter<W>) 
        -> Result<(), mysql::Error> 
    {
        use policy::UnsubscribePolicy::*;

        warn!("Unsubscribing uid {}", uid);

        // table name of entity, eid, gids for eid
        let mut ghost_eid_mappings : Vec<GhostEidMapping> = vec![];

        // all sensitive entities replaced with ghosted versions
        // this is updated prior to the entity's modification (if, for example, it is assigned a
        // ghost pointer)
        let mut sensitive_entities: Vec<EntityData> = vec![];

        // track all parent-children edges, may have repeat children
        let mut parent_child_edges : HashSet<TraversedEntity> = HashSet::new();
        // track traversed children (may come multiple times via different parents)
        let mut traversed_children : HashSet<(String, u64)> = HashSet::new();
        // queue of children to look at next
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];

        // initialize with the entity specified by the uid
        let mut view_ptr = self.views.get_view(&self.policy_config.decor_etype).unwrap();
        let matching_row = HashedRowPtr::new(view_ptr.borrow().get_row_of_id(uid), view_ptr.borrow().primary_index);
        children_to_traverse.push(TraversedEntity{
                table_name: self.policy_config.decor_etype.clone(),
                eid : uid,
                vals: matching_row.clone(),
                from_table: "".to_string(),
                from_col_index: 0,
                sensitivity: OrderedFloat(-1.0),
        });

        /* 
         * Step 1: TRAVERSAL + DECORRELATION
         * TODO could parallelize to reduce time to traverse?
         */
        while children_to_traverse.len() > 0 {
            let node = children_to_traverse.pop().unwrap();

            let start = time::Instant::now();

            /*
             * All sensitive entities are ghosted in some form or another, 
             * and the original data returned to the user
             */
            sensitive_entities.push(EntityData{
                table: node.table_name.to_string(), 
                row_strs: node.vals.row().borrow().iter().map(|v| v.to_string()).collect()
            });

            let mut gid_values = vec![];
            /* 
             * 1. Get all sibling GIDs corresponding to this EID if this entity has not already been unsubscribed
             */
            if let Some(ghost_families) = self.ghost_maps.unsubscribe(node.eid, db, &node.table_name)? {
                for ghost_family in &ghost_families {
                    let mut family_ghost_names = vec![];
                    for ghost_entities in &ghost_family.family_members { 
                        // update this node's MV to have an entry for all its ghost entities 
                        // NOTE: entries must already be in the datatables!!
                        // We must ensure that any parent ghosts of these ghost entities also become
                        // visible in the MVs for referential integrity
                        self.views.insert(&ghost_entities.table, None, &ghost_entities.rptrs)?;
                       
                        for gid in &ghost_entities.gids {
                            family_ghost_names.push((ghost_entities.table.clone(), *gid));
                        }
                    }
                    gid_values.push(Value::Number(ghost_family.root_gid.to_string()));
                    ghost_eid_mappings.push(GhostEidMapping{
                        table: node.table_name.clone(), 
                        eid2gidroot: Some((node.eid, ghost_family.root_gid)), 
                        ghosts: family_ghost_names, 
                    });
                }
            }
            let mut gid_index = 0;
            let children : EntityTypeRows;
            match self.views.graph.get_children_of_parent(&node.table_name, node.eid) {
                None => continue,
                Some(cs) => children = cs,
            }
            warn!("Found children {:?} of {:?}", children, node);

            // TODO make a pointer so we don't have to clone
            for ((child_table, child_ci), child_hrptrs) in children.iter() {
                let child_table_epolicies = self.policy_config.table2policies.get(child_table).unwrap();
                view_ptr = self.views.get_view(&child_table).unwrap();

                for rptr in child_hrptrs {
                    for policy in child_table_epolicies {
                        // skip any policies for edges not to this parent table type
                        let ci = helpers::get_col_index(&policy.column, &view_ptr.borrow().columns).unwrap();
                        if ci != *child_ci ||  policy.parent != node.table_name {
                            continue;
                        }
                        
                        // we decorrelate or delete all in the parent-child direction
                        match policy.pc_policy {
                            Decorrelate(f) => {
                                assert!(f < 1.0); 
                                sensitive_entities.push(EntityData{
                                    table: child_table.to_string(), 
                                    row_strs: rptr.row().borrow().iter().map(|v| v.to_string()).collect(),
                                });
                                assert!(gid_index < gid_values.len());
                                let val = &gid_values[gid_index];
                                warn!("UNSUB Decorrelate: updating {} {:?} with {}", child_table, rptr, val);

                                // add child of decorrelated edge to traversal queue 
                                // this adds the child WITH THE GID instead of the EID
                                let child = TraversedEntity {
                                    table_name: child_table.clone(),
                                    eid: helpers::parser_val_to_u64(&rptr.row().borrow()[view_ptr.borrow().primary_index]),
                                    vals: rptr.clone(),
                                    from_table: node.table_name.clone(), 
                                    from_col_index: ci,
                                    sensitivity: OrderedFloat(-1.0),
                                };

                                self.views.update_index_and_row_of_view(&child_table, rptr.row().clone(), ci, Some(&val));
                                gid_index += 1;

                                // if child hasn't been seen yet, traverse
                                if traversed_children.insert((child.table_name.clone(), child.eid)) {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    children_to_traverse.push(child);
                                }
                            },
                            Delete(f) => {
                                assert!(f < 1.0); 
                                sensitive_entities.append(&mut self.recursive_remove(&child, db)?.into_iter().collect());
                            },
                            Retain => {
                                let child = TraversedEntity {
                                    table_name: child_table.clone(),
                                    eid: helpers::parser_val_to_u64(&rptr.row().borrow()[view_ptr.borrow().primary_index]),
                                    vals: rptr.clone(),
                                    from_table: node.table_name.clone(), 
                                    from_col_index: ci,
                                    sensitivity: OrderedFloat(1.0),
                                };
                                // if child hasn't been seen yet, traverse
                                if traversed_children.insert((child.table_name.clone(), child.eid)) {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    children_to_traverse.push(child);
                                } 
                            }
                        }
                    }
                }
            }
            warn!("UNSUB {}: Duration to traverse+decorrelate {}, {:?}: {}us", 
                      uid, node.table_name, node, start.elapsed().as_micros());
           
            // add edge to seen edges because we want to check their outgoing
            // child->parent edges for sensitivity
            parent_child_edges.insert(node.clone());
        }
        
         /* Step 3: Child->Parent Decorrelation: for all edges to the parent entity that need to reach a particular sensitivity
         * threshold, either generate new children (if possible), or remove the children. If the
         * edge can be decorrelated, decorrelate this edge (creating one ghost)
         */
        self.unsubscribe_child_parent_edges(&parent_child_edges, &mut ghost_eid_mappings, db)?;

        /*
         * Step 4: Change leaf entities to ghosts
         * TODO change all entities to ghosts...?
         */

        /*
         * Step 5: Return data to user
         */
        self.subscriber.record_unsubbed_user_and_return_results(writer, uid, &mut ghost_eid_mappings, &mut sensitive_entities, db)
    }

    pub fn unsubscribe_child_parent_edges(&mut self, 
        children: &HashSet<TraversedEntity>, 
        ghost_eid_mappings: &mut Vec<GhostEidMapping>,
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();

        // for every parent edge from each seen child
        let mut tables_to_children : HashMap<String, Vec<&TraversedEntity>> = HashMap::new();
        for child in children.iter() {
            if let Some(cs) = tables_to_children.get_mut(&child.table_name) {
                cs.push(child);
            } else {
                tables_to_children.insert(child.table_name.clone(), vec![child]);
            }
        }
        
        for (table_name, table_children) in tables_to_children.iter() {
            let mut ghosted_cols_and_types : Vec<(String, String)> = vec![];
            let mut sensitive_cols_and_types: Vec<(String, String, f64)> = vec![];
            if let Some(gcts) = self.policy_config.child_parent_ghosted_tables.get(table_name) {
                ghosted_cols_and_types = gcts.clone();
            }
            if let Some(scts) = self.policy_config.child_parent_sensitive_tables.get(table_name) {
                sensitive_cols_and_types = scts.clone();
            }
            
            let poster_child = table_children[0];
            let child_columns = self.views.get_view_columns(&poster_child.table_name);
            
            // this table type has ghosted columns! decorrelate the edges of the children
            // if they have not yet been decorrelated
            for (col, parent_table) in ghosted_cols_and_types {
                let ci = child_columns.iter().position(|c| col == c.to_string()).unwrap();
                for child in table_children {
                    // if parent is not the from_parent (which could be a ghost!),
                    if !ghosts::is_ghost_eidval(&child.vals.row().borrow()[ci]) {
                        let eid = helpers::parser_val_to_u64(&child.vals.row().borrow()[ci]);
                        
                        if let Some(family) = self.ghost_maps.take_one_ghost_family_for_eid(eid, db, &parent_table)? {
                            // this parent already has ghosts! remove the mapping from the real parent 
                            // ensure that any parent ghosts of these ghost entities also become
                            // visible in the MVs for referential integrity
                            let mut ancestor_table_ghosts = vec![];
                            for ghost_entities in &family.family_members {
                                self.views.insert(&ghost_entities.table, None, &ghost_entities.rptrs)?;
                                for gid in &ghost_entities.gids {
                                    ancestor_table_ghosts.push((family.root_table.clone(), *gid));
                                }
                            }
                            
                            // changing child to point ghost of the real parent,
                            self.views.update_index_and_row_of_view(
                                &table_name, child.vals.row().clone(), 
                                ci, Some(&Value::Number(family.root_gid.to_string())));
                            
                            ghost_eid_mappings.push(GhostEidMapping{
                                table: parent_table.clone(), 
                                eid2gidroot: Some((eid, family.root_gid)),
                                ghosts: ancestor_table_ghosts,
                            });
                        } else {
                            unimplemented!("Ghost entity must already exist for decorrelatable edges!");
                        }
                    }
                }
            }

            let mut removed = HashSet::new();
            // this table has sensitive parents! deal with accordingly
            for (col, parent_table, sensitivity) in sensitive_cols_and_types {
                if sensitivity == 0.0 {
                    // if sensitivity is 0, remove the child :-\
                    for child in table_children {
                        // TODO
                        /*if !removed.contains(*child) {
                            warn!("Unsub child-parent Removing {:?}", child);
                            removed.extend(self.recursive_remove(child, db)?);
                        }*/
                    }
                }
                if sensitivity == 1.0 {
                    // if sensitivity is 1, we don't need to do anything
                    continue
                } 
                // otherwise, collect all edges to measure sensitivity 
                let ci = child_columns.iter().position(|c| col == c.to_string()).unwrap();
                
                // don't re-add parents that were traversed...
                let mut parent_eid_counts : HashMap<u64, usize> = HashMap::new();
                
                // group all table children by EID
                for child in table_children {
                    // TODO
                    /*if removed.contains(*child) {
                        continue;
                    }*/
                    let parent_eid_val = &child.vals.row().borrow()[ci];
                    if !ghosts::is_ghost_eidval(parent_eid_val) {
                        let parent_eid = helpers::parser_val_to_u64(parent_eid_val);
                        if let Some(count) = parent_eid_counts.get_mut(&parent_eid) {
                            *count += 1;
                        } else {
                            parent_eid_counts.insert(parent_eid, 1);
                        }
                    }
                }

                for (parent_eid, sensitive_count) in parent_eid_counts.iter() {
                    // get all children of this type with the same parent entity eid
                    let childrows = self.views.graph.get_children_of_parent(&parent_table, *parent_eid).unwrap();
                    let total_count = childrows.get(&(poster_child.table_name.clone(), ci)).unwrap().len();
                    warn!("Found {} total and {} sensitive children of type {} with parent {}", 
                          total_count, sensitive_count, poster_child.table_name, parent_eid);
                    let needed = (*sensitive_count as f64 / sensitivity).ceil() as i64 - total_count as i64;

                    if needed > 0 && self.policy_config.ghost_policies.get(&poster_child.table_name).is_none() {
                        // TODO
                        // no ghost generation policy for this table; remove as many children as needed :-\
                        /*for i in 0..needed {
                            warn!("Unsub parent-child Removing {:?}", table_children[i as usize]);
                            removed.extend(self.recursive_remove(&table_children[i as usize], children, db)?);
                        }*/
                    } else if needed > 0 {
                        let gids = ghosts::generate_new_ghost_gids(needed as usize);
                        // TODO could choose a random child as the poster child 
                        warn!("Achieve child parent sensitivity: generating values for gids {:?}", gids);
                        let new_entities = ghosts::generate_new_ghosts_with_gids(
                            &self.views, &self.policy_config.ghost_policies, db, 
                            &TemplateEntity{
                                table: poster_child.table_name.clone(),
                                row: poster_child.vals.row().clone(), 
                                fixed_colvals: Some(vec![(ci, Value::Number(parent_eid.to_string()))]),
                            },
                            &gids,
                            &mut self.cur_stat.nqueries)?;

                        // insert all ghost ancestors created into the MV
                        let mut ancestor_table_ghosts = vec![];
                        for ghost_entities in &new_entities {
                            self.views.insert(&ghost_entities.table, None, &ghost_entities.rptrs)?;
                            for gid in &ghost_entities.gids {
                                ancestor_table_ghosts.push((ghost_entities.table.clone(), *gid));
                            }
                        }
                        ghost_eid_mappings.push(GhostEidMapping{
                            table: poster_child.table_name.clone(), 
                            eid2gidroot: None, 
                            ghosts: ancestor_table_ghosts,
                        });
                    }
                }
            }
        }
        warn!("UNSUB: Duration to look at and remove/desensitize child-parent edges: {}us", start.elapsed().as_micros());
        Ok(())
    }

    pub fn recursive_remove(&mut self, 
        child: &TraversedEntity, 
        db: &mut mysql::Conn) 
        -> Result<HashSet<EntityData>, mysql::Error> 
    {
        let mut seen_children : HashSet<EntityData> = HashSet::new();
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];
        children_to_traverse.push(child.clone());
        let mut node: TraversedEntity;

        while children_to_traverse.len() > 0 {
            node = children_to_traverse.pop().unwrap().clone();

            // see if any entity has a foreign key to this one; we'll need to remove those too
            // NOTE: because traversal was parent->child, all potential children down the line
            // SHOULD already been in seen_children
            if let Some(children) = self.views.graph.get_children_of_parent(&node.table_name, node.eid) {
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    let view_ptr = self.views.get_view(&child_table).unwrap();
                    for hrptr in child_hrptrs {
                        children_to_traverse.push(TraversedEntity {
                            table_name: child_table.clone(),
                            eid: helpers::parser_val_to_u64(&hrptr.row().borrow()[view_ptr.borrow().primary_index]),
                            vals: hrptr.clone(),
                            from_table: node.table_name.clone(), 
                            from_col_index: *child_ci,
                            sensitivity: OrderedFloat(0.0),
                        });
                    }
                }
            }

            self.remove_entities(&vec![node.clone()], db)?;
            seen_children.insert(EntityData {
                table: node.table_name,
                row_strs: node.vals.row().borrow().iter().map(|v| v.to_string()).collect(),
            });
        }

        Ok(seen_children)
    }

    fn remove_entities(&mut self, nodes: &Vec<TraversedEntity>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        let id_col = Expr::Identifier(helpers::string_to_idents(ID_COL));
        let eid_exprs : Vec<Expr> = nodes.iter().map(|node| Expr::Value(Value::Number(node.eid.to_string()))).collect();
        let ids: Vec<u64> = nodes.iter().map(|node| node.eid).collect();
        let selection = Some(Expr::InList{
                expr: Box::new(id_col),
                list: eid_exprs,
                negated: false,
        });
     
        warn!("UNSUB remove: deleting {:?} {:?}", nodes, ids);
        self.views.delete_rptrs_with_ids(&nodes[0].table_name, &ids)?;

        let delete_eid_from_table = Statement::Delete(DeleteStatement {
            table_name: helpers::string_to_objname(&nodes[0].table_name),
            selection: selection.clone(),
        });
        warn!("UNSUB remove: {}", delete_eid_from_table);
        db.query_drop(format!("{}", delete_eid_from_table.to_string()))?;
        self.cur_stat.nqueries+=1;
        Ok(())
    }

    /*******************************************************
     ****************** RECORRELATION *********************
     *******************************************************/
    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     */
    pub fn resubscribe(&mut self, uid: u64, ghost_eid_mappings: &Vec<GhostEidMapping>, entity_data: &Vec<EntityData>, db: &mut mysql::Conn) -> 
        Result<(), mysql::Error> {
        // TODO check auth token?
         warn!("Resubscribing uid {}", uid);
      
        let mut ghost_eid_mappings = ghost_eid_mappings.clone();
        let mut entity_data = entity_data.clone();
        self.subscriber.check_and_sort_resubscribed_data(uid, &mut ghost_eid_mappings, &mut entity_data, db)?;

        /*
         * Add resubscribing data to data tables + MVs 
         */
        // parse entity data into tables -> data
        let mut curtable = entity_data[0].table.clone();
        let mut curvals = vec![];
        for entity in entity_data {
            //warn!("processing {}, {:?}, {}", table, eid, gid);
            // do all the work for this table at once!
            if !(curtable == entity.table) {
                self.reinsert_view_rows(&curtable, &curvals, db)?;
                
                // reset 
                curtable = entity.table.clone();
                curvals = vec![entity.row_strs.clone()];
            } else {
                curvals.push(entity.row_strs.clone()); 
            }
        }
        self.reinsert_view_rows(&curtable, &curvals, db)?;

        // parse gids into table eids -> set of gids
        let mut table = ghost_eid_mappings[0].table.clone();
        let mut eid2gid = ghost_eid_mappings[0].eid2gidroot.clone();
        let mut ghosts : Vec<Vec<(String, u64)>> = vec![];
        for mapping in ghost_eid_mappings {
            // do all the work for this eid at once!
            if !(table == mapping.table && eid2gid == mapping.eid2gidroot) {
                self.resubscribe_ghosts_map(&table, &eid2gid, &ghosts, db)?;

                // reset 
                eid2gid = mapping.eid2gidroot.clone();
                table = mapping.table.clone();
                ghosts = vec![mapping.ghosts.clone()];
            } else {
                ghosts.push(mapping.ghosts.clone());
            }
        }
        self.resubscribe_ghosts_map(&table, &eid2gid, &ghosts, db)?;

        Ok(())
    }

    fn reinsert_view_rows(&mut self, curtable: &str, curvals: &Vec<Vec<String>>, db: &mut mysql::Conn) 
    -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();
        let viewptr = &self.views.get_view(curtable).unwrap();
        warn!("{}: Reinserting values {:?}", curtable, curvals);
        let mut rowptrs = vec![];
        let mut bodyvals = vec![];
        for row in curvals {
            let vals = helpers::string_vals_to_parser_vals(row, &viewptr.borrow().columns);
            rowptrs.push(Rc::new(RefCell::new(vals.clone())));
            bodyvals.push(vals.iter().map(|v| Expr::Value(v.clone())).collect());
        }

        self.views.insert(curtable, None, &rowptrs)?;
        warn!("RESUB insert into view {:?} took {}us", rowptrs, start.elapsed().as_micros());

        let insert_entities_stmt = Statement::Insert(InsertStatement{
            table_name: helpers::string_to_objname(&curtable),
            columns: self.views.get_view_columns(&curtable),
            source: InsertSource::Query(Box::new(Query{
                ctes: vec![],
                body: SetExpr::Values(Values(bodyvals)),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            })),
        });

        warn!("RESUB issuing {}", insert_entities_stmt);
        db.query_drop(format!("{}", insert_entities_stmt))?;
        self.cur_stat.nqueries+=1;
       
        warn!("RESUB db {} finish reinsert took {}us", insert_entities_stmt.to_string(), start.elapsed().as_micros());
        Ok(())
    }
 

    fn resubscribe_ghosts_map(&mut self, curtable: &str, eid2gidroot: &Option<(u64, u64)>, ghosts: &Vec<Vec<(String, u64)>>, db: &mut mysql::Conn) -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();

        let mut ghost_families : Vec<GhostFamily> = vec![];
        
        // maps from tables to the gid/rptrs of ghost entities from that table
        let mut table_to_gid_rptrs: HashMap<String, Vec<(u64, RowPtr)>> = HashMap::new();
        for ancestor_group in ghosts {
            let mut family_members = vec![]; 
            let mut cur_ancestor_table = "";
            let mut cur_ancestor_rptrs = vec![]; 
            let mut cur_ancestor_gids= vec![]; 
            for (ancestor_table, ancestor_gid) in ancestor_group {
                // get rptr for this ancestor
                let view_ptr = self.views.get_view(&ancestor_table).unwrap();
                let ancestor_rptr = view_ptr.borrow().get_row_of_id(*ancestor_gid);

                // add to rptr to list to delete
                if let Some(gidrptrs) = table_to_gid_rptrs.get_mut(ancestor_table) {
                    gidrptrs.push((*ancestor_gid, ancestor_rptr.clone()));
                } else {
                    table_to_gid_rptrs.insert(
                        ancestor_table.to_string(), 
                        vec![(*ancestor_gid, ancestor_rptr.clone())]);
                }

                /*
                 * We only care about this next part of the loop if eid2gidroot is some
                 * This means that the rptrs need to be kept around because they were ancestors of
                 * some actual entity that was decorrelated
                 */
                if eid2gidroot.is_some() {
                    if cur_ancestor_table != ancestor_table {
                        if !cur_ancestor_table.is_empty() {
                            family_members.push(TableGhostEntities{
                                table: cur_ancestor_table.to_string(),
                                gids: cur_ancestor_gids,
                                rptrs: cur_ancestor_rptrs,
                            });
                        }
                        cur_ancestor_table = ancestor_table;
                        cur_ancestor_rptrs = vec![]; 
                        cur_ancestor_gids = vec![];
                    }
                    cur_ancestor_rptrs.push(ancestor_rptr.clone());
                    cur_ancestor_gids.push(*ancestor_gid);
                }
            }
            if let Some((_eid, gidroot)) = eid2gidroot {
                if !cur_ancestor_table.is_empty() {
                    family_members.push(TableGhostEntities{
                        table: cur_ancestor_table.to_string(),
                        gids: cur_ancestor_gids,
                        rptrs: cur_ancestor_rptrs,
                    });
                }
                ghost_families.push(GhostFamily{
                    root_table: cur_ancestor_table.to_string(),
                    root_gid: *gidroot,
                    family_members: family_members,
                });
            }
        }
        // these GIDs were stored in an actual non-ghost entity before decorrelation
        // we need to update child in the MV to now show the EID
        // and also put these GIDs back in the ghost map
        if let Some((eid, gidroot)) = eid2gidroot {
            let eid_val = Value::Number(eid.to_string());

            warn!("RESUB: actually restoring {} eid {}, gprtrs {:?}", curtable, eid, ghost_families);
            if !self.ghost_maps.resubscribe(*eid, &ghost_families, db, curtable)? {
                return Err(mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, format!("not unsubscribed {}", eid))));
            }             
            // Note: all ghosts in families will be deleted from the MV, so we only need to restore
            // the EID value for the root level GID entries
            if let Some(children) = self.views.graph.get_children_of_parent(curtable, *gidroot) {
                warn!("Get children of table {} GID {}: {:?}", curtable, gidroot, children);
                // for each child row
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    let child_viewptr = self.views.get_view(&child_table).unwrap();
                    let ghosted_cols = helpers::get_ghosted_col_indices_of_datatable(
                        &self.policy_config, &child_table, &child_viewptr.borrow().columns);
                    // if the child has a column that is ghosted and the ghost ID matches this gid
                    for (ci, parent_table) in &ghosted_cols {
                        if ci == child_ci && parent_table == &curtable {
                            for hrptr in child_hrptrs {
                                if hrptr.row().borrow()[*ci].to_string() == gidroot.to_string() {
                                    // then update this child to use the actual real EID
                                    warn!("Updating child row with GID {} to point to actual eid {}", gidroot, eid_val);
                                    self.views.update_index_and_row_of_view(&child_table, hrptr.row().clone(), *ci, Some(&eid_val));
                                }
                            }
                        }
                    }
                }
            }
        }

        // delete all ghosts from from MV
        for (table, gidrptrs) in table_to_gid_rptrs.iter() {
            self.views.delete_rptrs(&table, &gidrptrs.iter().map(|(_, rptr)| rptr.clone()).collect())?;
            warn!("RESUB: remove {} ghost entities from view {} took {}us", gidrptrs.len(), table, start.elapsed().as_micros());
        }

        // delete from actual data table if was created during unsub
        // this includes any recursively created parents
        if eid2gidroot.is_none() {
            for (table, gidrptrs) in table_to_gid_rptrs.iter() {
                let select_ghosts = Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(ID_COL))),
                    list: gidrptrs.iter().map(|(gid, _)| Expr::Value(Value::Number(gid.to_string()))).collect(),
                    negated: false,
                };
                let delete_gids_as_entities = Statement::Delete(DeleteStatement {
                    table_name: helpers::string_to_objname(&table),
                    selection: Some(select_ghosts),
                });
                warn!("RESUB removing entities: {}", delete_gids_as_entities);
                db.query_drop(format!("{}", delete_gids_as_entities.to_string()))?;
                self.cur_stat.nqueries+=1;
            }
        }
        Ok(())
    }

    pub fn rebuild_view_with_all_rows(&mut self,
            name: &str,
            columns: Vec<ColumnDef>,
            constraints: Vec<TableConstraint>,
            indexes: Vec<IndexDef>,
            db: &mut mysql::Conn) 
    {
        let objname = helpers::string_to_objname(name);
        // get parent columns so that we can keep track of the graph 
        let parent_cols_of_table = helpers::get_parent_col_indices_of_datatable(&self.policy_config, &objname, &columns);
        
        // create view for this table
        self.views.add_view(
            name.to_string(), 
            &columns,
            &indexes,
            &constraints,
            &parent_cols_of_table,
        );
        let viewptr = self.views.get_view(name).unwrap();
        
        // 1. get all rows of this table; some may be ghosts
        let get_all_rows_query = Query::select(Select{
            distinct: true,
            projection: vec![SelectItem::Wildcard],
            from: vec![TableWithJoins{
                relation: TableFactor::Table{
                    name: objname,
                    alias: None,
                },
                joins: vec![],
            }],
            selection: None,
            group_by: vec![],
            having: None,
        });
        let rows = db.query_iter(get_all_rows_query.to_string()).unwrap();
        
        // 2. convert rows to appropriate rowptrs
        let mut rptrs : RowPtrs = vec![];
        for row in rows {
            let vals = row.unwrap().unwrap();
            let parsed_row = helpers::string_vals_to_parser_vals(
                &vals.iter().map(|v| helpers::mysql_val_to_string(v)).collect(), 
                &viewptr.borrow().columns);
            rptrs.push(Rc::new(RefCell::new(parsed_row)));    
        }
        // 3. insert all eid rows AND potentially ghost rows back into the view
        // We need to remove mapped-to ghosts and rewrite foreign key columns to point to EIDs at a later step
        warn!("Rebuilding view {} with all rows {:?}", name, rptrs);
        self.views.insert(name, None, &rptrs).unwrap();
    }

    pub fn reupdate_with_ghost_mappings(&mut self, db: &mut mysql::Conn) {
        // All views have been populated with BOTH ghost and real entities. In addition, none of
        // the real entities have any children yet: child view entities always point to their ghost
        // counterparts. 
        //
        // Only some of these view ghosts should be kept in the view, namely those that have no
        // real counterpart (which has unsubscribed).
        //
        // For those that do have a real counterpart, we need to remove these ghosts from the view,
        // and rewrite child view entities to point to the actual entity.

        // collect the GIDs of each table that are currently mapped to a real EID
        let mut table2gids_to_delete: HashMap<String, Vec<u64>> = HashMap::new();
        let mut table_to_eid_to_fam: Vec<(String, u64, GhostFamily)> = vec![];
        for table in self.views.get_table_names() {
            self.ghost_maps.new_ghost_map_cache_only(table.to_string());
            let mappings : Vec<GhostEidMapping> = self.ghost_maps.get_ghost_eid_mappings(db, &table).unwrap();
            for mut mapping in mappings {
                let mut family_members = vec![];

                let mut cur_table_ghost_entities = TableGhostEntities{
                    table: mapping.ghosts[0].0.clone(), 
                    gids: vec![],
                    rptrs: vec![],
                };
                mapping.ghosts.sort();
                for (ghost_table, gid) in mapping.ghosts {

                    // update current table entities with rptr
                    if ghost_table != cur_table_ghost_entities.table {
                        family_members.push(cur_table_ghost_entities); 
                        cur_table_ghost_entities = TableGhostEntities {
                            table: ghost_table.clone(),
                            gids: vec![],
                            rptrs: vec![],
                        };
                    } 
                    let rptr = self.views.get_row_of_id(&ghost_table, gid);
                    cur_table_ghost_entities.gids.push(gid);
                    cur_table_ghost_entities.rptrs.push(rptr);

                    // remember to delete this gid from the MV
                    if let Some(gids) = table2gids_to_delete.get_mut(&ghost_table) {
                        gids.push(gid);
                    } else {
                        table2gids_to_delete.insert(ghost_table, vec![gid]);
                    }
                }
                family_members.push(cur_table_ghost_entities);
                let (eid, gidroot) = mapping.eid2gidroot.unwrap();
                table_to_eid_to_fam.push((table.clone(), eid, 
                    GhostFamily {
                        root_table: mapping.table.clone(),
                        root_gid: gidroot,
                        family_members: family_members,
                    }
                ));

                // get children of this parent
                // update assignments in MV to use EID again for this gid
                warn!("Getting children of ghost entity with gid {}", gidroot);
                let children : EntityTypeRows;
                match self.views.graph.get_children_of_parent(&mapping.table, gidroot) {
                    None => continue,
                    Some(cs) => children = cs,
                }
                // for each child row
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    let child_viewptr = self.views.get_view(&child_table).unwrap();
                    let ghosted_cols = helpers::get_ghosted_col_indices_of_datatable(
                        &self.policy_config, &child_table, &child_viewptr.borrow().columns);
                    
                    // if the child has a column that is ghosted and the ghost ID matches this gid
                    for (ci, parent_table) in &ghosted_cols {
                        if ci == child_ci && &mapping.table == parent_table {
                            for hrptr in child_hrptrs {
                                if hrptr.row().borrow()[*ci].to_string() == gidroot.to_string() {
                                    
                                    // then update this child to use the actual real EID
                                    self.views.update_index_and_row_of_view(
                                        &child_table, hrptr.row().clone(), 
                                        *ci, Some(&Value::Number(eid.to_string())));
                                }
                            }
                        }
                    }
                }
            }
        }

        // remove the ghosts that are mapped to read IDs
        for (table, gids) in table2gids_to_delete.iter() {
            self.views.delete_rptrs_with_ids(table, &gids).unwrap();
        }

        // all families into ghost maps cache to recreate in-memory mapping
        self.ghost_maps.regenerate_cache_entries(&table_to_eid_to_fam);
    }
}
