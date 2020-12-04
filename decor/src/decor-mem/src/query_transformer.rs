use rand::prelude::*;
use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{select, helpers, ghosts_map, ghosts_map::GhostMaps, policy, stats, views, ID_COL, GhostMappingShard, EntityDataShard, graph::EntityTypeRows};
use crate::views::{TableColumnDef, Views, Row, RowPtrs, HashedRowPtr, RowPtr};
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::*;
use msql_srv::{QueryResultWriter};
use log::{debug, warn};
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use ordered_float::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TraversedEntity {
    pub table_name: String,
    pub eid : u64,
    pub columns: Vec<Ident>,
    pub vals : HashedRowPtr,

    pub from_table: String,
    pub from_col_index: usize,
    pub sensitivity: OrderedFloat<f64>,
}

pub struct QueryTransformer {
    views: Views,
    
    // map from table names to columns with ghost parent IDs
    decor_config: policy::Config,
    ghost_policies: policy::EntityGhostPolicies,
    ghost_maps: GhostMaps,
    unsubscribed: HashMap<u64, (String, String)>,
    rng: ThreadRng,
    hasher : Sha3,
    
    // for tests
    params: super::TestParams,
    pub cur_stat: stats::QueryStat,
    pub stats: Vec<stats::QueryStat>,
}

impl QueryTransformer {
    pub fn new(policy: policy::ApplicationPolicy, params: &super::TestParams) -> Self {
        let decor_config = policy::policy_to_config(&policy);
        QueryTransformer{
            views: Views::new(),
            decor_config: decor_config,
            ghost_policies: policy.ghost_policies,
            ghost_maps: GhostMaps::new(),
            unsubscribed: HashMap::new(),
            rng: rand::thread_rng(),
            hasher : Sha3::sha3_256(),
            
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
        let ghost_cols = helpers::get_ghosted_cols_of_datatable(&self.decor_config, &stmt.table_name);
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
        debug!("issue_insert_datatable_stmt issued {}, {}", dt_stmt, dur.as_micros());
        Ok(())
    }

    fn insert_ghost_parent(&mut self, parent_table: &str, eid: u64, db: &mut mysql::Conn) -> Result<u64, mysql::Error> {
        let view_ptr = self.views.get_view(parent_table).unwrap();
        let matching= select::get_rptrs_matching_constraint(
                        &Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(helpers::string_to_idents(ID_COL))),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(Value::Number(eid.to_string()))),
                        }, &view_ptr.borrow(), &view_ptr.borrow().columns);
        assert!(matching.len() == 1, format!("Matching parent returned {:?}", matching));
        let vals = matching.iter().next().unwrap();
        self.ghost_maps.insert_gid_for_eid(&self.views, &self.ghost_policies, vals.row().clone(), eid, db, parent_table)
    }
       
    fn issue_update_datatable_stmt(&mut self, assign_vals: &Vec<Expr>, stmt: UpdateStatement, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();
        let ghosted_cols = helpers::get_ghosted_cols_of_datatable(&self.decor_config, &stmt.table_name);
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
        let ghosted_cols = helpers::get_ghosted_cols_of_datatable(&self.decor_config, &stmt.table_name);
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
                let writing_gids = helpers::contains_ghosted_columns(&self.decor_config, &table_name.to_string());
                
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
                self.views.insert(&table_name.to_string(), &columns, &values, false)?;
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                let start = time::Instant::now();
                let writing_gids = helpers::contains_ghosted_columns(&self.decor_config, &table_name.to_string());

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
                let writing_gids = helpers::contains_ghosted_columns(&self.decor_config, &table_name.to_string());
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
                self.ghost_maps.insert(name.to_string(), db, self.params.in_memory);

                // get parent columns so that we can keep track of the graph 
                let parent_cols_of_table = helpers::get_parent_col_indices_of_datatable(&self.decor_config, &name, columns);

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

    pub fn record_query_stats(&mut self, qtype: stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries += self.ghost_maps.get_nqueries();
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
     ****************** DECORRELATION *********************
     *******************************************************/
    pub fn unsubscribe<W: io::Write>(&mut self, uid: u64, db: &mut mysql::Conn, writer: QueryResultWriter<W>) -> Result<(), mysql::Error> 
    {
        warn!("Unsubscribing uid {}", uid);

        // table name of entity, eid, gids for eid
        let mut generated_eid_gids: Vec<(String, Option<u64>, u64)> = vec![];
        // all completely decorrelated entities
        let mut removed_entities: Vec<(String, Vec<String>)> = vec![];

        // track all parent-children edges, may have repeat children
        let mut parent_child_edges : HashSet<TraversedEntity> = HashSet::new();
        // track the children that have been traversed (may come multiple times via different
        // parents)
        let mut traversed_children : HashSet<(String, u64)> = HashSet::new();
        // queue of children to look at next
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];

        // initialize with the entity specified by the uid
        let mut view_ptr = self.views.get_view(&self.decor_config.entity_type_to_decorrelate).unwrap();
        let matching_users = select::get_rptrs_matching_constraint(
                        &Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(helpers::string_to_idents(ID_COL))),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
                        }, &view_ptr.borrow(), &view_ptr.borrow().columns);
        assert!(matching_users.len() == 1, format!("Matching users returned {:?}", matching_users));
        let user_vals = matching_users.iter().next().unwrap();
        children_to_traverse.push(TraversedEntity{
                table_name: self.decor_config.entity_type_to_decorrelate.clone(),
                eid : uid,
                columns: view_ptr.borrow().columns.iter().map(|tcd| Ident::new(tcd.colname.clone())).collect(),
                vals: user_vals.clone(),
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

            // this is a leaf table!
            if !self.decor_config.tables_with_children.contains(&node.table_name) {
                // this table has no retained links to parents, this leaf will be completely
                // abandoned. remove it and give it to the user for later
                if self.decor_config.completely_decorrelated_children.contains(&node.table_name) {
                    removed_entities.push((node.table_name.to_string(), node.vals.row().borrow().iter().map(|v| v.to_string()).collect()));
                    self.remove_entities(&vec![node], db)?;
                } else {
                    parent_child_edges.insert(node.clone());
                }
                continue;
            }
            let children : EntityTypeRows;
            match self.views.graph.get_children_of_parent(&node.table_name, node.eid) {
                None => continue,
                Some(cs) => children = cs,
            }
            warn!("Found children {:?} of {:?}", children, node);
            
            let mut gid_values = vec![];
            if let Some((gids, rptrs)) = self.ghost_maps.unsubscribe(node.eid, db, &node.table_name)? {
                // 1. Get all GIDs corresponding to this EID if this entity has not already been unsubscribed
                assert!(!gids.is_empty());
                gid_values = gids.iter().map(|g| Value::Number(g.to_string())).collect();
                for gid in &gids {
                    gid_values.push(Value::Number(gid.to_string()));
                    generated_eid_gids.push((node.table_name.clone(), Some(node.eid), *gid));
                }

                // 2. update this node's MV to have an entry for all the parent ghost entities 
                // NOTE: entries are already in the datatables!!
                // we don't need to modify the graph...
                let columns = self.views.get_view_columns(&node.table_name);
                self.views.insert(&node.table_name, &columns, &rptrs, true)?;
            }

            for ((child_table, child_ci), child_hrptrs) in children.iter() {
                view_ptr = self.views.get_view(&child_table).unwrap();
                let columns = self.views.get_view_columns(&child_table);
                let ghosted_cols = helpers::get_ghosted_col_indices_of(&self.decor_config, &child_table, &view_ptr.borrow().columns); 
                let sensitive_cols = helpers::get_sensitive_col_indices_of(
                    &self.decor_config, &child_table, &view_ptr.borrow().columns); 

                let mut gid_index = 0;
                for rptr in child_hrptrs {
                    // ********************  DECORRELATED EDGES OF EID ************************ //
                    for (ci, _) in &ghosted_cols {
                        if ci == child_ci {
                            // decorrelate:
                            // swap out value in MV to be the GID instead of the EID
                            assert!(gid_index < gid_values.len());
                            let val = &gid_values[gid_index];
                            warn!("UNSUB: updating {:?} with {}", rptr, val);

                            view_ptr.borrow_mut().update_index_and_row(rptr.row().clone(), *ci, Some(&val));
                            gid_index += 1;

                            // add child of decorrelated edge to traversal queue 
                            // NOTE: this adds the child WITH the GID instead of the EID
                            let child = TraversedEntity {
                                table_name: child_table.clone(),
                                eid: helpers::parser_val_to_u64(&rptr.row().borrow()[view_ptr.borrow().primary_index]),
                                columns: columns.clone(),
                                vals: rptr.clone(),
                                from_table: node.table_name.clone(), 
                                from_col_index: *ci, 
                                sensitivity: OrderedFloat(-1.0),
                            };
                            // if child hasn't been seen yet, traverse
                            if traversed_children.insert((child.table_name.clone(), child.eid)) {
                                warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                children_to_traverse.push(child);
                            }
                        }
                    }
                    // ********************  SENSITIVE EDGES OF EID ************************ //
                    for (ci, _, sensitivity) in &sensitive_cols {
                        if child_ci == ci {
                            // add child of sensitive edge to traversal queue 
                            let child = TraversedEntity {
                                table_name: child_table.clone(),
                                eid: helpers::parser_val_to_u64(&rptr.row().borrow()[view_ptr.borrow().primary_index]),
                                columns: columns.clone(),
                                vals: rptr.clone(),
                                from_table: node.table_name.clone(), 
                                from_col_index: *ci,
                                sensitivity: OrderedFloat(*sensitivity),
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
            warn!("UNSUB {}: Duration to traverse+decorrelate {}, {:?}: {}us", 
                      uid, node.table_name, node, start.elapsed().as_micros());
           
            // if this is a table whose edges to children are *all* decorrelated, then 
            // remove these nodes, add to data to return to user
            // because these are removed, we don't need to add them to nodes to check
            if self.decor_config.completely_decorrelated_parents.contains(&node.table_name) {
                removed_entities.push((node.table_name.to_string(), node.vals.row().borrow().iter().map(|v| v.to_string()).collect()));
                self.remove_entities(&vec![node], db)?;
            } else {
                // in all other cases, add edge to seen edges because we want to check their outgoing
                // child->parent edges for sensitivity
                parent_child_edges.insert(node.clone());
            }
        }

        /* 
         * Step 2 (cont): for all edges to the parent entity that need to reach a particular sensitivity
         * threshold, either generate new children (if possible), or remove the children
         */
        let start = time::Instant::now();
        let mut removed = HashSet::new();
        for child in parent_child_edges.iter() {
            if removed.contains(child) {
                continue;
            }
            if child.sensitivity == 0.0 { //remove 
                warn!("Removing {:?} during step 2", child);
                removed.extend(self.recursive_remove(child, &parent_child_edges, db)?);
            } else if child.sensitivity.0 > 0.0 && child.sensitivity.0 < 1.0 { 
                // how many other children of this type came from this parent?
                // do we need to add children?
                removed.extend(self.achieve_parent_child_sensitivity(child, &parent_child_edges, &mut generated_eid_gids, db)?);
            }
        }
        warn!("UNSUB {}: Duration to remove or add sensitive step 2: {}us", uid, start.elapsed().as_micros());

        /*
         * Step 3: Child->Parent Decorrelation: for all edges to the parent entity that need to reach a particular sensitivity
         * threshold, either generate new children (if possible), or remove the children. If the
         * edge can be decorrelated, decorrelate this edge (creating one ghost)
         */
        self.unsubscribe_child_parent_edges(&parent_child_edges, &mut generated_eid_gids, db)?;

        // cache the hash of the gids we are returning
        generated_eid_gids.sort();
        let serialized1 = serde_json::to_string(&generated_eid_gids).unwrap();
        self.hasher.input_str(&serialized1);
        let result1 = self.hasher.result_str();
        self.hasher.reset();
       
        // note, the recipient has to just return the entities in order...
        removed_entities.sort();
        let serialized2 = serde_json::to_string(&removed_entities).unwrap();
        self.hasher.input_str(&serialized2);
        let result2 = self.hasher.result_str();
        self.hasher.reset();
        self.unsubscribed.insert(uid, (result1, result2));
        ghosts_map::answer_rows(writer, serialized1, serialized2)
    }

    pub fn unsubscribe_child_parent_edges(&mut self, 
        children: &HashSet<TraversedEntity>, 
        generated_eid_gids: &mut Vec<(String, Option<u64>, u64)>,  
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
            if let Some(gcts) = self.decor_config.child_parent_ghosted_tables.get(table_name) {
                ghosted_cols_and_types = gcts.clone();
            }
            if let Some(scts) = self.decor_config.child_parent_sensitive_tables.get(table_name) {
                sensitive_cols_and_types = scts.clone();
            }
            
            let poster_child = table_children[0];
            let columns = &poster_child.columns;
            
            // this table type has ghosted columns! decorrelate the edges of the children
            // if they have not yet been decorrelated
            for (col, parent_table) in ghosted_cols_and_types {
                let ci = columns.iter().position(|c| col == c.to_string()).unwrap();
                for child in table_children {
                    // if parent is not the from_parent (which could be a ghost!),
                    // generate a new parent and point the child to that parent
                    if !helpers::is_ghost_eid(&child.vals.row().borrow()[ci]) {
                        warn!("Generating foreign key entity for {}", parent_table);
                        let mut new_entities = vec![];
                        let newgid = policy::generate_foreign_key_value(
                            &self.views, &self.ghost_policies, db, generated_eid_gids, &parent_table, &mut new_entities, &mut self.cur_stat.nqueries)?;
                        for (table, columns, rptrs) in &new_entities {
                            self.views.insert(&table, &columns, &rptrs, true)?;
                        }
                        child.vals.row().borrow_mut()[ci] = newgid;
                    }
                }
            }

            let mut removed = HashSet::new();
            // this table has sensitive parents! deal with accordingly
            for (col, parent_table, sensitivity) in sensitive_cols_and_types {
                if sensitivity == 0.0 {
                    // if sensitivity is 0, remove the child :-\
                    for child in table_children {
                        if !removed.contains(*child) {
                            warn!("Unsub child-parent Removing {:?}", child);
                            removed.extend(self.recursive_remove(child, children, db)?);
                        }
                    }
                }
                if sensitivity == 1.0 {
                    // if sensitivity is 1, we don't need to do anything
                    continue
                } 
                // otherwise, collect all edges to measure sensitivity 
                let ci = poster_child.columns.iter().position(|c| col == c.to_string()).unwrap();
                
                // don't re-add parents that were traversed...
                let mut parent_eid_counts : HashMap<u64, usize> = HashMap::new();
                
                // group all table children by EID
                for child in table_children {
                    if removed.contains(*child) {
                        continue;
                    }
                    let parent_eid_val = &child.vals.row().borrow()[ci];
                    if !helpers::is_ghost_eid(parent_eid_val) {
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

                    if needed > 0 && self.ghost_policies.get(&poster_child.table_name).is_none() {
                        // no ghost generation policy for this table; remove as many children as needed :-\
                        for i in 0..needed {
                            warn!("Unsub parent-child Removing {:?}", table_children[i as usize]);
                            removed.extend(self.recursive_remove(&table_children[i as usize], children, db)?);
                        }
                    } else if needed > 0 {
                        let mut gids = vec![];
                        for _i in 0..needed {
                            let gid = self.rng.gen_range(ghosts_map::GHOST_ID_START, ghosts_map::GHOST_ID_MAX);
                            gids.push(Value::Number(gid.to_string()));
                            generated_eid_gids.push((poster_child.table_name.clone(), None, gid));
                        }
                        // TODO could choose a random child as the poster child 
                        warn!("Achieve child parent sensitivity: generating values for gids {:?}", gids);
                        let new_entities = policy::generate_new_entities_from(
                            &self.views,
                            &self.ghost_policies,
                            db, 
                            generated_eid_gids,
                            &poster_child.table_name,
                            poster_child.vals.row().clone(), 
                            &gids,
                            Some((ci, Value::Number(parent_eid.to_string()))),
                            &mut self.cur_stat.nqueries)?;
                        for (table, columns, rptrs) in &new_entities {
                            self.views.insert(table, columns, rptrs, true)?;
                        }
                    }
                }
            }
        }
        warn!("UNSUB: Duration to look at and remove/desensitize child-parent edges: {}us", start.elapsed().as_micros());
        Ok(())
    }

    pub fn achieve_parent_child_sensitivity(&mut self, 
        child: &TraversedEntity, 
        descendants: &HashSet<TraversedEntity>, 
        generated_eid_gids: &mut Vec<(String, Option<u64>, u64)>,  
        db: &mut mysql::Conn) 
        -> Result<HashSet<TraversedEntity>, mysql::Error> 
    {
        if self.ghost_policies.get(&child.table_name).is_none() {
            // no ghost generation policy for this table!!!
            // just remove the child
            warn!("no_gen: parent-child Removing {:?}", child);
            return self.recursive_remove(child, descendants, db);
        }

        let mut removed = HashSet::new();
        // count the number of entities with the same parent
        let mut count = 0;
        let parent_val = &child.vals.row().borrow()[child.from_col_index];
        for desc in descendants.iter() {
            // check if it's the same type of edge
            // note that this will count child too...
            if desc.table_name == child.table_name
                && desc.from_table == child.from_table 
                && desc.vals.row().borrow()[desc.from_col_index] == *parent_val
            {
                count += 1;
                removed.insert(desc.clone());
            } 
        }
        warn!("Found {} total and {} sensitive children of type {} with parent {}", count, count, child.table_name, parent_val);
        let needed = (count as f64 / child.sensitivity.0).ceil() as usize - count;
        if needed > 0 {
            // generate ghosts until the threshold is met
            let mut gids = vec![];
            for _i in 0..needed {
                let gid = self.rng.gen_range(ghosts_map::GHOST_ID_START, ghosts_map::GHOST_ID_MAX);
                generated_eid_gids.push((child.table_name.clone(), None, gid));
                gids.push(Value::Number(gid.to_string()));
            }
            warn!("Achieve parent child sensitivity: generating values for gids {:?}", gids);
            let new_entities = policy::generate_new_entities_from(
                &self.views,
                &self.ghost_policies,
                db, 
                generated_eid_gids,
                &child.table_name,
                child.vals.row().clone(), 
                &gids,
                Some((child.from_col_index, parent_val.clone())),
                &mut self.cur_stat.nqueries)?;
            for (table, columns, rptrs) in &new_entities {
                self.views.insert(table, columns, rptrs, true)?;
            }
        }
        Ok(removed)
    }

    pub fn recursive_remove(&mut self, 
        child: &TraversedEntity, 
        descendants: &HashSet<TraversedEntity>, 
        db: &mut mysql::Conn) 
        -> Result<HashSet<TraversedEntity>, mysql::Error> 
    {
        let mut seen_children : HashSet<TraversedEntity> = HashSet::new();
        let mut children_to_traverse: Vec<&TraversedEntity> = vec![];
        children_to_traverse.push(child);
        let mut node: TraversedEntity;

        while children_to_traverse.len() > 0 {
            node = children_to_traverse.pop().unwrap().clone();

            // see if any entity has a foreign key to this one; we'll need to remove those too
            // NOTE: because traversal was parent->child, all potential children down the line
            // SHOULD already been in seen_children
            for desc in descendants.iter() {
                // if this is a descendant of the current child
                if desc.from_table == node.table_name  
                    && helpers::parser_val_to_u64(&desc.vals.row().borrow()[desc.from_col_index]) == node.eid
                        && !seen_children.contains(&desc)
                {
                    children_to_traverse.push(desc);
                }
            }

            self.remove_entities(&vec![node.clone()], db)?;
            seen_children.insert(node);
        }

        Ok(seen_children)
    }

    fn remove_entities(&mut self, nodes: &Vec<TraversedEntity>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        let id_col = Expr::Identifier(helpers::string_to_idents(ID_COL));
        let eid_exprs : Vec<Expr> = nodes.iter().map(|node| Expr::Value(Value::Number(node.eid.to_string()))).collect();
        let selection = Some(Expr::InList{
                expr: Box::new(id_col),
                list: eid_exprs,
                negated: false,
        });

        warn!("UNSUB remove: deleting {:?} {:?}", nodes, selection);
        self.views.delete(&nodes[0].table_name, &selection)?;

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
    pub fn resubscribe(&mut self, uid: u64, gids: &GhostMappingShard, entity_data: &EntityDataShard, db: &mut mysql::Conn) -> 
        Result<(), mysql::Error> {
        // TODO check auth token?
         warn!("Resubscribing uid {}", uid);
      
        let mut gids = gids.clone();
        let mut entity_data = entity_data.clone();
        match self.unsubscribed.get(&uid) {
            Some((gidshash, datahash)) => {
                gids.sort();
                let serialized = serde_json::to_string(&gids).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *gidshash != hashed {
                    warn!("Resubscribing {} gidshash mismatch {}, {}", uid, gidshash, hashed);
                    return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!(
                                    "User attempting to resubscribe with bad data {} {}", uid, serialized))));
                }
                self.hasher.reset();

                entity_data.sort();
                let serialized = serde_json::to_string(&entity_data).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *datahash != hashed {
                    warn!("Resubscribing {} datahash mismatch {}, {}", uid, datahash, hashed);
                    return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!(
                                    "User attempting to resubscribe with bad data {} {}", uid, serialized))));
                }
                self.hasher.reset();
                self.unsubscribed.remove(&uid); 
            }
            None => {
                return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!("User not unsubscribed {}", uid))));
            }
        }
        warn!("Entity data is {:?}", entity_data);

        /*
         * Add resubscribing data to data tables + MVs 
         */
        // parse entity data into tables -> data
        let mut curtable = &entity_data[0].0;
        let mut curvals = vec![];
        for (table, vals) in &entity_data {
            //warn!("processing {}, {:?}, {}", table, eid, gid);
            // do all the work for this table at once!
            if !(curtable == table) {
                self.reinsert_with_vals(curtable, &curvals, db)?;
                
                // reset 
                curtable = table;
                curvals = vec![vals];
            } else {
                curvals.push(vals); 
            }
        }
        self.reinsert_with_vals(curtable, &curvals, db)?;

        // parse gids into table eids -> set of gids
        let mut curtable = &gids[0].0;
        let mut cureid = &gids[0].1;
        let mut curgids : Vec<u64> = vec![];
        for (table, eid, gid) in &gids {
            //warn!("processing {}, {:?}, {}", table, eid, gid);
            // do all the work for this eid at once!
            if !(curtable == table && cureid == eid) {
                self.resubscribe_with_gids(curtable, cureid, &curgids, db)?;

                // reset 
                cureid = eid;
                curtable = table;
                curgids = vec![*gid];
            } else {
                curgids.push(*gid);
            }
        }
        self.resubscribe_with_gids(curtable, cureid, &curgids, db)?;

        Ok(())
    }

    fn reinsert_with_vals(&mut self, curtable: &str, curvals: &Vec<&Vec<String>>, db: &mut mysql::Conn) 
    -> Result<(), mysql::Error> 
    {
        let columns = self.views.get_view_columns(curtable);
        let viewptr = &self.views.get_view(curtable).unwrap();
        warn!("{}: Reinserting values {:?}", curtable, curvals);
        let mut rowptrs = vec![];
        let mut bodyvals = vec![];
        for row in curvals {
            let vals = helpers::string_vals_to_parser_vals(row, &viewptr.borrow().columns);
            rowptrs.push(Rc::new(RefCell::new(vals.clone())));
            bodyvals.push(vals.iter().map(|v| Expr::Value(v.clone())).collect());
        }

        self.views.insert(curtable, &columns, &rowptrs, false)?;

        let insert_entities_stmt = Statement::Insert(InsertStatement{
            table_name: helpers::string_to_objname(&self.decor_config.entity_type_to_decorrelate.clone()),
            columns: columns,
            source: InsertSource::Query(Box::new(Query{
                ctes: vec![],
                body: SetExpr::Values(Values(bodyvals)),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            })),
        });

        warn!("RESUB INSERT {}", insert_entities_stmt.to_string());
        db.query_drop(format!("{}", insert_entities_stmt.to_string()))?;
        self.cur_stat.nqueries+=1;
        Ok(())
    }
 

    fn resubscribe_with_gids(&mut self, curtable: &str, cureid: &Option<u64>, curgids: &Vec<u64>, db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        let gid_exprs : Vec<Expr> = curgids
            .iter()
            .map(|g| Expr::Value(Value::Number(g.to_string())))
            .collect();
        
        // get rows from MV belonging to the ghost entities
        let select_ghosts = Expr::InList{
            expr: Box::new(Expr::Identifier(helpers::string_to_idents(ID_COL))),
            list: gid_exprs.clone(),
            negated: false,
        };
        let view_ptr = self.views.get_view(&curtable).unwrap();
        let mut view = view_ptr.borrow_mut();
        // don't use graph because ghosts weren't added to graph
        let ghost_rptrs = select::get_rptrs_matching_constraint(&select_ghosts, &view, &view.columns);

        // delete from actual data table if was created during unsub
        if cureid.is_none() {
            let delete_gids_as_entities = Statement::Delete(DeleteStatement {
                table_name: helpers::string_to_objname(&curtable),
                selection: Some(select_ghosts.clone()),
            });
            warn!("RESUB removing entities: {}", delete_gids_as_entities);
            db.query_drop(format!("{}", delete_gids_as_entities.to_string()))?;
            self.cur_stat.nqueries+=1;

            // delete from MV
            view.delete_ghost_rptrs(&ghost_rptrs)?;
        }

        // this GID was prior stored in an actual non-ghost entity
        // we need to update these entities in the MV to now show the EID
        // and also put these GIDs back in the ghost map
        else if let Some(eid) = cureid {

            warn!("RESUB: actually restoring {} eid {}, gprtrs {:?} for gids {:?}", curtable, eid, ghost_rptrs, curgids);
            let eid_val = Value::Number(eid.to_string());
            if !self.ghost_maps.resubscribe(*eid, &ghost_rptrs, db, curtable)? {
                return Err(mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, format!("not unsubscribed {}", eid))));
            }             
            
            // update assignments in MV to use EID again
            // NOTE: assuming that parent-child decorrelation is a superset of child-parent
            for (dtname, ghosted_cols) in self.decor_config.parent_child_ghosted_tables.iter() {
                let view_ptr = self.views.get_view(&dtname).unwrap();
                let mut view = view_ptr.borrow_mut();
                let mut select_constraint = Expr::Value(Value::Boolean(false));
                let mut cis = vec![];
                for (col, _) in ghosted_cols {
                    // push all user columns, even if some of them might not "belong" to us
                    cis.push(view.columns.iter().position(|c| helpers::tablecolumn_matches_col(c, col)).unwrap());
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

                let rptrs_to_update = select::get_rptrs_matching_constraint(&select_constraint, &view, &view.columns);
                for rptr in &rptrs_to_update {
                    for ci in &cis {
                        warn!("RESUB: updating {:?} with {}", rptr, eid_val);
                        // update the columns to use the uid
                        if curgids.iter().any(|g| g.to_string() == rptr.row().borrow()[*ci].to_string()) {
                            view.update_index_and_row(rptr.row().clone(), *ci, Some(&eid_val));
                        }
                    }
                }
            }

            // delete ghost entities from MV
            view.delete_ghost_rptrs(&ghost_rptrs)?;
        }
        Ok(())
    }
}
