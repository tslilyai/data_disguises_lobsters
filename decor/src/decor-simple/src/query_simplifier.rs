use sql_parser::ast::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::*;
use crate::views::*;
use crate::helpers;

/**************************************** 
 **** Converts Queries/Exprs to Values **
 ****************************************/
/* 
 * This issues the specified query to the MVs, and returns a VALUES query that
 * represents the values retrieved by the query to the MVs.
 * NOTE: queries are read-only operations (whereas statements may be writes)
 */
fn query_to_value_query(query: &Query, views: &Views) -> Result<Query, mysql::Error> {
    let mut vals_vec : Vec<Vec<Expr>>= vec![];
    for row in views.query_iter(query)?.1 {
        vals_vec.push(row.borrow().iter().map(|v| Expr::Value(v.clone())).collect());
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
 */
pub fn expr_to_value_expr(expr: &Expr, views: &Views) 
    -> Result<Expr, mysql::Error> 
{
    let new_expr = match expr {
        Expr::FieldAccess {
            expr,
            field,
        } => {
            Expr::FieldAccess {
                expr: Box::new(expr_to_value_expr(&expr, views)?),
                field: field.clone(),
            }
        }
        Expr::WildcardAccess(e) => {
            Expr::WildcardAccess(Box::new(expr_to_value_expr(&e, views)?))
        }
        Expr::IsNull{
            expr,
            negated,
        } => Expr::IsNull {
            expr: Box::new(expr_to_value_expr(&expr, views)?),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut new_list = vec![];
            for e in list {
                new_list.push(expr_to_value_expr(&e, views)?);
            }
            Expr::InList {
                expr: Box::new(expr_to_value_expr(&expr, views)?),
                list: new_list,
                negated: *negated,
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let new_query = query_to_value_query(&subquery, views)?;
            // otherwise just return table column IN subquery
            Expr::InSubquery {
                expr: Box::new(expr_to_value_expr(&expr, views)?),
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
            let new_low = expr_to_value_expr(&low, views)?;
            let new_high = expr_to_value_expr(&high, views)?;
            Expr::Between {
                expr: Box::new(expr_to_value_expr(&expr, views)?),
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
            let new_left = expr_to_value_expr(&left, views)?;
            let new_right = expr_to_value_expr(&right, views)?;
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
            expr: Box::new(expr_to_value_expr(&expr, views)?),
        },
        Expr::Cast{
            expr,
            data_type,
        } => Expr::Cast{
            expr: Box::new(expr_to_value_expr(&expr, views)?),
            data_type: data_type.clone(),
        },
        Expr::Collate {
            expr,
            collation,
        } => Expr::Collate{
            expr: Box::new(expr_to_value_expr(&expr, views)?),
            collation: collation.clone(),
        },
        Expr::Nested(expr) => Expr::Nested(Box::new(expr_to_value_expr(&expr, views)?)),
        Expr::Row{
            exprs,
        } => {
            let mut new_exprs = vec![];
            for e in exprs {
                new_exprs.push(expr_to_value_expr(&e, views)?);
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
                        new_exprs.push(expr_to_value_expr(&e, views)?);
                    }
                    FunctionArgs::Args(new_exprs)
                }                
            },
            filter: match &f.filter {
                Some(filt) => Some(Box::new(expr_to_value_expr(&filt, views)?)),
                None => None,
            },
            over: match &f.over {
                Some(ws) => {
                    let mut new_pb = vec![];
                    for e in &ws.partition_by {
                        new_pb.push(expr_to_value_expr(&e, views)?);
                    }
                    let mut new_ob = vec![];
                    for obe in &ws.order_by {
                        new_ob.push(OrderByExpr {
                            expr: expr_to_value_expr(&obe.expr, views)?,
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
                new_cond.push(expr_to_value_expr(&e, views)?);
            }
            let mut new_res= vec![];
            for e in results {
                new_res.push(expr_to_value_expr(&e, views)?);
            }
            Expr::Case{
                operand: match operand {
                    Some(e) => Some(Box::new(expr_to_value_expr(&e, views)?)),
                    None => None,
                },
                conditions: new_cond ,
                results: new_res, 
                else_result: match else_result {
                    Some(e) => Some(Box::new(expr_to_value_expr(&e, views)?)),
                    None => None,
                },
            }
        }
        Expr::Exists(q) => Expr::Exists(Box::new(query_to_value_query(&q, views)?)),
        Expr::Subquery(q) => Expr::Subquery(Box::new(query_to_value_query(&q, views)?)),
        Expr::Any {
            left,
            op,
            right,
        } => Expr::Any {
            left: Box::new(expr_to_value_expr(&left, views)?),
            op: op.clone(),
            right: Box::new(query_to_value_query(&right, views)?),
        },
        Expr::All{
            left,
            op,
            right,
        } => Expr::All{
            left: Box::new(expr_to_value_expr(&left, views)?),
            op: op.clone(),
            right: Box::new(query_to_value_query(&right, views)?),
        },
        Expr::List(exprs) => {
            let mut new_exprs = vec![];
            for e in exprs {
                new_exprs.push(expr_to_value_expr(&e, views)?);
            }
            Expr::List(new_exprs)
        }
        Expr::SubscriptIndex {
            expr,
            subscript,
        } => Expr::SubscriptIndex{
            expr: Box::new(expr_to_value_expr(&expr, views)?),
            subscript: Box::new(expr_to_value_expr(&subscript, views)?),
        },
        Expr::SubscriptSlice{
            expr,
            positions,
        } => {
            let mut new_pos = vec![];
            for pos in positions {
                new_pos.push(SubscriptPosition {
                    start: match &pos.start {
                        Some(e) => Some(expr_to_value_expr(&e, views)?),
                        None => None,
                    },
                    end: match &pos.end {
                        Some(e) => Some(expr_to_value_expr(&e, views)?),
                        None => None,
                    },                
                });
            }
            Expr::SubscriptSlice{
                expr: Box::new(expr_to_value_expr(&expr, views)?),
                positions: new_pos,
            }
        }
        _ => expr.clone(),
    };
    Ok(new_expr)
}

/* 
 * Convert all expressions to insert to primitive values
 */
pub fn insert_source_query_to_rptrs(q: &Query, views: &Views) 
    -> Result<RowPtrs, mysql::Error> 
{
    let mut vals_vec : RowPtrs = vec![];
    match &q.body {
        SetExpr::Values(Values(expr_vals)) => {
            // NOTE: only need to modify values if we're dealing with a DT,
            // could perform check here rather than calling vals_vec
            for row in expr_vals {
                let mut vals_row : Row = vec![];
                for val in row {
                    let value_expr = expr_to_value_expr(&val, views)?;
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
            vals_vec = views.query_iter(q)?.1;
        }
    }    
    Ok(vals_vec)
}
