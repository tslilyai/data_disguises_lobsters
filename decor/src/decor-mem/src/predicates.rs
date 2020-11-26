use crate::views::{View, TableColumnDef, HashedRowPtrs, HashedRowPtr};
use crate::{helpers};
use log::{warn, debug};
use std::collections::{HashSet};
use std::cmp::Ordering;
use std::time;
use sql_parser::ast::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColComputation {
    index1: usize,
    index2: Option<usize>, 
    val: Option<Value>, 
    binop: BinaryOperator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    ColValEq {
        index: usize, 
        val: Value,
        neg: bool,
    },

    ColValsEq {
        index: usize, 
        vals: Vec<Value>, 
        neg: bool,
    },

    ColCmp {
        index1: usize, 
        index2: Option<usize>, 
        val: Option<Value>, 
        op: BinaryOperator,
    },

    ComputeValCmp {
        computation: ColComputation,
        val: Value, 
        op: BinaryOperator,
    },
    
    Bool(bool),
}

fn lhs_expr_to_index(left: &Expr, columns: &Vec<TableColumnDef>) -> usize {
    match left {
        Expr::Identifier(_) => {
            let (_tab, col) = helpers::expr_to_col(&left);
            let ci = helpers::get_col_index(&col, &columns).unwrap();
            ci
        }
        _ => unimplemented!("Bad lhs {}", left),
    }
}

fn rhs_expr_to_index_or_value(right: &Expr, columns: &Vec<TableColumnDef>) -> (Option<usize>, Option<Value>) {
    let mut rval = None;
    let mut rindex = None;
    match right {
        Expr::Identifier(_) => {
            let (_tab, col) = helpers::expr_to_col(&right);
            rindex = Some(helpers::get_col_index(&col, &columns).unwrap());
        }
        Expr::Value(val) => {
            rval = Some(val.clone());
        }
        Expr::UnaryOp{op, expr} => {
            if let Expr::Value(ref val) = **expr {
                match op {
                    UnaryOperator::Minus => {
                        let n = -1.0 * helpers::parser_val_to_f64(&val);
                        rval = Some(Value::Number(n.to_string()));
                    }
                    _ => unimplemented!("Unary op not supported! {:?}", expr),
                }
            } else {
                unimplemented!("Unary op not supported! {:?}", expr);
            }
        }
        _ => unimplemented!("Bad rhs? {}", right),
    }
    (rindex, rval)
}

/*
 * Turn predicate into a value for row
 */
pub fn get_compute_closure_for_row(computation: &ColComputation)
    -> Box<dyn Fn(&Vec<Value>) -> Value> 
{
    let closure: Option<Box<dyn Fn(&Vec<Value>) -> Value>>;
    let start = time::Instant::now();
    let i1 = computation.index1;
    match computation.binop {
        BinaryOperator::Plus => {
            if let Some(v) = &computation.val {
                let v = v.clone();
                closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[i1].clone(), &v)));
            } else {
                let i2 = computation.index2.unwrap();
                closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[i1], &row[i2])));
            }
        }
        BinaryOperator::Minus => {
            if let Some(v) = &computation.val {
                let v = v.clone();
                closure = Some(Box::new(move |row| helpers::minus_parser_vals(&row[i1].clone(), &v)));
            } else {
                let i2 = computation.index2.unwrap();
                closure = Some(Box::new(move |row| helpers::minus_parser_vals(&row[i1], &row[i2])));
            }
        }
        _ => unimplemented!("op {} not supported to get value", computation.binop),
    }
    let dur = start.elapsed();
    warn!("Get closure for expr {:?} took: {}us", computation, dur.as_micros());
    closure.unwrap()
}

pub fn vals_satisfy_cmp(lval: &Value, rval: &Value, op: &BinaryOperator) -> bool {
    let cmp = helpers::parser_vals_cmp(&lval, &rval);
    match op {
        BinaryOperator::Eq => {
            cmp == Ordering::Equal
        }
        BinaryOperator::NotEq => {
            cmp != Ordering::Equal
        }
        BinaryOperator::Lt => {
            cmp == Ordering::Less
        }
        BinaryOperator::Gt => {
            cmp == Ordering::Greater
        }
        BinaryOperator::LtEq => {
            cmp != Ordering::Greater
        }
        BinaryOperator::GtEq => {
            cmp != Ordering::Less
        }
        _ => unimplemented!("bad binop"),
    }
}

// OR
pub fn get_predicated_rptrs_from_view(p: &Predicate, v: &View, matching_rows: &mut HashSet<HashedRowPtr>)
{
    use Predicate::*;
    let mut negate = false;
    match p {
        Bool(b) => {
            // we want to take all rows if b is true
            negate = *b;
        }
        ColValEq{index, val, neg} => {
            v.get_rptrs_of_col(*index, &val.to_string(), matching_rows);
            negate = *neg;
        }
        ColValsEq{index, vals, neg} => {
            for lv in vals{
                v.get_rptrs_of_col(*index, &lv.to_string(), matching_rows);
            }
            negate = *neg;
        }
        ColCmp{index1, index2, val, op} => {
            for (_, rptr) in v.rows.borrow().iter() {
                let row = rptr.borrow();
                let left_val = &row[*index1];
                let right_val : &Value;
                if let Some(i2) = index2 {
                    right_val = &row[*i2];
                } else {
                    right_val = &val.as_ref().unwrap();
                }
                if vals_satisfy_cmp(left_val, right_val, op) {
                    matching_rows.insert(HashedRowPtr::new(rptr.clone(), v.primary_index));
                }
            }
        }
        ComputeValCmp{computation, val, op} => {
            let comp_func = get_compute_closure_for_row(&computation);
            for (_, rptr) in v.rows.borrow().iter() {
                let left_val = comp_func(&rptr.borrow());
                if vals_satisfy_cmp(&left_val, val, op) {
                    matching_rows.insert(HashedRowPtr::new(rptr.clone(), v.primary_index));
                }
            }
        }
    }
    if negate {
        let mut all_rptrs : HashSet<HashedRowPtr> = v.rows.borrow().iter().map(
            |(_pk, rptr)| HashedRowPtr::new(rptr.clone(), v.primary_index)).collect();
        warn!("get all ptrs for selection {:?}", p);
        for rptr in matching_rows.iter() {
            all_rptrs.remove(&rptr);
        }
        *matching_rows = all_rptrs;
    }
}

// AND 
pub fn get_predicated_rptrs_from_matching(p: &Predicate, v: &View, matching_rows: &mut HashSet<HashedRowPtr>)
{
    use Predicate::*;
    match p {
        Bool(b) => {
            if !b {
                matching_rows.clear();
            }
        }
        ColValEq{index, val, neg} => {
            if let Some(hs) = v.get_indexed_rptrs_of_col(*index, &val.to_string()){
                matching_rows.retain(|r| !neg == hs.get(r).is_some());
            } else {
                matching_rows.retain(|hrp| {
                    let cmp_eq = helpers::parser_vals_cmp(&hrp.row().borrow()[*index], &val) == Ordering::Equal;
                    cmp_eq == !neg
                });
            }
        }
        ColValsEq{index, vals, neg} => {
            for lv in vals{
                if let Some(hs) = v.get_indexed_rptrs_of_col(*index, &lv.to_string()){
                    matching_rows.retain(|r| !neg == hs.get(r).is_some());
                } else {
                    matching_rows.retain(|hrp| {
                        let cmp_eq = helpers::parser_vals_cmp(&hrp.row().borrow()[*index], &lv) == Ordering::Equal;
                        cmp_eq == !neg
                    });
                }
            }
        }
        ColCmp{index1, index2, val, op} => {
            matching_rows.retain(|hrp| {
                let row = hrp.row().borrow();
                let left_val = &row[*index1];
                let right_val : &Value;
                if let Some(i2) = index2 {
                    right_val = &row[*i2];
                } else {
                    right_val = &val.as_ref().unwrap();
                }
                vals_satisfy_cmp(left_val, right_val, op)
            });
        }
        ComputeValCmp{computation, val, op} => {
            let comp_func = get_compute_closure_for_row(&computation);
            matching_rows.retain(|hrp| {
                let left_val = comp_func(&hrp.row().borrow());
                vals_satisfy_cmp(&left_val, val, op)
            });
        }
    }
}

/* 
 * returns lists of predicates  
 */
pub fn get_predicates_of_constraint(e: &Expr, v: &View, columns: &Vec<TableColumnDef>, preds: &mut Vec<Predicate>)
{
    let start = time::Instant::now();
    debug!("getting predicates of constraint {:?}", e);
    match e {
        Expr::Value(Value::Boolean(b)) => {
            preds.push(Predicate::Bool(*b));
        } 
        Expr::InList { expr, list, negated } => {
            let list_vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (_tab, col) = helpers::expr_to_col(&expr);
            if let Some(ci) = helpers::get_col_index(&col, &columns) {
                preds.push(Predicate::ColValsEq {
                    index: ci, 
                    vals: list_vals,
                    neg: *negated,
                });
            } 
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = helpers::expr_to_col(&expr);
            if let Some(ci) = helpers::get_col_index(&col, columns) {
                preds.push(Predicate::ColValEq {
                    index: ci, 
                    val: Value::Null,
                    neg: *negated,
                });
            }
        }
        Expr::BinaryOp {left, op, right} => {
            match op {
                BinaryOperator::And => {
                    get_predicates_of_constraint(left, v, columns, preds);
                    get_predicates_of_constraint(right, v, columns, preds);
                }
                BinaryOperator::Or => {
                    unimplemented!("No nested ORs yet");
                }
                _ => {
                    // special case: perform eq comparisons against fixed value 
                    let mut fastpath = false;
                    if let Expr::Identifier(_) = **left {
                        if let Expr::Value(ref val) = **right {
                            if *op == BinaryOperator::Eq || *op == BinaryOperator::NotEq {
                                debug!("getting rptrs of constraint: Fast path {:?}", e);
                                fastpath = true;
                                let (_tab, col) = helpers::expr_to_col(&left);
                                if let Some(ci) = helpers::get_col_index(&col, columns) {
                                    preds.push(Predicate::ColValEq {
                                        index: ci, 
                                        val: val.clone(),
                                        neg: *op == BinaryOperator::Eq,
                                    });
                                } 
                            }
                        }
                    }
                    if !fastpath {
                        warn!("get_rptrs_matching_constraint: Slow path {:?}", e);
                        let cmp_op = op.clone();
                        let (rindex, rval) = rhs_expr_to_index_or_value(&right, &columns);
                        match &**left {
                            Expr::Identifier(_) =>  {
                                let lindex = lhs_expr_to_index(&left, &columns);
                                preds.push(Predicate::ColCmp{
                                    index1: lindex, 
                                    index2: rindex, 
                                    val: rval,
                                    op: cmp_op,
                                });
                            }
                            Expr::BinaryOp{left, op, right} => {
                                let innerlindex = lhs_expr_to_index(&left, &columns);
                                let (innerrindex, innerrval) = rhs_expr_to_index_or_value(&right, &columns);
                                let comp = ColComputation {
                                    index1: innerlindex,
                                    index2: innerrindex,
                                    val: innerrval,
                                    binop: op.clone(),
                                };
                                preds.push(Predicate::ComputeValCmp {
                                    computation: comp, 
                                    val: rval.unwrap().clone(), 
                                    op: cmp_op,
                                });
                            }
                            _ => unimplemented!("Bad lhs? {}", left)
                        }
                    }
                }
            }
        }
        _ => unimplemented!("Constraint not supported {:?}", e),
    }
    let dur = start.elapsed();
    warn!("get predicates of constraint {} duration {}us", e, dur.as_micros());
}

pub fn get_rptrs_matching_constraint(e: &Expr, v: &View, columns: &Vec<TableColumnDef>) -> HashedRowPtrs
{
    debug!("getting rptrs of constraint {:?}", e);
    let start = time::Instant::now();
    let mut disjoint_preds : Vec<Vec<Predicate>> = vec![];
    let mut is_or = false;
    match e {
        Expr::BinaryOp{left, op, right} => {
            if *op == BinaryOperator::Or{
                let mut left_preds : Vec<Predicate> = vec![];
                let mut right_preds : Vec<Predicate> = vec![];
                get_predicates_of_constraint(&left, v, columns, &mut left_preds);
                get_predicates_of_constraint(&right, v, columns, &mut right_preds);
                disjoint_preds.push(left_preds);
                disjoint_preds.push(right_preds);
                is_or = true;
            } 
        }
        _ => (),
    } 
    if !is_or {
        let mut preds : Vec<Predicate> = vec![];
        get_predicates_of_constraint(&e, v, columns, &mut preds);
        disjoint_preds.push(preds);
    }

    let mut matching = HashSet::new();
    for mut preds in disjoint_preds {
        matching.extend(get_predicated_rptrs(&mut preds, v));
    }

    let dur = start.elapsed();
    warn!("get rptrs matching constraint {} duration {}us", e, dur.as_micros());
    matching
}

pub fn get_predicated_rptrs(preds: &mut Vec<Predicate>, v: &View) -> HashedRowPtrs {
    use Predicate::*;
    preds.sort_by(|p1, p2| match p1 {
        ColValEq {..} => Ordering::Less,
        ColValsEq {..} => Ordering::Less,
        _ => match p2 {
            ColValEq {..} => Ordering::Greater,
            ColValsEq {..} => Ordering::Greater,
            _ => Ordering::Less,
        }
    });
    let mut matching_rptrs = HashSet::new();
    get_predicated_rptrs_from_view(&preds[0], v, &mut matching_rptrs);
    for i in 1..preds.len() {
        get_predicated_rptrs_from_matching(&preds[i], v, &mut matching_rptrs);
    }
    matching_rptrs
}
