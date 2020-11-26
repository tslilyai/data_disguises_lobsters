use crate::views::{View, TableColumnDef, RowPtrs, ViewIndex, HashedRowPtr};
use crate::{helpers, INIT_CAPACITY};
use log::{warn, error, debug};
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::time;
use sql_parser::ast::*;
use std::cell::RefCell;
use std::rc::Rc;

struct ColComputation {
    index1: usize,
    index2: Option<usize>, 
    val: Option<Value>, 
    unaryop: Option<UnaryOperator>,
    binop: Option<BinaryOperator>,
}

enum Predicate {
    ColValEq {
        index: usize, 
        val: Value,
        neg: bool,
    },

    ColValsEq {
        index: usize, 
        vals: vec![Value], 
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

/*
 * Turn predicate into a value for row
 */
pub fn get_compute_closure_for_row(computation: &ColComputation, columns: &Vec<TableColumnDef>)
-> Box<dyn Fn(&Vec<Value>) -> Value> {
    let mut closure: Option<Box<dyn Fn(&Vec<Value>) -> Value>> = None;
    let start = time::Instant::now();
    if let Some(uop) = computation.unaryop {
        match uop {
            UnaryOperator::Minus => {
                let n = -1.0 * helpers::parser_val_to_f64(&val);
                closure = Some(Box::new(move |_row| Value::Number(n.to_string())));
            }
            _ => unimplemented!("Unary op not supported! {:?}", expr),
        }
    } else {
        let binop = computation.binop.unwrap();
            match binop {
                BinaryOperator::Plus => {
                    if let Some(v) = computation.val {
                        closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[computation.index1], &v)));
                    } else {
                        closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[computation.index1], &row[computation.index2.unwrap()])));
                    }
                }
                BinaryOperator::Minus => {
                    if let Some(v) = computation.val {
                        closure = Some(Box::new(move |row| helpers::minus_parser_vals(&row[computation.index1], &v)));
                    } else {
                        closure = Some(Box::new(move |row| helpers::minus_parser_vals(&row[computation.index1], &row[computation.index2.unwrap()])));
                    }
                }
                _ => unimplemented!("op {} not supported to get value", op),
            }
        }
        _ => unimplemented!("get value not supported {}", e),
    }
    let dur = start.elapsed();
    warn!("Get closure for expr {} took: {}us", e, dur.as_micros());
    closure.unwrap()
}

pub fn vals_satisfy_comp(lval: &Value, rval: &Value, op: &BinaryOperator) -> bool {
    let cmp = helpers::parser_vals_cmp(&lval, &lval);
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
        _ => unimplemented!(bad binop)
    }
}

// OR
pub fn get_predicated_rptrs_from_view(p: &Predicate, v: &View, columns: &Vec<TableColumnDef>, &mut matching_rows: HashSet<HashedRowPtr>)
{
    use Predicate::*;
    let mut negate = false;
    match p {
        Bool(b) => {
            // we want to take all rows if b is true
            negate = *b;
        }
        ColValEq{index, val, neg} => {
            v.get_rptrs_of_col(index, &val.to_string(), matching_rows);
            negate = *neg;
        }
        ColValsEq{index, vals, neg} => {
            for lv in &vals{
                v.get_rptrs_of_col(index, &lv.to_string(), matching_rows);
            }
            negate = *neg;
        }
        ColCmp{index1, index2, val, op} => {
            for (_, rptr) in v.rows.borrow() {
                let row = rptr.borrow();
                let left_val = row[index1];
                let right_val : Value;
                if let Some(i2) = index2 {
                    right_val = row[index2];
                } else {
                    right_val = v.unwrap();
                }
                if vals_satisfy_cmp(left_val, right_val, op) {
                    matching_rows.insert(HashedRowPtr::new(rptr.clone, v.primary_index));
                }
            }
        }
        ComputeValCmp{computation, val, op} => {
            let comp_func = get_compute_closure_for_row(&computation, columns);
            for (_, rptr) in v.rows.borrow() {
                let left_val = comp_func(rptr.borrow());
                if vals_satisfy_cmp(left_val, val, op) {
                    matching_rows.insert(HashedRowPtr::new(rptr.clone, v.primary_index));
                }
            }
        }
        if neg {
            let mut all_rptrs : HashSet<HashedRowPtr> = from_view.rows.borrow().iter().map(
                |(_pk, rptr)| HashedRowPtr::new(rptr.clone(), from_view.primary_index)).collect();
            warn!("get all ptrs for selection {}", selection);
            for rptr in matching_rows {
                all_rptrs.remove(&rptr);
            }
            matching_rows = all_rptrs;
        }
    }
}

// AND 
pub fn get_predicated_rptrs_from_rows(p: &Predicate, v: &View, columns: &Vec<TableColumnDef>, &mut matching_rows: HashSet<HashedRowPtr>)
{
    use Predicate::*;
    match p {
        Bool(b) => {
            if !b {
                matching_rows.clear();
            }
        }
        ColValEq{index, val, neg} => {
            if let Some(hs) = v.get_indexed_rows_of_col(index, &val.to_string()){
                matching_rows.retain(|r| !neg == hs.get(r).is_some());
            } else {
                for hrp in matching_rows {
                    let cmp_eq = helpers::parser_vals_cmp(&hrp.row().borrow()[index], &val) == Ordering::Equal;
                    let is_match = cmp_eq == !neg;
                    if (neg && cmp_eq) || (!neg && !cmp_eq) {
                        matching_rows.remove(hrp);
                    }
                }
            }
        }
        ColValsEq{index, vals, neg} => {
            for lv in &vals{
                if let Some(hs) = v.get_indexed_rows_of_col(index, &lv.to_string()){
                    matching_rows.retain(|r| !neg == hs.get(r).is_some());
                } else {
                    for hrp in matching_rows {
                        let cmp_eq = helpers::parser_vals_cmp(&hrp.row().borrow()[index], &lv) == Ordering::Equal;
                        let is_match = cmp_eq == !neg;
                        if (neg && cmp_eq) || (!neg && !cmp_eq) {
                            matching_rows.remove(hrp);
                        }
                    }
                }
            }
        }
        ColCmp{index1, index2, val, op} => {
            for hrp in matching_rows {
                let row = hrp.row().borrow();
                let left_val = row[index1];
                let right_val : Value;
                if let Some(i2) = index2 {
                    right_val = row[index2];
                } else {
                    right_val = v.unwrap();
                }
                if !vals_satisfy_cmp(left_val, right_val, op) {
                    matching_rows.remove(hrp);
                }
            }
        }
        ComputeValCmp{computation, val, op} => {
            let comp_func = get_compute_closure_for_row(&computation, columns);
            for hrp in matching_rows {
                let left_val = comp_func(hrp.row().borrow());
                if !vals_satisfy_cmp(left_val, val, op) {
                    matching_rows.remove(hrp);
                }
            }
        }
    }
}

/* 
 * returns lists of predicates; predicates in the same list are ANDed together, lists are ORed
 * together
 * 
 * */
pub fn get_predicates_of_constraint(e: &Expr, v: &View, columns: &Vec<TableColumnDef>) -> Vec<Vec<Predicate>> 
{
    let start = time::Instant::now();
    let mut matching_rows = HashSet::with_capacity(INIT_CAPACITY); //BTreeSet::new(); 
    let mut negated_res = false;
    debug!("getting rptrs of constraint {:?}", e);
    match e {
        Expr::Value(Value::Boolean(b)) => {
            Predicate::Bool(b)
        } 
        Expr::InList { expr, list, negated } => {
            let list_vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (_tab, col) = helpers::expr_to_col(&expr);
            if let Some(ci) = get_col_index(&col, &columns) {
                Predicate::ColValsEq {
                    index: ci, 
                    vals: list_vals,
                    neg: *negated,
                }
            } 
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = helpers::expr_to_col(&expr);
            if let Some(ci) = get_col_index(&col, columns) {
                Predicate::ColValEq {
                    index: ci, 
                    val: Value::Null,
                    neg: *negated,
                }
            }
        }
        Expr::BinaryOp {left, op, right} => {
            // special case: perform eq comparisons against fixed value 
            let mut fastpath = false;
            if let Expr::Identifier(_) = **left {
                if let Expr::Value(ref val) = **right {
                    debug!("getting rptrs of constraint: Fast path {:?}", e);
                    if *op == BinaryOperator::Eq || *op == BinaryOperator::NotEq {
                        fastpath = true;
                        let (_tab, col) = helpers::expr_to_col(&left);
                        if let Some(ci) = get_col_index(&col, columns) {
                            Predicate::ColValEq {
                                index: ci, 
                                val: val.clone(),
                                neg: *negated,
                            }
                        } 
                    }
                }
            }
            if !fastpath {
                warn!("get_rptrs_matching_constraint: Slow path {:?}", e);
                let left_fn = get_value_for_row_closure(&left, columns);
                let right_fn = get_value_for_row_closure(&right, columns);
            }
        }
        _ => unimplemented!("Constraint not supported {:?}", e),
    }
    let dur = start.elapsed();
    warn!("get rptrs matching constraint {} duration {}us", e, dur.as_micros());
    (negated_res, matching_rows)
}

    pub fn get_rptrs_matching_constraint(e: &Expr, v: &View, columns: &Vec<TableColumnDef>)
    -> (bool, HashSet<HashedRowPtr>)
{
    let start = time::Instant::now();
    let mut matching_rows = HashSet::with_capacity(INIT_CAPACITY); //BTreeSet::new(); 
    let mut negated_res = false;
    debug!("getting rptrs of constraint {:?}", e);
    match e {
        Expr::Value(Value::Boolean(b)) => {
            negated_res = *b;
        } 
        Expr::InList { expr, list, negated } => {
            let list_vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (_tab, col) = helpers::expr_to_col(&expr);
              
            if let Some(ci) = get_col_index(&col, &columns) {
                for lv in &list_vals {
                    v.get_rptrs_of_col(ci, &lv.to_string(), &mut matching_rows);
                }
            }
            negated_res = *negated;
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = helpers::expr_to_col(&expr);
            if let Some(ci) = get_col_index(&col, columns) {
                v.get_rptrs_of_col(ci, &Value::Null.to_string(), &mut matching_rows);
            }
            negated_res = *negated;
        }
        Expr::BinaryOp {left, op, right} => {
            match op {
                BinaryOperator::And => {
                    let (lnegated, mut lptrs) = get_rptrs_matching_constraint(left, v, columns);
                    let (rnegated, mut rptrs) = get_rptrs_matching_constraint(right, v, columns);
                    // if both are negated or not negated, return (negated?, combo of ptrs)
                    if lnegated == rnegated {
                        lptrs.retain(|lptr| rptrs.get(&lptr).is_some());
                        negated_res = lnegated;
                        matching_rows = lptrs;
                    } else {
                        if lnegated {
                            // only lefthandside negated, return (false, rptrs - lptrs)
                            for lptr in lptrs {
                                rptrs.remove(&lptr);
                            }
                            matching_rows = rptrs;
                        } else {
                            // only right negated, return (false, lptrs - rptrs)
                            for rptr in rptrs {
                                lptrs.remove(&rptr);
                            }
                            matching_rows = lptrs;
                        } 
                        negated_res = false;
                    }
                }
                BinaryOperator::Or => {
                    let (lnegated, mut lptrs) = get_rptrs_matching_constraint(left, v, columns);
                    let (rnegated, mut rptrs) = get_rptrs_matching_constraint(right, v, columns);
                    if lnegated == rnegated {
                        lptrs.extend(rptrs);
                        negated_res = lnegated;
                        matching_rows = lptrs;
                    } else {
                        if lnegated {
                            // only lefthandside negated, return (true, lptrs - rptrs)
                            for rptr in rptrs {
                                lptrs.remove(&rptr);
                            }
                            matching_rows = lptrs;
                        } else {
                            // only righthandside negated, return (left, all rptrs - lptrs)
                            for lptr in lptrs {
                                rptrs.remove(&lptr);
                            }
                            matching_rows = rptrs;
                        }
                        negated_res = true;
                    }
                }
                _ => {
                    // special case: use index to perform comparisons against 
                    // fixed value on the RHS
                    let mut fastpath = false;
                    if let Expr::Identifier(_) = **left {
                        if let Expr::Value(ref val) = **right {
                            debug!("getting rptrs of constraint: Fast path {:?}", e);
                            if *op == BinaryOperator::Eq || *op == BinaryOperator::NotEq {
                                fastpath = true;
                                let (_tab, col) = helpers::expr_to_col(&left);
                                if let Some(ci) = get_col_index(&col, columns) {
                                    v.get_rptrs_of_col(ci, &val.to_string(), &mut matching_rows);
                                    negated_res = *op == BinaryOperator::NotEq;
                                } 
                            }
                        }
                    } 
                    if !fastpath {
                        warn!("get_rptrs_matching_constraint: Slow path {:?}", e);
                        let left_fn = get_value_for_row_closure(&left, columns);
                        let right_fn = get_value_for_row_closure(&right, columns);

                        for (_pk, row) in v.rows.borrow().iter() {
                            let left_val = left_fn(&row.borrow());
                            let right_val = right_fn(&row.borrow());
                            let cmp = helpers::parser_vals_cmp(&left_val, &right_val);
                            match op {
                                BinaryOperator::Eq => {
                                    if cmp == Ordering::Equal {
                                        matching_rows.insert(HashedRowPtr::new(row.clone(), v.primary_index));
                                    }
                                }
                                BinaryOperator::NotEq => {
                                    if cmp != Ordering::Equal {
                                        matching_rows.insert(HashedRowPtr::new(row.clone(), v.primary_index));
                                    }
                                }
                                BinaryOperator::Lt => {
                                    if cmp == Ordering::Less {
                                        matching_rows.insert(HashedRowPtr::new(row.clone(), v.primary_index));
                                    }
                                }
                                BinaryOperator::Gt => {
                                    if cmp == Ordering::Greater {
                                        matching_rows.insert(HashedRowPtr::new(row.clone(), v.primary_index));
                                    }
                                }
                                BinaryOperator::LtEq => {
                                    if cmp != Ordering::Greater {
                                        matching_rows.insert(HashedRowPtr::new(row.clone(), v.primary_index));
                                    }
                                }
                                BinaryOperator::GtEq => {
                                    if cmp != Ordering::Less {
                                        matching_rows.insert(HashedRowPtr::new(row.clone(), v.primary_index));
                                    }
                                }
                                _ => unimplemented!("binop constraint not supported {:?}", e),
                            }
                        }
                        negated_res = false;
                    }
                }
            }
        }
        _ => unimplemented!("Constraint not supported {:?}", e),
    }
    let dur = start.elapsed();
    warn!("get rptrs matching constraint {} duration {}us", e, dur.as_micros());
    (negated_res, matching_rows)
}

