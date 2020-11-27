use crate::views::{View, TableColumnDef, HashedRowPtrs, HashedRowPtr, Row};
use crate::{helpers};
use log::{warn, debug};
use std::collections::{HashSet};
use std::cmp::Ordering;
use std::time;
use sql_parser::ast::*;

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
        comp_func: Box<dyn Fn(&Vec<Value>) -> Value>,
        val: Value, 
        op: BinaryOperator,
    },
    
    Bool(bool),
}
impl std::fmt::Debug for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        use Predicate::*;
        match self {
            Bool(b) => f.debug_struct("Bool").field("b", b).finish(),
            ColValEq{index, val, neg} => f.debug_struct("ColValEq")
                .field("index", index)
                .field("val", val)
                .field("neg", neg)
                .finish(),
            ColValsEq{index, vals, neg} => f.debug_struct("ColValsEq")
                .field("index", index)
                .field("vals", vals)
                .field("neg", neg)
                .finish(),
            ColCmp{index1, index2, val, op} => f.debug_struct("ColCmp")
                .field("index1", index1)
                .field("index2", index2)
                .field("val", val)
                .field("op", op)
                .finish(),
            ComputeValCmp{val, op, ..} => f.debug_struct("ComputeValCmp")
                .field("val", val)
                .field("op", op)
                .finish(),
        }
    }
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
pub fn get_compute_closure_for_row(index1: usize, index2: Option<usize>, val: Option<Value>, op: BinaryOperator)
    -> Box<dyn Fn(&Vec<Value>) -> Value> 
{
    let closure: Option<Box<dyn Fn(&Vec<Value>) -> Value>>;
    let start = time::Instant::now();
    match op {
        BinaryOperator::Plus => {
            if let Some(v) = &val {
                let v = v.clone();
                closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[index1].clone(), &v)));
            } else {
                let i2 = index2.unwrap();
                closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[index1], &row[i2])));
            }
        }
        BinaryOperator::Minus => {
            if let Some(v) = &val {
                let v = v.clone();
                closure = Some(Box::new(move |row| helpers::minus_parser_vals(&row[index1].clone(), &v)));
            } else {
                let i2 = index2.unwrap();
                closure = Some(Box::new(move |row| helpers::minus_parser_vals(&row[index1], &row[i2])));
            }
        }
        _ => unimplemented!("op {} not supported to get value", op),
    }
    let dur = start.elapsed();
    warn!("Get closure for expr {:?} took: {}us", op, dur.as_micros());
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

pub fn get_predicated_rptrs_from_view(preds: &Vec<&Predicate>, v: &View) -> HashedRowPtrs
{
    let mut matching_rptrs = HashSet::new();
    warn!("Applying predicates {:?} to all view rows", preds);
    'rowloop: for (_, rptr) in v.rows.borrow().iter() {
        let row = rptr.borrow();
        for p in preds {
            if !pred_matches_row(&row, p) {
                continue 'rowloop;
            }
        }
        matching_rptrs.insert(HashedRowPtr::new(rptr.clone(), v.primary_index));
    }
    matching_rptrs
}

pub fn get_predicated_rptrs_from_matching(preds: &Vec<&Predicate>, matching: &mut HashedRowPtrs) 
{
    warn!("Applying predicates {:?} to {} matching rows", preds, matching.len());
    matching.retain(|hrp| {
        let row = hrp.row().borrow();
        let mut matches = true;
        for p in preds {
            matches &= pred_matches_row(&row, p);
        }
        matches
    });
    warn!("Post-application len: {}", matching.len());
}

fn pred_matches_row(row: &Row, p: &Predicate) -> bool {
    use Predicate::*;
    match p {
        Bool(b) => *b,
        ColValEq{index, val, neg} => !neg == (helpers::parser_vals_cmp(&row[*index], &val) == Ordering::Equal),
        ColValsEq{index, vals, neg} => {
            for lv in vals {
                if !neg == (helpers::parser_vals_cmp(&row[*index], &lv) == Ordering::Equal) {
                    return true
                }
            }
            false
        }
        ColCmp{index1, index2, val, op} => {
            let left_val = &row[*index1];
            let right_val : &Value;
            if let Some(i2) = index2 {
                right_val = &row[*i2];
            } else {
                right_val = &val.as_ref().unwrap();
            }
            vals_satisfy_cmp(left_val, right_val, op)
        }
        ComputeValCmp{comp_func, val, op} => {
            let left_val = comp_func(&row);
            vals_satisfy_cmp(&left_val, val, op)
        }
    }
}

/* 
 * returns lists of predicates  
 */
pub fn get_predicates_of_constraint(e: &Expr, v: &View, columns: &Vec<TableColumnDef>, preds: &mut Vec<Predicate>)
{
    let start = time::Instant::now();
    debug!("getting predicates of constraint {}", e);
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
                                debug!("getting rptrs of constraint: Fast path {}", e);
                                fastpath = true;
                                let (_tab, col) = helpers::expr_to_col(&left);
                                if let Some(ci) = helpers::get_col_index(&col, columns) {
                                    preds.push(Predicate::ColValEq {
                                        index: ci, 
                                        val: val.clone(),
                                        neg: *op != BinaryOperator::Eq,
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
                                let comp_func = get_compute_closure_for_row(
                                    innerlindex,
                                    innerrindex,
                                    innerrval,
                                    op.clone(),
                                );
                                preds.push(Predicate::ComputeValCmp {
                                    comp_func: comp_func, 
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
    debug!("getting rptrs of constraint {}", e);
    let start = time::Instant::now();
    let mut is_or = false;
    let mut matching = HashSet::new();
    match e {
        Expr::BinaryOp{left, op, right} => {
            match op {
                BinaryOperator::Or => {
                    // NOTE: this could inefficiently linearly scan twice. oh well...
                    matching.extend(get_rptrs_matching_constraint(&left, v, columns));
                    matching.extend(get_rptrs_matching_constraint(&right, v, columns));
                    is_or = true;
                }  
                _ => (),
            }
        }
        _ => (),
    } 
    if !is_or {
        let mut preds : Vec<Predicate> = vec![];
        get_predicates_of_constraint(&e, v, columns, &mut preds);
        matching = get_predicated_rptrs(&preds, v);
    }
    let dur = start.elapsed();
    warn!("get rptrs matching constraint {} duration {}us", e, dur.as_micros());
    matching
}

pub fn get_predicated_rptrs(preds: &Vec<Predicate>, v: &View) -> HashedRowPtrs {
    use Predicate::*;

    let mut matching : Option<HashedRowPtrs> = None;
    let mut not_applied = vec![];

    // first try to narrow down by a single index select
    for pred in preds {
        if let ColValEq{index, val, neg} = pred {
            // we scan all pointers if it's negated anyway...
            // don't do more than one intiial select at first
            if *neg || matching.is_some() {
                not_applied.push(pred);
                continue;
            } 
            if let Some(hrptrs) = v.get_indexed_rptrs_of_col(*index, &val.to_string()) {
                matching = Some(hrptrs);
                continue;
            } 
        }
        not_applied.push(pred);
    }
    // next narrow down by InList select
    if matching.is_none() {
        not_applied.clear();
        for pred in preds {
            if let ColValsEq{index, vals, neg} = pred {
                if *neg || matching.is_some() {
                    not_applied.push(pred);
                    continue;
                } 
                if v.is_indexed_col(*index) {
                    let mut hrptrs = HashSet::new();
                    for lv in vals {
                        hrptrs.extend(v.get_indexed_rptrs_of_col(*index, &lv.to_string()).unwrap());
                    }
                    matching = Some(hrptrs);
                    continue;
                } 
            } 
            not_applied.push(pred);
        }
    }
    if let Some(mut matching) = matching {
        get_predicated_rptrs_from_matching(&not_applied, &mut matching);
        return matching;
    } else {
        // if we got to this point we have to linear scan and apply all predicates :\
        return get_predicated_rptrs_from_view(&not_applied, v);
    }
}
