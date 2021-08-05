use crate::helpers::*;
use log::warn;
use sql_parser::ast::*;
use std::cmp::Ordering;
use std::str::FromStr;
use std::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Predicate {
    ColValEq {
        name: String,
        val: Value,
        neg: bool,
    },

    ColInList {
        name: String,
        vals: Vec<Value>,
        neg: bool,
    },

    ColCmp {
        name1: String,
        name2: Option<String>,
        val: Option<Value>,
        op: BinaryOperator,
    },

    ComputeValCmp {
        name1: String,
        name2: Option<String>,
        innerval: Option<Value>,
        innerop: BinaryOperator,
        val: Value,
        op: BinaryOperator,
    },

    Bool(bool),
}

pub fn predicate_applies_to_row(p: Predicate, row: &Vec<RowVal>) -> bool {
    use Predicate::*;
    let matches = match &p {
        ColValEq { name, val, neg } => {
            let found = match row.iter().find(|rv| &rv.column == name) {
                Some(rv) => rv.value == val.to_string(),
                None => false,
            };
            found != *neg
        }
        ColInList { name, vals, neg } => {
            let found = match row.iter().find(|rv| &rv.column == name) {
                Some(rv) => vals.iter().find(|v| v.to_string() == rv.value).is_some(),
                None => false,
            };
            found != *neg
        }
        ColCmp {
            name1,
            name2,
            val,
            op,
        } => {
            let rv1: String;
            let rv2: String;
            match row.iter().find(|rv| &rv.column == name1) {
                Some(rv) => rv1 = rv.value.clone(),
                None => unimplemented!("bad predicate, no name1 {:?}", p),
            }
            if let Some(name2) = name2 {
                match row.iter().find(|rv| &rv.column == name2) {
                    Some(rv) => rv2 = rv.value.clone(),
                    None => unimplemented!("bad predicate, no name2 {:?}", p),
                }
            } else if let Some(v) = val {
                rv2 = v.to_string();
            } else {
                unimplemented!("bad predicate, no rhs val {:?}", p);
            }
            vals_satisfy_cmp(&rv1, &rv2, &op)
        }
        ComputeValCmp {
            name1,
            name2,
            innerval,
            innerop,
            val,
            op,
        } => {
            let rv1: String;
            let rv2: String;
            match row.iter().find(|rv| &rv.column == name1) {
                Some(rv) => rv1 = rv.value.clone(),
                None => unimplemented!("bad predicate, no name1 {:?}", p),
            }
            if let Some(name2) = name2 {
                match row.iter().find(|rv| &rv.column == name2) {
                    Some(rv) => rv2 = rv.value.clone(),
                    None => unimplemented!("bad predicate, no name2 {:?}", p),
                }
            } else if let Some(v) = innerval {
                rv2 = v.to_string();
            } else {
                unimplemented!("bad predicate, no rhs val {:?}", p);
            }
            let left_val = compute_op(&rv1, &rv2, &innerop);
            vals_satisfy_cmp(&left_val, &val.to_string(), &op)
        }
        Bool(b) => *b,
    };
    warn!("Predicate {:?} matches {:?}: {}", p, row, matches);
    matches
}

pub fn compute_op(lval: &str, rval: &str, op: &BinaryOperator) -> String {
    let v1 = f64::from_str(lval).unwrap();
    let v2 = f64::from_str(rval).unwrap();
    match op {
        BinaryOperator::Plus => (v1 + v2).to_string(),
        BinaryOperator::Minus => (v1 - v2).to_string(),
        _ => unimplemented!("bad compute binop"),
    }
}

pub fn vals_satisfy_cmp(lval: &str, rval: &str, op: &BinaryOperator) -> bool {
    let cmp = string_vals_cmp(&lval, &rval);
    match op {
        BinaryOperator::Eq => cmp == Ordering::Equal,
        BinaryOperator::NotEq => cmp != Ordering::Equal,
        BinaryOperator::Lt => cmp == Ordering::Less,
        BinaryOperator::Gt => cmp == Ordering::Greater,
        BinaryOperator::LtEq => cmp != Ordering::Greater,
        BinaryOperator::GtEq => cmp != Ordering::Less,
        _ => unimplemented!("bad binop"),
    }
}

pub fn get_predicates_of_constraint(e: &Expr, preds: &mut Vec<Predicate>) {
    let start = time::Instant::now();
    warn!("getting predicates of constraint {}", e);
    match e {
        Expr::Value(Value::Boolean(b)) => {
            preds.push(Predicate::Bool(*b));
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list_vals: Vec<Value> = list
                .iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (tab, mut col) = expr_to_col(&expr);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            preds.push(Predicate::ColInList {
                name: col,
                vals: list_vals,
                neg: *negated,
            });
        }
        Expr::IsNull { expr, negated } => {
            let (tab, mut col) = expr_to_col(&expr);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            preds.push(Predicate::ColValEq {
                name: col,
                val: Value::Null,
                neg: *negated,
            });
        }
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And => {
                    get_predicates_of_constraint(left, preds);
                    get_predicates_of_constraint(right, preds);
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
                                warn!("getting rptrs of constraint: Fast path {}", e);
                                fastpath = true;
                                let (tab, mut col) = expr_to_col(&left);
                                if !tab.is_empty() {
                                    col = format!("{}.{}", tab, col);
                                }
                                preds.push(Predicate::ColValEq {
                                    name: col,
                                    val: val.clone(),
                                    neg: *op != BinaryOperator::Eq,
                                });
                            }
                        }
                    }
                    if !fastpath {
                        let cmp_op = op.clone();
                        let (rname, rval) = rhs_expr_to_name_or_value(&right);
                        match &**left {
                            Expr::Identifier(_) => {
                                let lname = lhs_expr_to_name(&left);
                                preds.push(Predicate::ColCmp {
                                    name1: lname,
                                    name2: rname,
                                    val: rval,
                                    op: cmp_op,
                                });
                            }
                            Expr::BinaryOp { left, op, right } => {
                                let innerlname = lhs_expr_to_name(&left);
                                let (innerrname, innerrval) = rhs_expr_to_name_or_value(&right);
                                preds.push(Predicate::ComputeValCmp {
                                    name1: innerlname,
                                    name2: innerrname,
                                    innerval: innerrval,
                                    innerop: op.clone(),
                                    val: rval.unwrap().clone(),
                                    op: cmp_op,
                                });
                            }
                            _ => unimplemented!("Bad lhs? {}", left),
                        }
                    }
                }
            }
        }
        _ => unimplemented!("Constraint not supported {:?}", e),
    }
    let dur = start.elapsed();
    warn!(
        "get predicates of constraint {} duration {}us: {:?}",
        e,
        dur.as_micros(),
        preds
    );
}
