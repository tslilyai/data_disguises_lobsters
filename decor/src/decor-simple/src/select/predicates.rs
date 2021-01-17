use crate::views::{View, TableColumnDef, HashedRowPtrs, HashedRowPtr, Row, RowPtrs};
use crate::{helpers};
use log::{warn};
use std::collections::{HashSet};
use std::cmp::Ordering;
use std::time;
use sql_parser::ast::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamedPredicate {
    ColValEq {
        name: String, 
        val: Value,
        neg: bool,
    },

    ColValsEq {
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

pub enum IndexedPredicate {
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

impl std::fmt::Debug for IndexedPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        use IndexedPredicate::*;
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

impl NamedPredicate {
    pub fn to_indexed_predicate(&self, columns: &Vec<TableColumnDef>) -> Option<IndexedPredicate> {
        use NamedPredicate::*;
        match self {
            Bool(b) => Some(IndexedPredicate::Bool(*b)),
            ColValEq {name, val, neg} => {
                if let Some(ci) = helpers::get_col_index(&name, columns) {
                    Some(IndexedPredicate::ColValEq {
                        index: ci,
                        val: val.clone(),
                        neg: *neg,
                    })
                } else {
                    None
                }
            }
            ColValsEq {name, vals, neg} => {
                if let Some(ci) = helpers::get_col_index(&name, columns) {
                    Some(IndexedPredicate::ColValsEq {
                        index: ci,
                        vals: vals.clone(),
                        neg: *neg,
                    })
                } else {
                    None
                }
            } 
            ColCmp {name1, name2, val, op} => {
                let i2 = match name2 {
                    Some(n) => helpers::get_col_index(&n, columns),
                    None => None
                };
                if i2.is_none() && val.is_none() {
                    return None;
                }
                if let Some(ci) = helpers::get_col_index(&name1, columns) {
                    Some(IndexedPredicate::ColCmp{
                        index1: ci,
                        index2: i2, 
                        val: val.clone(),
                        op: op.clone(),
                    })
                } else {
                    None
                }
            } 
            ComputeValCmp {name1, name2, innerval, innerop, val, op} => {
                let i2 = match name2 {
                    Some(n) => helpers::get_col_index(&n, columns),
                    None => None,
                };
                if i2.is_none() && innerval.is_none() {
                    return None;
                }
                if let Some(i1) = helpers::get_col_index(name1, columns) {
                    let comp_func = get_compute_closure_for_row(i1, i2, innerval, innerop);
                    Some(IndexedPredicate::ComputeValCmp{
                        comp_func: comp_func,
                        val: val.clone(),
                        op: op.clone(),
                    })
                } else {
                    None
                }
            }
        }
    }
}



/*
 * Turn predicate into a value for row
 */
pub fn get_compute_closure_for_row(index1: usize, index2: Option<usize>, val: &Option<Value>, op: &BinaryOperator)
    -> Box<dyn Fn(&Vec<Value>) -> Value> 
{
    let closure: Option<Box<dyn Fn(&Vec<Value>) -> Value>>;
    let start = time::Instant::now();
    match *op {
        BinaryOperator::Plus => {
            if let Some(v) = val {
                let v = v.clone();
                closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[index1].clone(), &v)));
            } else {
                let i2 = index2.unwrap();
                closure = Some(Box::new(move |row| helpers::plus_parser_vals(&row[index1], &row[i2])));
            }
        }
        BinaryOperator::Minus => {
            if let Some(v) = val {
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

/* 
 * returns lists of predicates  
 */
pub fn get_predicates_of_constraint(e: &Expr, preds: &mut Vec<NamedPredicate>)
{
    let start = time::Instant::now();
    warn!("getting predicates of constraint {}", e);
    match e {
        Expr::Value(Value::Boolean(b)) => {
            preds.push(NamedPredicate::Bool(*b));
        } 
        Expr::InList { expr, list, negated } => {
            let list_vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (tab, mut col) = helpers::expr_to_col(&expr);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            preds.push(NamedPredicate::ColValsEq {
                name: col,
                vals: list_vals,
                neg: *negated,
            });
        }
        Expr::IsNull { expr, negated } => {
            let (tab, mut col) = helpers::expr_to_col(&expr);
            if !tab.is_empty() {
                col = format!("{}.{}", tab, col);
            }
            preds.push(NamedPredicate::ColValEq {
                name: col, 
                val: Value::Null,
                neg: *negated,
            });
        }
        Expr::BinaryOp {left, op, right} => {
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
                                let (tab, mut col) = helpers::expr_to_col(&left);
                                if !tab.is_empty() {
                                    col = format!("{}.{}", tab, col);
                                }
                                preds.push(NamedPredicate::ColValEq {
                                    name: col, 
                                    val: val.clone(),
                                    neg: *op != BinaryOperator::Eq,
                                });
                            }
                        }
                    }
                    if !fastpath {
                        let cmp_op = op.clone();
                        let (rname, rval) = helpers::rhs_expr_to_name_or_value(&right);
                        match &**left {
                            Expr::Identifier(_) =>  {
                                let lname = helpers::lhs_expr_to_name(&left);
                                preds.push(NamedPredicate::ColCmp{
                                    name1: lname, 
                                    name2: rname, 
                                    val: rval,
                                    op: cmp_op,
                                });
                            }
                            Expr::BinaryOp{left, op, right} => {
                                let innerlname = helpers::lhs_expr_to_name(&left);
                                let (innerrname, innerrval) = helpers::rhs_expr_to_name_or_value(&right);
                                preds.push(NamedPredicate::ComputeValCmp {
                                    name1: innerlname,
                                    name2: innerrname,
                                    innerval: innerrval,
                                    innerop: op.clone(),
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
    warn!("get predicates of constraint {} duration {}us: {:?}", e, dur.as_micros(), preds);
}

pub fn get_predicate_sets_of_constraint(e: &Expr) -> Vec<Vec<NamedPredicate>>
{
    warn!("getting rptrs of constraint {}", e);
    let start = time::Instant::now();
    let mut is_or = false;
    let mut pred_sets = Vec::new();
    match e {
        Expr::BinaryOp{left, op, right} => {
            match op {
                BinaryOperator::Or => {
                    // NOTE: this could inefficiently linearly scan twice. oh well...
                    pred_sets.append(&mut get_predicate_sets_of_constraint(&left));
                    pred_sets.append(&mut get_predicate_sets_of_constraint(&right));
                    is_or = true;
                }  
                _ => (),
            }
        }
        _ => (),
    } 
    if !is_or {
        let mut preds = vec![];
        get_predicates_of_constraint(&e, &mut preds);
        pred_sets.push(preds);
    }
    let dur = start.elapsed();
    warn!("get predicate sets of constraint {} duration {}us", e, dur.as_micros());
    pred_sets
}

/*
 * Returns matching rows and any predicates which have not yet been applied
 */
pub fn get_rptrs_matching_preds_vec(v: &View, columns: &Vec<TableColumnDef>, predsets: &Vec<Vec<NamedPredicate>>) 
    -> RowPtrs
{
    let rptrs = get_rptrs_matching_preds(v, columns, predsets); 
    rptrs.iter().map(|r| r.row().clone()).collect()
}

pub fn get_rptrs_matching_preds(v: &View, columns: &Vec<TableColumnDef>, predsets: &Vec<Vec<NamedPredicate>>) -> HashedRowPtrs 
{
    warn!("{}: getting rptrs of preds {:?}", v.name, predsets);
    assert!(!predsets.is_empty());

    let start = time::Instant::now();
    let mut matching = HashSet::new();
    for preds in predsets {
        let mut indexed_preds = vec![]; 
        for p in preds {
            indexed_preds.push(p.to_indexed_predicate(columns).unwrap());
        }
        // we failed to index
        if indexed_preds.is_empty() {
            continue;
        } else {
            matching.extend(get_predicated_rptrs(&indexed_preds, v));
        }
    }
    let dur = start.elapsed();
    warn!("get rptrs matching preds duration {}us", dur.as_micros());
    matching
}

pub fn get_applicable_and_failed_preds(v: &View, columns: &Vec<TableColumnDef>, predsets: &Vec<Vec<NamedPredicate>>) -> (Vec<Vec<NamedPredicate>>, Vec<Vec<NamedPredicate>>) 
{
    warn!("{}: getting rptrs of preds {:?}", v.name, predsets);
    assert!(!predsets.is_empty());

    let start = time::Instant::now();
    let mut applicable_predsets = vec![];
    let mut failed_predsets = vec![];
    for preds in predsets {
        let mut failed = vec![];
        let mut indexed_preds = vec![]; 
        for p in preds {
            if p.to_indexed_predicate(columns).is_some() {
                indexed_preds.push(p.clone());
            } else {
                failed.push(p.clone());
            }
        }
        if !(failed.is_empty()) {
            failed_predsets.push(failed);
        } else {
            applicable_predsets.push(indexed_preds);
        }
    }
    let dur = start.elapsed();
    warn!("get rptrs matching preds duration {}us", dur.as_micros());
    (applicable_predsets, failed_predsets)
}

/*
 * Invariant: preds is never empty
 */
pub fn get_predicated_rptrs(preds: &Vec<IndexedPredicate>, v: &View) -> HashedRowPtrs {
    use IndexedPredicate::*;
    
    let mut matching : Option<HashedRowPtrs> = None;
    let mut not_applied = vec![];
    assert!(!preds.is_empty());

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
    if not_applied.is_empty() {
        not_applied = vec![&IndexedPredicate::Bool(true)];
    }
    if let Some(mut matching) = matching {
        get_predicated_rptrs_from_matching(&not_applied, &mut matching);
        return matching;
    } else {
        // if we got to this point we have to linear scan and apply all predicates :\
        return get_predicated_rptrs_from_view(&not_applied, v);
    }
}

pub fn get_predicated_rptrs_from_view(preds: &Vec<&IndexedPredicate>, v: &View) -> HashedRowPtrs
{
    warn!("Applying predicates {:?} to all view rows", preds);
    assert!(!preds.is_empty());
    let mut matching_rptrs = HashSet::new();
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

pub fn get_predicated_rptrs_from_matching(preds: &Vec<&IndexedPredicate>, matching: &mut HashedRowPtrs) 
{
    warn!("Applying predicates {:?} to {} matching rows", preds, matching.len());
    assert!(!preds.is_empty());
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

pub fn pred_matches_row(row: &Row, p: &IndexedPredicate) -> bool {
    warn!("evaluating row {:?} with predicate {:?}", row, p);
    use IndexedPredicate::*;
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

