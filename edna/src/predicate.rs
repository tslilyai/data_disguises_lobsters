use crate::helpers::*;
use crate::{RowVal, UID};
use crate::tokens::*;
use log::{debug, warn};
use sql_parser::ast::*;
use std::cmp::Ordering;
use std::str::FromStr;
use std::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredClause {
    ColInList {
        col: String,
        vals: Vec<String>,
        neg: bool,
    },

    ColColCmp {
        col1: String,
        col2: String,
        op: BinaryOperator,
    },

    ColValCmp {
        col: String,
        val: String,
        op: BinaryOperator,
    },

    Bool(bool),
}

impl ToString for PredClause {
    fn to_string(&self) -> String {
        use PredClause::*;
        match self {
            ColInList { col, vals, neg } => {
                let strvals : Vec<String> = vals.iter().map(|v| format!("\'{}\'", v)).collect();
                match neg {
                    true => format!("{} NOT IN ({})", col, strvals.join(",")),
                    false => format!("{} IN ({})", col, strvals.join(",")),
                }
            }

            ColColCmp { col1, col2, op } => {
                use BinaryOperator::*;
                match op {
                    Gt => format!("{} > {}", col1, col2),
                    Lt => format!("{} < {}", col1, col2),
                    GtEq => format!("{} >= {}", col1, col2),
                    LtEq => format!("{} <= {}", col1, col2),
                    Eq => format!("{} = {}", col1, col2),
                    NotEq => format!("{} != {}", col1, col2),
                    And => format!("{} AND {}", col1, col2),
                    Or => format!("{} OR {}", col1, col2),
                    _ => unimplemented!("No support for op {}", op),
                }
            }
            ColValCmp { col, val, op } => {
                use BinaryOperator::*;
                match op {
                    Gt => format!("{} > {}", col, val),
                    Lt => format!("{} < {}", col, val),
                    GtEq => format!("{} >= {}", col, val),
                    LtEq => format!("{} <= {}", col, val),
                    // escape strings
                    Eq => if val.chars().all(char::is_numeric) {
                        format!("{} = {}", col, val)
                    } else {
                        format!("{} = '{}'", col, val)
                    },
                    NotEq => if val.chars().all(char::is_numeric) {
                        format!("{} != {}", col, val)
                    } else {
                        format!("{} != '{}'", col, val)
                    },                    
                    And => format!("{} AND {}", col, val),
                    Or => format!("{} OR {}", col, val),
                    BitwiseAnd => format!("{} & {}", col, val),
                    _ => unimplemented!("No support for op {}", op),
                }
            }
            Bool(b) => b.to_string(),
        }
    }
}

pub fn pred_to_sql_where(pred: &Vec<Vec<PredClause>>) -> String {
    let mut ors = vec![];
    for and_clauses in pred {
        let mut ands = vec![];
        for clause in and_clauses {
            ands.push(clause.to_string());
        }
        ors.push(format!("({})", ands.join(" AND ")));
    }
    ors.join(" OR ")
}

pub fn modify_predicate_with_owners(
    pred: &Vec<Vec<PredClause>>,
    fk_col: &str,
    ots: &Vec<OwnershipTokenWrapper>,
) -> (Vec<Vec<PredClause>>, Option<UID>) {
    use PredClause::*;
    let mut new_pred = vec![];
    let mut changed = false;
    if ots.is_empty() {
        return (new_pred, None);
    }
    let old_uid = &ots[0].old_uid;
    let new_owners : Vec<String> = ots.iter().map(|ot| ot.new_uid.to_string()).collect();
    for and_clauses in pred {
        let mut new_and_clauses = vec![];
        // change clause to reference new user instead of old
        for clause in and_clauses {
            match clause {
                ColInList { col, vals, neg } => {
                    let mut found = false;
                    for val in vals {
                        if val == old_uid && col == &fk_col {
                            found = true;
                            break;
                        }
                    }
                    if found {
                        new_and_clauses.push(ColInList {
                            col: col.clone(),
                            vals: new_owners.clone(),
                            neg: *neg,
                        });
                        changed = true;
                        debug!("Modified pred val cmp to {:?}\n", new_and_clauses);
                    } else {
                        new_and_clauses.push(clause.clone())
                    }
                }
                ColColCmp { .. } => unimplemented!("No ownership comparison of cols"),
                ColValCmp { col, val, op } => {
                    if val == old_uid && col == &fk_col {
                        match op {
                        BinaryOperator::Eq => {
                            new_and_clauses.push(ColInList {
                                col: col.clone(),
                                vals: new_owners.clone(),
                                neg: false,
                            });
                        }, 
                        BinaryOperator::NotEq => {
                            new_and_clauses.push(ColInList {
                                col: col.clone(),
                                vals: new_owners.clone(),
                                neg: true,
                            });
                        },
                        _ => unimplemented!("No support for ops that aren't eq or neq for composition"),
                            /*new_and_clauses.push(ColValCmp {
                                col: col.clone(),
                                val: ownership_token.new_uid.to_string(),
                                op: op.clone(),
                            });*/
                        }
                        changed = true;
                        warn!("Modified pred val cmp to {:?}\n", new_and_clauses);
                    } else {
                        new_and_clauses.push(clause.clone())
                    }
                }
                Bool(_) => new_and_clauses.push(clause.clone()),
            }
        }
        new_pred.push(new_and_clauses);
    }
    debug!("Modified pred {:?} to {:?} with {} ots\n", pred, new_pred, ots.len());
    if changed {
        (new_pred, Some(old_uid.to_string()))
    } else {
        (new_pred, None)
    }
}

pub fn diff_token_matches_pred(pred: &Vec<Vec<PredClause>>, name: &str, t: &EdnaDiffToken) -> bool {
    if t.guise_name != name {
        return false;
    }
    if predicate_applies_to_val(pred, &t.col, &t.old_val) || predicate_applies_to_val(pred, &t.col, &t.new_val)
    {
        //debug!("Pred: OwnershipToken matched pred {:?}! Pushing matching to len {}\n", pred, matching.len());
        return true;
    }
    false
}

pub fn get_all_preds_with_owners(
    pred: &Vec<Vec<PredClause>>,
    fk_cols: &Vec<String>,
    own_tokens: &Vec<OwnershipTokenWrapper>,
) -> (Option<UID>, Vec<Vec<Vec<PredClause>>>) {
    let mut preds = vec![pred.clone()];
    let mut old_uid = None;
    for col in fk_cols {
        let (modified_pred, changed_from) = modify_predicate_with_owners(pred, col, own_tokens);
        if changed_from.is_none() {
            continue;
        }
        old_uid = changed_from;
        preds.push(modified_pred);
    }
    (old_uid, preds)
}

pub fn get_ownership_tokens_matching_pred(
    pred: &Vec<Vec<PredClause>>,
    fk_col: &str,
    tokens: &Vec<OwnershipTokenWrapper>,
) -> Vec<OwnershipTokenWrapper> {
    let mut matching = vec![];
    for t in tokens {
        if predicate_applies_with_col(pred, fk_col, &t.old_uid)
            || predicate_applies_with_col(pred, fk_col, &t.new_uid)
        {
            debug!("Pred: OwnershipToken matched pred {:?}! Pushing matching to len {}\n", pred, matching.len());
            matching.push(t.clone());
        }
    }
    matching
}

pub fn predicate_applies_with_col(p: &Vec<Vec<PredClause>>, col: &str, val: &str) -> bool {
    let mut found_true = false;
    for and_clauses in p {
        let mut all_true = true;
        for clause in and_clauses {
            if !clause_applies_to_col(clause, col, &val) {
                all_true = false;
                break;
            }
        }
        if all_true {
            found_true = true;
            break;
        }
    }
    debug!("Predicate {:?} applies with col {} and val {}? {}\n", p, col, val, found_true);
    found_true
}

pub fn clause_applies_to_col(p: &PredClause, c: &str, v: &str) -> bool {
    use PredClause::*;
    let matches = match p {
        ColInList { col, vals, neg } => {
            let found = col == c
                && vals
                    .iter()
                    .find(|v2| v2.to_string() == v)
                    .is_some();
            found != *neg
        }
        ColColCmp { .. } => unimplemented!("No ownership comparison of cols"),
        ColValCmp { col, val, op } => {
            if c == col {
                vals_satisfy_cmp(&v.to_string(), &val, &op)
            } else {
                false
            }
        }
        Bool(b) => *b,
    };
    //debug!("PredClause {:?} matches {:?}: {}\n", p, row, matches);
    matches
}

pub fn predicate_applies_to_row(p: &Vec<Vec<PredClause>>, row: &Vec<RowVal>) -> bool {
    let mut found_true = false;
    for and_clauses in p {
        let mut all_true = true;
        for clause in and_clauses {
            if !clause_applies_to_row(clause, row) {
                all_true = false;
                break;
            }
        }
        if all_true {
            found_true = true;
            break;
        }
    }
    debug!("Predicate {:?} applies to row {:?}? {}\n", p, row, found_true);
    found_true
}

pub fn predicate_applies_to_val(p: &Vec<Vec<PredClause>>, col: &str, val: &str) -> bool {
    let mut found_true = false;
    for and_clauses in p {
        let mut all_true = true;
        for clause in and_clauses {
            if !clause_applies_to_val(clause, col, val) {
                all_true = false;
                break;
            }
        }
        if all_true {
            found_true = true;
            break;
        }
    }
    debug!("Predicate {:?} applies to val {:?}? {}\n", p, val, found_true);
    found_true
}

pub fn clause_applies_to_val(p: &PredClause, column: &str, value: &str) -> bool {
    use PredClause::*;
    let matches = match p {
        ColInList { col, vals, neg } => {
            if col == column {
                vals.iter().find(|v| &v.to_string() == value).is_some() != *neg
            } else {
                false
            }
        }
        ColColCmp { .. } => {
            unimplemented!("oops");
        }
        ColValCmp { col, val, op } => {
            if col == column {
                vals_satisfy_cmp(value, &val, &op)
            } else {
                false
            }
        }
        Bool(b) => *b,
    };
    debug!("PredClause {:?} matches {:?}: {} {}\n", p, column, value, matches);
    matches
}

pub fn clause_applies_to_row(p: &PredClause, row: &Vec<RowVal>) -> bool {
    use PredClause::*;
    let matches = match p {
        ColInList { col, vals, neg } => {
            let found = match row.iter().find(|rv| &rv.column == col) {
                Some(rv) => vals.iter().find(|v| v.to_string() == rv.value).is_some(),
                None => false,
            };
            found != *neg
        }
        ColColCmp { col1, col2, op } => {
            let rv1: String;
            let rv2: String;
            match row.iter().find(|rv| &rv.column == col1) {
                Some(rv) => rv1 = rv.value.clone(),
                None => unimplemented!("bad predicate, no col1 {:?}", p),
            }
            match row.iter().find(|rv| &rv.column == col2) {
                Some(rv) => rv2 = rv.value.clone(),
                None => unimplemented!("bad predicate, no col2 {:?}", p),
            }
            vals_satisfy_cmp(&rv1, &rv2, &op)
        }
        ColValCmp { col, val, op } => {
            let rv1: String;
            match row.iter().find(|rv| &rv.column == col) {
                Some(rv) => rv1 = rv.value.clone(),
                None => {
                    debug!("Didn't find column {} in row {:?}", col, row);
                    return false; // this can happen if the row just isn't of the right guise type?
                }
            }
            let rv2 = val;
            vals_satisfy_cmp(&rv1, &rv2, &op)
        }
        Bool(b) => *b,
    };
    debug!("PredClause {:?} matches {:?}: {}\n", p, row, matches);
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
