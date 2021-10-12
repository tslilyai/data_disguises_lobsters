use crate::helpers::*;
use crate::tokens::*;
//use log::warn;
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
            ColInList { col, vals, neg } => match neg {
                true => format!("{} IN ({})", col, vals.join(",")),
                false => format!("{} NOT IN ({})", col, vals.join(",")),
            },

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
                    Eq => format!("{} = {}", col, val),
                    NotEq => format!("{} != {}", col, val),
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
        ors.push(format!("({})", ands.join(" AND")));
    }
    ors.join(" OR ")
}

pub fn diff_token_matches_pred(
    pred: &Vec<Vec<PredClause>>,
    name: &str,
    t: &DiffToken,
) -> bool {
    if t.guise_name != name {
        return false;
    }
    if predicate_applies_to_row(pred, &t.old_value)
        || predicate_applies_to_row(pred, &t.new_value)
    {
        //warn!("Pred: OwnershipToken matched pred {:?}! Pushing matching to len {}\n", pred, matching.len());
        return true;
    }
    false
}

pub fn get_ownership_tokens_matching_pred(
    pred: &Vec<Vec<PredClause>>,
    name: &str,
    tokens: &Vec<OwnershipToken>,
) -> Vec<OwnershipToken> {
    let mut matching = vec![];
    for t in tokens {
        //warn!(
        //   "Pred: get_tokens_matching_pred table {}, pred {:?}, checking token {:?}\n",
        //  name, pred, t
        //);
        if t.guise_name != name {
            continue;
        }
        if predicate_applies_with_col(pred, &t.fk_col, t.uid)
            || predicate_applies_with_col(pred, &t.fk_col, t.new_uid)
        {
            //warn!("Pred: OwnershipToken matched pred {:?}! Pushing matching to len {}\n", pred, matching.len());
            matching.push(t.clone());
        }
    }
    matching
}

pub fn predicate_applies_with_col(p: &Vec<Vec<PredClause>>, col: &str, val: u64) -> bool {
    let mut found_true = false;
    for and_clauses in p {
        let mut all_true = true;
        for clause in and_clauses {
            if !clause_applies_to_col(clause, col, val) {
                all_true = false;
                break;
            }
        }
        if all_true {
            found_true = true;
            break;
        }
    }
    //warn!("Predicate {:?} applies to row {:?}? {}\n", p, row, found_true);
    found_true
}

pub fn clause_applies_to_col(p: &PredClause, c: &str, v: u64) -> bool {
    use PredClause::*;
    let matches = match p {
        ColInList { col, vals, neg } => {
            let found = col == c
                && vals
                    .iter()
                    .find(|v| v.to_string() == v.to_string())
                    .is_some();
            found != *neg
        }
        ColColCmp { col1, col2, op } => unimplemented!("No ownership comparison of cols"),
        ColValCmp { col, val, op } => {
            let rv1: String;
            if c == col {
                vals_satisfy_cmp(&v.to_string(), &val, &op)
            } else {
                false
            }
        }
        Bool(b) => *b,
    };
    //warn!("PredClause {:?} matches {:?}: {}\n", p, row, matches);
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
    //warn!("Predicate {:?} applies to row {:?}? {}\n", p, row, found_true);
    found_true
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
                    //warn!("Didn't find column {} in row {:?}", col, row);
                    return false; // this can happen if the row just isn't of the right guise type?
                }
            }
            let rv2 = val;
            vals_satisfy_cmp(&rv1, &rv2, &op)
        }
        Bool(b) => *b,
    };
    //warn!("PredClause {:?} matches {:?}: {}\n", p, row, matches);
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
