use sql_parser::ast::*;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use crate::views::{View, TableColumnDef};
use crate::helpers;

/*
 * Convert table name (with optional alias) to current view
 */
fn tablefactor_to_view(views: &HashMap<String, View>, tf: &TableFactor) -> Result<View, Error> {
    match tf {
        TableFactor::Table {
            name,
            alias,
        } => {
            let tab = views.get(&name.to_string());
            match tab {
                None => Err(Error::new(ErrorKind::Other, format!("table {:?} does not exist", tf))),
                Some(t) => {
                    let mut view = t.clone();
                    if let Some(a) = alias {
                        // only alias table name..
                        assert!(a.columns.is_empty());
                        view.name = a.name.to_string();
                    }
                    Ok(view)
                }
            }
        }
        _ => unimplemented!("no derived joins {:?}", tf),
    }
}

/*
 * Only handle join constraints of form "table.col = table'.col'"
 */
fn get_join_on_col_indices(e: &Expr, v1: &View, v2: &View) -> Result<(usize, usize), Error> {
    let err = Error::new(ErrorKind::Other, format!("joins constraint not supported: {:?}", e));
    let i1: Option<usize>; 
    let i2 : Option<usize>;
    if let Expr::BinaryOp {left, op, right} = e {
        if let BinaryOperator::Eq = op {
            let (tab1, col1) = expr_to_col(left);
            let (tab2, col2) = expr_to_col(right);
            if v1.name == tab1 && v2.name == tab2 {
                i1 = v1.columns.iter().position(|c| c.name() == col1);
                i2 = v2.columns.iter().position(|c| c.name() == col2);
            } else if v2.name == tab1.to_string() && v1.name == tab2.to_string() {
                i1 = v1.columns.iter().position(|c| c.name() == col1);
                i2 = v2.columns.iter().position(|c| c.name() == col2);
            } else {
                return Err(err);
            }
            if i1 == None || i2 == None {
                return Err(err);
            }
            return Ok((i1.unwrap(), i2.unwrap()));
        }
    }
    unimplemented!("join_on {}", e)
}

fn join_views(jo: &JoinOperator, v1: &View, v2: &View) -> Result<View, Error> {
    let mut new_cols : Vec<TableColumnDef> = v1.columns.clone();
    new_cols.append(&mut v2.columns.clone());
    let mut new_view = View::new(new_cols);
    match jo {
        JoinOperator::Inner(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            // this seems very very inefficient
            for row1 in &v1.rows {
                new_view.rows.append(&mut v2.get_rows_of_col(i2, &row1[i1]));
            }
        }
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            for row1 in &v1.rows {
                let mut found = false;
                let mut rows2 = v2.get_rows_of_col(i2, &row1[i1]);
                if !rows2.is_empty() {
                    new_view.rows.append(&mut rows2);
                    found = true;
                }
                if !found {
                    let mut new_row = row1.clone();
                    new_row.append(&mut vec![Value::Null; v2.columns.len()]);
                    new_view.rows.push(new_row);
                }
            }
        }
        JoinOperator::RightOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            for row2 in &v2.rows {
                let mut found = false;
                let mut rows1 = v1.get_rows_of_col(i1, &row2[i2]);
                if !rows1.is_empty() {
                    new_view.rows.append(&mut rows1);
                    found = true;
                }
                if !found {
                    let mut new_row = vec![Value::Null; v1.columns.len()];
                    new_row.append(&mut row2.clone());
                    new_view.rows.push(new_row);
                }
            }            
        }
        JoinOperator::FullOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            for row1 in &v1.rows {
                let mut found = false;
                let mut rows2 = v2.get_rows_of_col(i2, &row1[i1]);
                if !rows2.is_empty() {
                    new_view.rows.append(&mut rows2);
                    found = true;
                } 
                if !found {
                    let mut new_row = row1.clone();
                    new_row.append(&mut vec![Value::Null; v2.columns.len()]);
                    new_view.rows.push(new_row);
                }
            }
            // only add null rows for rows that weren't matched
            for row2 in &v2.rows {
                let mut found = false;
                let rows1 = v1.get_rows_of_col(i1, &row2[i2]);
                if !rows1.is_empty() {
                    found = true;
                } 
                if !found {
                    let mut new_row = vec![Value::Null; v1.columns.len()];
                    new_row.append(&mut row2.clone());
                    new_view.rows.push(new_row);
                }
            }            
        }
        _ => unimplemented!("No support for join type {:?}", jo),
    }
    Ok(new_view)
}

fn tablewithjoins_to_view(views: &HashMap<String, View>, twj: &TableWithJoins) -> Result<View, Error> {
    let mut view1 = tablefactor_to_view(views, &twj.relation)?;
    for j in &twj.joins {
        let view2 = tablefactor_to_view(views, &j.relation)?;
        view1 = join_views(&j.join_operator, &view1, &view2)?;
    }
    Ok(view1)
}

// return table name and optionally column if not wildcard
fn expr_to_col(e: &Expr) -> (String, String) {
    match e {
        // only support form table.[column | *]
        Expr::Identifier(ids) => {
            if ids.len() != 2 {
                unimplemented!("expr needs to be of form table.column {}", e);
            }
            return (ids[0].to_string(), format!("{}.{}", ids[0], ids[1]));
        }
        _ => unimplemented!("expr_to_col {} not supported", e),
    }
}

fn get_value_for_rows(e: &Expr, v: &View) -> Vec<f64> {
    let mut res = vec![];
    match e {
        Expr::Identifier(_) => {
            let (_tab, col) = expr_to_col(&e);
            let index = v.columns.iter().position(|c| c.name() == col).unwrap();
            for row in &v.rows {
                res.push(helpers::parser_val_to_f64(&row[index]));
            }
        }
        Expr::Value(val) => {
            res.push(helpers::parser_val_to_f64(&val));
        }
        Expr::BinaryOp{left, op, right} => {
            let mut lindex : Option<usize> = None;
            let mut rindex : Option<usize> = None;
            let mut lval : f64 = 0.0;
            let mut rval : f64 = 0.0;
            match &**left {
                Expr::Identifier(_) => {
                    let (_ltab, lcol) = expr_to_col(&left);
                    lindex = Some(v.columns.iter().position(|c| c.name() == lcol).unwrap());
                }
                Expr::Value(val) => {
                    lval = helpers::parser_val_to_f64(&val);
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            match &**right {
                Expr::Identifier(_) => {
                    let (_rtab, rcol) = expr_to_col(&right);
                    rindex = Some(v.columns.iter().position(|c| c.name() == rcol).unwrap());
                }
                Expr::Value(val) => {
                    rval = helpers::parser_val_to_f64(&val);
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            for r in &v.rows {
                if let Some(li) = lindex {
                    lval = helpers::parser_val_to_f64(&r[li]);
                }
                if let Some(ri) = rindex {
                    rval = helpers::parser_val_to_f64(&r[ri]);
                }
                match op {
                    BinaryOperator::Plus => {
                        res.push(lval + rval);
                    }
                    BinaryOperator::Minus => {
                        res.push(lval + rval);
                    }
                    _ => unimplemented!("op {} not supported to get value", op),
                }
            }
        }
        _ => unimplemented!("get value not supported {}", e),
    }
    res
}

/* returns pairs of columns and the value(s) that the column must take on */
fn get_rows_matching_constraint(e: &Expr, v: &mut View) -> Vec<Vec<Value>> {
    let mut new_rows = vec![];
    match e {
        Expr::InList { expr, list, negated } => {
            let (_tab, col) = expr_to_col(&expr);
            let vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let i = v.columns.iter().position(|c| c.name() == col).unwrap();
            for row in &v.rows {
                if (!*negated && vals.iter().any(|v| *v == row[i])) 
                    || (*negated && vals.iter().any(|v| *v == row[i])) 
                {
                    new_rows.push(row.clone());
                }
            }
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = expr_to_col(&expr);
            let i = v.columns.iter().position(|c| c.name() == col).unwrap();
            for row in &v.rows {
                if (*negated && row[i] != Value::Null) || (!*negated && row[i] == Value::Null) {
                   new_rows.push(row.clone());
                }
            }
        }
        Expr::BinaryOp {left, op, right} => {
            match op {
                BinaryOperator::And => {
                    v.rows = get_rows_matching_constraint(left, v);
                    new_rows = get_rows_matching_constraint(right, v);
                }
                BinaryOperator::Or => {
                    new_rows = get_rows_matching_constraint(left, v);
                    new_rows.append(&mut get_rows_matching_constraint(right, v));
                }
                _ => {
                    let left_vals = get_value_for_rows(&left, v);
                    let right_vals = get_value_for_rows(&right, v);
                    for (i, row) in v.rows.iter().enumerate() {
                        match op {
                            BinaryOperator::Eq => {
                                if left_vals[i] == right_vals[i] {
                                    new_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::NotEq => {
                                if left_vals[i] != right_vals[i] {
                                    new_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::Lt => {
                                if left_vals[i] < right_vals[i] {
                                    new_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::Gt => {
                                if left_vals[i] > right_vals[i] {
                                    new_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::LtEq => {
                                if left_vals[i] <= right_vals[i] {
                                    new_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::GtEq => {
                                if left_vals[i] >= right_vals[i] {
                                    new_rows.push(row.clone());
                                }
                            }
                            _ => unimplemented!("Constraint not supported {}", e),
                        }
                    }
                }
            }
        }
        _ => unimplemented!("Constraint not supported {}", e),
    }
    new_rows
}

fn get_setexpr_results(views: &HashMap<String, View>, se: &SetExpr) -> Result<View, Error> {
    match se {
        SetExpr::Select(s) => {
            let mut new_view = View::new(vec![]);
            if s.having != None {
                unimplemented!("No support for having queries");
            }

            for twj in &s.from {
                let mut v = tablewithjoins_to_view(views, &twj)?;
                new_view.columns.append(&mut v.columns);
                new_view.rows.append(&mut v.rows);
            }

            // filter out rows by where clause
            if let Some(selection) = &s.selection {
                new_view.rows = get_rows_matching_constraint(&selection, &mut new_view);
            } 
            
            // which columns to keep for which tables
            let cols_to_keep : HashMap<String, Vec<usize>> = HashMap::new();
            for proj in &s.projection {
                match proj {
                    SelectItem::Wildcard => {
                        // only support wildcards if there are no other projections...
                        assert!(s.projection.len() == 1);
                    },
                    SelectItem::Expr {expr, alias} => {
                        let (tab, col) = expr_to_col(expr);
                    }
                }
            }
            Ok(new_view)
        }
        SetExpr::Query(q) => {
            return get_query_results(views, &q);
        }
        SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } => {
            let left_view = get_setexpr_results(views, &left)?;
            let right_view = get_setexpr_results(views, &right)?;
            let mut view = left_view.clone();
            match op {
                // TODO primary keys / unique keys 
                SetOperator::Union => {
                    view.rows.append(&mut right_view.rows.clone());
                    return Ok(view);
                }
                SetOperator::Except => {
                    let mut view = left_view.clone();
                    view.rows.retain(|r| !right_view.contains_row(&r));
                    return Ok(view);
                },
                SetOperator::Intersect => {
                    let mut view = left_view.clone();
                    view.rows.retain(|r| right_view.contains_row(&r));
                    return Ok(view);
                }
            }
        }
        SetExpr::Values(vals) => {
            unimplemented!("Shouldn't be getting values when looking up results: {}", se); 
        }
    }
}

pub fn get_query_results(views: &HashMap<String, View>, q: &Query) -> Result<View, Error> {
    get_setexpr_results(views, &q.body)
}

