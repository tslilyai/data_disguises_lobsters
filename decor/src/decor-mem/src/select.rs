use crate::views::{View, TableColumnDef};
use crate::helpers;
use std::collections::{HashMap, hash_set::HashSet};
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use sql_parser::ast::*;

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
    let mut new_view = View::new_with_cols(new_cols);
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
        // only support form column or table.column
        Expr::Identifier(ids) => {
            if ids.len() > 2 || ids.len() < 1 {
                unimplemented!("expr needs to be of form table.column {}", e);
            }
            if ids.len() == 2 {
                return (ids[0].to_string(), format!("{}.{}", ids[0], ids[1]));
            }
            return ("".to_string(), ids[0].to_string());
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

/* 
 * returns the rows matching the WHERE clause
 * and the indices into the view rows where these rows reside
 * 
 * */
pub fn get_rows_matching_constraint(e: &Expr, v: &View) -> (Vec<Vec<Value>>, HashSet<usize>) {
    let mut new_rows = vec![];
    let mut row_indices = HashSet::new();
    match e {
        Expr::InList { expr, list, negated } => {
            let (_tab, col) = expr_to_col(&expr);
            let vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let coli = v.columns.iter().position(|c| c.name() == col).unwrap();
            for (i, row) in v.rows.iter().enumerate() {
                if (!*negated && vals.iter().any(|v| *v == row[coli])) 
                    || (*negated && vals.iter().any(|v| *v == row[coli])) 
                {
                    new_rows.push(row.clone());
                    row_indices.insert(i);
                }
            }
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = expr_to_col(&expr);
            let coli = v.columns.iter().position(|c| c.name() == col).unwrap();
            for (i, row) in v.rows.iter().enumerate() {
                if (*negated && row[coli] != Value::Null) || (!*negated && row[coli] == Value::Null) {
                   new_rows.push(row.clone());
                   row_indices.insert(i);
                }
            }
        }
        Expr::BinaryOp {left, op, right} => {
            // TODO can split up into two fxns, one to get rows, other to get indices...
            match op {
                BinaryOperator::And => {
                    let (_lrows, lindices) = get_rows_matching_constraint(left, v);
                    let (_rrows, rindices) = get_rows_matching_constraint(right, v);
                    for i in lindices.intersection(&rindices) {
                        new_rows.push(v.rows[*i as usize].clone());
                        row_indices.insert(*i as usize);
                    }
                }
                BinaryOperator::Or => {
                    let (_lrows, lindices) = get_rows_matching_constraint(left, v);
                    let (_rrows, rindices) = get_rows_matching_constraint(right, v);
                    for i in lindices.union(&rindices) {
                        new_rows.push(v.rows[*i as usize].clone());
                        row_indices.insert(*i as usize);
                    }                
                }
                _ => {
                    let left_vals = get_value_for_rows(&left, v);
                    let right_vals = get_value_for_rows(&right, v);
                    for (i, row) in v.rows.iter().enumerate() {
                        match op {
                            BinaryOperator::Eq => {
                                if left_vals[i] == right_vals[i] {
                                    new_rows.push(row.clone());
                                    row_indices.insert(i);
                                }
                            }
                            BinaryOperator::NotEq => {
                                if left_vals[i] != right_vals[i] {
                                    new_rows.push(row.clone());
                                    row_indices.insert(i);
                                }
                            }
                            BinaryOperator::Lt => {
                                if left_vals[i] < right_vals[i] {
                                    new_rows.push(row.clone());
                                    row_indices.insert(i);
                                }
                            }
                            BinaryOperator::Gt => {
                                if left_vals[i] > right_vals[i] {
                                    new_rows.push(row.clone());
                                    row_indices.insert(i);
                                }
                            }
                            BinaryOperator::LtEq => {
                                if left_vals[i] <= right_vals[i] {
                                    new_rows.push(row.clone());
                                    row_indices.insert(i);
                                }
                            }
                            BinaryOperator::GtEq => {
                                if left_vals[i] >= right_vals[i] {
                                    new_rows.push(row.clone());
                                    row_indices.insert(i);
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
    (new_rows, row_indices)
}

fn get_setexpr_results(views: &HashMap<String, View>, se: &SetExpr) -> Result<View, Error> {
    match se {
        SetExpr::Select(s) => {
            let mut new_view = View::new_with_cols(vec![]);
            if s.having != None {
                unimplemented!("No support for having queries");
            }

            for twj in &s.from {
                let mut v = tablewithjoins_to_view(views, &twj)?;
                new_view.columns.append(&mut v.columns);
                new_view.rows.append(&mut v.rows);
            }
            
            // take account columns to keep for which tables
            // also alter aliases here prior to where clause filtering
            let mut cols_to_keep = vec![];
            for proj in &s.projection {
                match proj {
                    SelectItem::Wildcard => {
                        // only support wildcards if there are no other projections...
                        assert!(s.projection.len() == 1);
                    },
                    // TODO note this doesn't allow for `tablename.*` selections
                    SelectItem::Expr {expr, alias} => {
                        let (_tab, col) = expr_to_col(expr);
                        let index = new_view.columns.iter().position(|c| c.name() == col).unwrap();
                        cols_to_keep.push(index);
                        // alias; use in WHERE will match against this alias
                        if let Some(a) = alias {
                            new_view.columns[index].column.name = a.clone();
                            new_view.columns[index].table = String::new();
                        }
                    }
                }
            }

            // filter out rows by where clause
            if let Some(selection) = &s.selection {
                new_view.rows = get_rows_matching_constraint(&selection, &new_view).0;
            } 

            // reduce view to only return selected columns
            if !cols_to_keep.is_empty() {
                let mut new_cols = vec![];
                let mut new_rows = vec![vec![]; new_view.rows.len()];
                for ci in cols_to_keep {
                    new_cols.push(new_view.columns[ci].clone());
                    for (i, row) in new_view.rows.iter().enumerate() {
                        new_rows[i].push(row[ci].clone());
                    }
                }
                new_view.columns = new_cols;
                new_view.rows = new_rows;
            }

            Ok(new_view)
        }
        SetExpr::Query(q) => {
            return get_query_results(views, &q);
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            ..
        } => {
            let left_view = get_setexpr_results(views, &left)?;
            let right_view = get_setexpr_results(views, &right)?;
            let mut view = left_view.clone();
            match op {
                // TODO primary keys / unique keys 
                SetOperator::Union => {
                    // TODO currently allowing for duplicates regardless of ALL...
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
        SetExpr::Values(_vals) => {
            unimplemented!("Shouldn't be getting values when looking up results: {}", se); 
        }
    }
}

pub fn get_query_results(views: &HashMap<String, View>, q: &Query) -> Result<View, Error> {
    let mut new_view = get_setexpr_results(views, &q.body)?;

    // don't support OFFSET or fetches yet
    assert!(q.offset.is_none() && q.fetch.is_none());

    // order_by
    if q.order_by.len() > 0 {
        // only support one order by constraint for now
        assert!(q.order_by.len() < 2);
        let orderby = &q.order_by[0];
        let (_tab, col) = expr_to_col(&orderby.expr);
        let ci = new_view.columns.iter().position(|c| c.name() == col).unwrap();
        match orderby.asc {
            Some(false) => {
                new_view.rows.sort_by(|r1, r2| helpers::parser_val_cmp(&r2[ci], &r1[ci]));
            }
            Some(true) | None => {
                new_view.rows.sort_by(|r1, r2| helpers::parser_val_cmp(&r1[ci], &r2[ci]));
            }
        }
    }

    // limit
    if q.limit.is_some() {
        if let Some(Expr::Value(Value::Number(n))) = &q.limit {
            let limit = usize::from_str(n).unwrap();
            new_view.rows.truncate(limit);
        } else {
            unimplemented!("bad limit! {}", q);
        }
    }

    Ok(new_view)
}
