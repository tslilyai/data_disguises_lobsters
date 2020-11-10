use sql_parser::ast::*;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use crate::views::{View, TableColumnDef};

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
            if let (Some(col1), Some(col2)) = (col1, col2) {
                if v1.name == tab1.to_string() && v2.name == tab2.to_string() {
                    i1 = v1.columns.iter().position(|c| c.column.name.to_string() == col1.to_string());
                    i2 = v2.columns.iter().position(|c| c.column.name.to_string() == col2.to_string());
                } else if v2.name == tab1.to_string() && v1.name == tab2.to_string() {
                    i1 = v1.columns.iter().position(|c| c.column.name.to_string() == col1.to_string());
                    i2 = v2.columns.iter().position(|c| c.column.name.to_string() == col2.to_string());
                } else {
                    return Err(err);
                }
                if i1 == None || i2 == None {
                    return Err(err);
                }
                return Ok((i1.unwrap(), i2.unwrap()));
            }
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
fn expr_to_col(e: &Expr) -> (Ident, Option<Ident>) {
    match e {
        // only support form table.[column | *]
        Expr::Identifier(ids) => {
            if ids.len() != 2 {
                unimplemented!("expr needs to be of form table.column {}", e);
            }
            return (ids[0].clone(), Some(ids[1].clone()));
        }
        Expr::QualifiedWildcard(ids) => {
            if ids.len() != 1 {
                unimplemented!("expr needs to be of form table.* {}", e);
            }
            return (ids[0].clone(), None);
        }
        _ => unimplemented!("projection {} not supported", e),
    }
}

fn get_setexpr_results(views: &HashMap<String, View>, se: &SetExpr) -> Result<View, Error> {
    match se {
        SetExpr::Select(s) => {
            let mut new_view = View::new(vec![]);
            if s.having != None {
                unimplemented!("No support for having queries");
            }

            let mut from_views = vec![];
            for twj in &s.from {
                let v = tablewithjoins_to_view(views, &twj)?;
                from_views.push(tablewithjoins_to_view(views, &twj)?);
            }

            // filter out rows by where clause
            if let Some(selection) = &s.selection {
                match selection {
                    Expr::InList { expr, list, negated } => {
                        let (tab, col) = expr_to_col(&expr);
                        for view in &from_views {
                            for row in &view.rows {
                            }
                        }
                    }
                    Expr::BinaryOp {left, op, right } => {

                    }
                    _ => unimplemented!("WHERE not supported: {}", selection)
                }
            } 
            
            // which columns to keep for which tables
            let cols_to_keep : HashMap<String, Vec<usize>> = HashMap::new();
            for proj in &s.projection {
                match proj {
                    SelectItem::Wildcard => {
                        for v in &from_views {
                            new_view.columns.append(&mut v.columns.clone());
                        }
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

