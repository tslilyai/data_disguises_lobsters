use crate::views::{View, TableColumnDef, ViewIndex};
use crate::types::{HashedRowPtrs};
use crate::{helpers, select::predicates};
use log::{warn, debug};
use std::collections::{HashMap};
use std::cmp::Ordering;
use std::time;
use sql_parser::ast::*;
use std::cell::RefCell;
use std::rc::Rc;

/*
 * Convert table name (with optional alias) to current view
 */
fn tablefactor_to_view(views: &HashMap<String, Rc<RefCell<View>>>, tf: &TableFactor) -> Rc<RefCell<View>> {
    match tf {
        TableFactor::Table {
            name,
            alias,
        } => {
            let tab = views.get(&name.to_string());
            match tab {
                None => unimplemented!("table {:?} does not exist", tf),
                Some(t) => {
                    if alias.is_some() {
                        unimplemented!("No aliasing of tables for now {}", tf);
                    }
                    /*if let Some(a) = alias {
                        // alias column table names too?
                        assert!(a.columns.is_empty());
                        view.name = a.name.to_string();
                    }*/
                    t.clone()
                }
            }
        }
        _ => unimplemented!("no derived joins {:?}", tf),
    }
}
fn get_binop_indices(e: &Expr, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) 
    -> (usize, usize)
{
    let i1: Option<usize>;
    let i2: Option<usize>; 
    if let Expr::BinaryOp{left, op, right} = e {
        if let BinaryOperator::Eq = op {
            let (tab1, col1) = helpers::expr_to_col(left);
            let (tab2, col2) = helpers::expr_to_col(right);
            debug!("Join got tables and columns {}.{} and {}.{} from expr {:?}", tab1, col1, tab2, col2, e);
            
            let v1 = v1.borrow();
            let v2 = v2.borrow();

            // note: view 1 may not have name attached any more because it was from a prior join.
            // the names are embedded in the columns of the view, so we should compare the entire
            // name of the new column/table
            if v2.name == tab2 {
                i1 = helpers::get_col_index(&format!("{}.{}", tab1, col1), &v1.columns);
                i2 = helpers::get_col_index(&format!("{}.{}", tab2, col2), &v2.columns);
            } else if v2.name == tab1 {
                i1 = helpers::get_col_index(&format!("{}.{}", tab2, col2), &v1.columns);
                i2 = helpers::get_col_index(&format!("{}.{}", tab1, col1), &v2.columns);
            } else {
                unimplemented!("Join: no matching tables for {}/{} and {}/{}", v1.name, tab1, v2.name, tab2);
            }
            if i1.is_none() || i2.is_none() {
                unimplemented!("No index for columns found! {:?}", e);
            }
            return (i1.unwrap(), i2.unwrap());
        }
    }
    unimplemented!("Join: unsupported join operation {:?}", e);
}

fn get_binop_indexes(e: &Expr, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) 
    -> (ViewIndex, ViewIndex)
{
    let i1: Option<ViewIndex>;
    let i2: Option<ViewIndex>; 
    if let Expr::BinaryOp{left, op, right} = e {
        if let BinaryOperator::Eq = op {
            let (tab1, col1) = helpers::expr_to_col(left);
            let (tab2, col2) = helpers::expr_to_col(right);
            debug!("Join got tables and columns {}.{} and {}.{} from expr {:?}", tab1, col1, tab2, col2, e);
            
            let v1 = v1.borrow();
            let v2 = v2.borrow();

            // note: view 1 may not have name attached any more because it was from a prior join.
            // the names are embedded in the columns of the view, so we should compare the entire
            // name of the new column/table
            if v2.name == tab2 {
                i1 = v1.get_index_of_view(&col1);
                i2 = v2.get_index_of_view(&col2);
            } else if v2.name == tab1 {
                i1 = v1.get_index_of_view(&col2);
                i2 = v2.get_index_of_view(&col1);
            } else {
                unimplemented!("Join: no matching tables for {}/{} and {}/{}", v1.name, tab1, v2.name, tab2);
            }
            if i1.is_none() || i2.is_none() {
                unimplemented!("No index for columns found! {:?}", e);
            }
            return (i1.unwrap(), i2.unwrap());
        }
    }
    unimplemented!("Join: unsupported join operation {:?}", e);
}

/*
 * Get indexes for views for a join expression `WHERE table.col = table.col`
 */
fn get_join_on_expr_indices(e: &Expr, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> (usize, usize) {
    let is: (usize, usize);
    if let Expr::Nested(binexpr) = e {
        is = get_binop_indices(binexpr, v1.clone(), v2.clone());
    } else {
        is = get_binop_indices(e, v1.clone(), v2.clone());
    }
    is
}

fn get_join_on_indices(jo: &JoinOperator, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> (usize, usize) {
    match jo {
        JoinOperator::Inner(JoinConstraint::On(e)) => get_join_on_expr_indices(e, v1, v2),
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => get_join_on_expr_indices(e, v1, v2),
        JoinOperator::RightOuter(JoinConstraint::On(e)) => get_join_on_expr_indices(e, v1, v2),
        _ => unimplemented!("bad join {:?}", jo),
    }
}

fn get_join_on_expr_indexes(e: &Expr, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> (ViewIndex, ViewIndex) {
    let is: (ViewIndex, ViewIndex);
    if let Expr::Nested(binexpr) = e {
        is = get_binop_indexes(binexpr, v1.clone(), v2.clone());
    } else {
        is = get_binop_indexes(e, v1.clone(), v2.clone());
    }
    is
}

fn get_join_on_indexes(jo: &JoinOperator, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> (ViewIndex, ViewIndex) {
    match jo {
        JoinOperator::Inner(JoinConstraint::On(e)) => get_join_on_expr_indexes(e, v1, v2),
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => get_join_on_expr_indexes(e, v1, v2),
        JoinOperator::RightOuter(JoinConstraint::On(e)) => get_join_on_expr_indexes(e, v1, v2),
        _ => unimplemented!("bad join {:?}", jo),
    }
}

fn set_primary_index_of_join(jo: &JoinOperator, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>, new_view: &mut View) {
    match jo {
        JoinOperator::Inner(JoinConstraint::On(_e)) => {
            new_view.primary_index = v1.borrow().primary_index;
        }
        JoinOperator::LeftOuter(JoinConstraint::On(_e)) => {
            new_view.primary_index = v1.borrow().primary_index;
        }
        JoinOperator::RightOuter(JoinConstraint::On(_e)) => {
            new_view.primary_index = v2.borrow().primary_index;
        }
        _ => unimplemented!("No support for join type {:?}", jo),
    }
}

fn join_using_indexes(jo: &JoinOperator, i1: &ViewIndex, i2: &ViewIndex, r1len: usize, r2len: usize, new_view: &mut View) 
{
    match jo {
        JoinOperator::Inner(JoinConstraint::On(_e)) => {
            match i1 {
                ViewIndex::Primary(ref i1, _pki) => {
                    for (id1, row1) in i1.borrow().iter() {
                        if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                            for row2 in rows2 {
                                let mut new_row = row1.borrow().clone();
                                let mut row2 = row2.row().borrow().clone();
                                new_row.truncate(r1len);
                                row2.truncate(r2len);
                                new_row.append(&mut row2);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        }
                    }
                }
                ViewIndex::Secondary(ref i1) => {
                    for (id1, rows1) in i1.borrow().iter() {
                        for row1 in rows1 {
                            if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                                for row2 in rows2 {
                                    let mut new_row = row1.row().borrow().clone();
                                    let mut row2 = row2.row().borrow().clone();
                                    new_row.truncate(r1len);
                                    row2.truncate(r2len);
                                    new_row.append(&mut row2);
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            }
                        }
                    }
                }
            }
        }
        JoinOperator::LeftOuter(JoinConstraint::On(_e)) => {
            match i1 {
                ViewIndex::Primary(ref i1, _pki) => {
                    for (id1, row1) in i1.borrow().iter() {
                        if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                            for row2 in rows2 {
                                let mut new_row = row1.borrow().clone();
                                let mut row2 = row2.row().borrow().clone();
                                new_row.truncate(r1len);
                                row2.truncate(r2len);
                                new_row.append(&mut row2);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        } else {
                            let mut new_row = row1.borrow().clone();
                            new_row.truncate(r1len);
                            new_row.append(&mut vec![Value::Null; r2len]);
                            new_view.insert_row(Rc::new(RefCell::new(new_row)));
                        }
                    }
                }
                ViewIndex::Secondary(ref i1) => {
                    for (id1, rows1) in i1.borrow().iter() {
                        for row1 in rows1 {
                            if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                                for row2 in rows2 {
                                    let mut new_row = row1.row().borrow().clone();
                                    let mut row2 = row2.row().borrow().clone();
                                    new_row.truncate(r1len);
                                    row2.truncate(r2len);
                                    new_row.append(&mut row2);
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            } else {
                                let mut new_row = row1.row().borrow().clone();
                                new_row.truncate(r1len);
                                new_row.append(&mut vec![Value::Null; r2len]);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        }
                    }
                }
            }
        }
        JoinOperator::RightOuter(JoinConstraint::On(_e)) => {
            match i2 {
                ViewIndex::Primary(ref i2, _pki) => {
                    for (id2, row2) in i2.borrow().iter() {
                        if let Some(rows1) = i1.get_index_rows_of_val(&id2) {
                            for row1 in rows1 {
                                let mut new_row = row2.borrow().clone();
                                let mut row1 = row1.row().borrow().clone();
                                new_row.truncate(r2len);
                                row1.truncate(r1len);
                                new_row.append(&mut row1);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        } else {
                            let mut new_row = row2.borrow().clone();
                            new_row.truncate(r2len);
                            new_row.append(&mut vec![Value::Null; r1len]);
                            new_view.insert_row(Rc::new(RefCell::new(new_row)));
                        }
                    }
                }
                ViewIndex::Secondary(ref i2) => {
                    for (id2, rows2) in i2.borrow().iter() {
                        for row2 in rows2 {
                            if let Some(rows1) = i1.get_index_rows_of_val(&id2) {
                                for row1 in rows1 {
                                    let mut new_row = row2.row().borrow().clone();
                                    let mut row1 = row1.row().borrow().clone();
                                    new_row.truncate(r2len);
                                    row1.truncate(r1len);
                                    new_row.append(&mut row1);
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            } else {
                                let mut new_row = row2.row().borrow().clone();
                                new_row.truncate(r2len);
                                new_row.append(&mut vec![Value::Null; r1len]);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        }
                    }
                }
            }
        }
        _ => unimplemented!("No support for join type {:?}", jo),
    }
}

fn join_using_matches(jo: &JoinOperator, i1: usize, i2: usize, r1len: usize, r2len: usize, v1rptrs: &HashedRowPtrs, v2rptrs: &HashedRowPtrs, new_view: &mut View) {
    warn!("Join using matches: v1 {:?}.{}, v2 {:?}.{}", v1rptrs, i1, v2rptrs ,i2);
    match jo {
        JoinOperator::Inner(JoinConstraint::On(_e)) => {
            for v1rptr in v1rptrs.iter() {
                let row1 = v1rptr.row().borrow();
                for v2rptr in v2rptrs.iter() {
                    let mut row2 = v2rptr.row().borrow_mut();
                    row2.truncate(r2len);
                    if helpers::parser_vals_cmp(&row2[i2], &row1[i1]) == Ordering::Equal {
                        let mut new_row = row1.clone();
                        new_row.truncate(r1len);
                        new_row.append(&mut row2.clone());
                        new_view.insert_row(Rc::new(RefCell::new(new_row)));
                    }
                }
            }
        }
        JoinOperator::LeftOuter(JoinConstraint::On(_e)) => {
            for v1rptr in v1rptrs.iter() {
                let row1 = v1rptr.row().borrow();
                let mut found = false;
                for v2rptr in v2rptrs.iter() {
                    let mut row2 = v2rptr.row().borrow_mut();
                    row2.truncate(r2len);
                    if helpers::parser_vals_cmp(&row2[i2], &row1[i1]) == Ordering::Equal {
                        let mut new_row = row1.clone();
                        new_row.truncate(r1len);
                        new_row.append(&mut row2.clone());
                        new_view.insert_row(Rc::new(RefCell::new(new_row)));
                        found = true;
                    }
                }
                if !found {
                    let mut new_row = v1rptr.row().borrow().clone();
                    new_row.truncate(r1len);
                    new_row.append(&mut vec![Value::Null; r2len]);
                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                }
            }
        }
        JoinOperator::RightOuter(JoinConstraint::On(_e)) => {
            for v2rptr in v2rptrs.iter() {
                let row2 = v2rptr.row().borrow();
                let mut found = false;
                for v1rptr in v1rptrs.iter() {
                    let mut row1 = v1rptr.row().borrow_mut();
                    row1.truncate(r1len);
                    if helpers::parser_vals_cmp(&row1[i1], &row2[i2]) == Ordering::Equal {
                        let mut new_row = row2.clone();
                        new_row.truncate(r2len);
                        new_row.append(&mut row1.clone());
                        new_view.insert_row(Rc::new(RefCell::new(new_row)));
                        found = true;
                    }
                }
                if !found {
                    let mut new_row = v2rptr.row().borrow().clone();
                    new_row.truncate(r2len);
                    new_row.append(&mut vec![Value::Null; r1len]);
                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                }
            }
        }
        _ => unimplemented!("No support for join type {:?}", jo),
    }
}

fn join_views(jo: &JoinOperator, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>, preds: &mut Vec<Vec<predicates::NamedPredicate>>) 
        -> Rc<RefCell<View>> 
{
    // XXX no order by support for joins yet
    let start = time::Instant::now();

    let (r1len, r2len) = (v1.borrow().columns.len(), v2.borrow().columns.len());

    let mut new_cols : Vec<TableColumnDef> = v1.borrow().columns.clone();
    new_cols.append(&mut v2.borrow().columns.clone());

    let mut new_view = View::new_with_cols(new_cols);
    set_primary_index_of_join(jo, v1.clone(), v2.clone(), &mut new_view);

    // select all predicate
    if preds.is_empty() {
        let (i1, i2) = get_join_on_indexes(jo, v1.clone(), v2.clone());
        join_using_indexes(jo, &i1, &i2, r1len, r2len, &mut new_view);
    } else {
        let (mut v1preds, remainder) = predicates::get_applicable_and_failed_preds(&v1.borrow(), &v1.borrow().columns, preds);
        warn!("predicates {:?} apply to v1", v1preds);
        let (mut v2preds, remainder) = predicates::get_applicable_and_failed_preds(&v2.borrow(), &v2.borrow().columns, &remainder);
        warn!("predicates {:?} apply to v2", v2preds);
        let num_predsets = remainder.iter().filter(|&preds| !preds.is_empty()).count();
        warn!("remaining predicates to apply: {:?}", remainder);
 
        // if we can't apply predicates or there is a lingering OR that could evaluate to TRUE for
        // all rows not yet constrained, join the rows by actually going through all values
        if (v1preds.is_empty() && v2preds.is_empty()) || num_predsets > 1 {
            warn!("join {}-{} using indices", v1.borrow().name, v2.borrow().name);
            let (i1, i2) = get_join_on_indexes(jo, v1.clone(), v2.clone());
            join_using_indexes(jo, &i1, &i2, r1len, r2len, &mut new_view);
        } else {
            // otherwise, we can apply some predicates, and then we just join these selected predicates and set them as the
            // new_view rows
            if v1preds.is_empty() {
                v1preds = vec![vec![predicates::NamedPredicate::Bool(true)]];
            }
            if v2preds.is_empty() {
                v2preds = vec![vec![predicates::NamedPredicate::Bool(true)]];
            }
            let v1rptrs = predicates::get_rptrs_matching_preds(&v1.borrow(), &v1.borrow().columns, &v1preds);
            let v2rptrs = predicates::get_rptrs_matching_preds(&v2.borrow(), &v2.borrow().columns, &v2preds);
            warn!("join {:?}-{:?} using matches", v1rptrs, v2rptrs);

            warn!("Applying predicates {:?} to rest", remainder);
            // note that these rows may still later have to be filtered by any remaining predicates
            // (e.g., on computed rows, or over the joined rows)
            *preds = remainder;
            let (i1, i2) = get_join_on_indices(jo, v1.clone(), v2.clone());
            join_using_matches(jo, i1, i2, r1len, r2len, &v1rptrs, &v2rptrs, &mut new_view);
        }
    }
    let dur = start.elapsed();
    warn!("Join views took: {}us", dur.as_micros());
    Rc::new(RefCell::new(new_view))
}

/*
 * Joins views, applying predicates when possible
 * Removes all prior applied predicates
 */
pub fn tablewithjoins_to_view(views: &HashMap<String, Rc<RefCell<View>>>, twj: &TableWithJoins, preds: &mut Vec<Vec<predicates::NamedPredicate>>) 
    -> Rc<RefCell<View>>
{
    let mut joined_views = tablefactor_to_view(views, &twj.relation);
    
    for j in &twj.joins {
        let view2 = tablefactor_to_view(views, &j.relation);
        joined_views = join_views(&j.join_operator, joined_views, view2, preds);
    }
    joined_views
}
