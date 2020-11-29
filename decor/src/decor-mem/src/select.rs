use crate::views::{View, TableColumnDef, RowPtrs, HashedRowPtr, RowPtr};
use crate::{helpers, INIT_CAPACITY, predicates, joins};
use log::{warn, debug};
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use std::io::{Error};
use std::str::FromStr;
use std::time;
use sql_parser::ast::*;
use std::cell::RefCell;
use std::rc::Rc;



/*
 * Turn expression into a value for row
 */
pub fn get_value_for_row_closure(e: &Expr, columns: &Vec<TableColumnDef>) -> Box<dyn Fn(&Vec<Value>) -> Value> {
    let closure: Option<Box<dyn Fn(&Vec<Value>) -> Value>>;
    let start = time::Instant::now();
    match &e {
        Expr::Identifier(_) => {
            let (_tab, col) = helpers::expr_to_col(&e);
            debug!("Identifier column {}", col);

            let ci = helpers::get_col_index(&col, &columns);
            if let Some(ci) = ci {
                closure = Some(Box::new(move |row| row[ci].clone()));
            } else {
                unimplemented!("No value?");
            }
        }
        Expr::Value(val) => {
            let newv = val.clone();
            closure = Some(Box::new(move |_row| newv.clone()));
        }
        Expr::UnaryOp{op, expr} => {
            if let Expr::Value(ref val) = **expr {
                match op {
                    UnaryOperator::Minus => {
                        let n = -1.0 * helpers::parser_val_to_f64(&val);
                        closure = Some(Box::new(move |_row| Value::Number(n.to_string())));
                    }
                    _ => unimplemented!("Unary op not supported! {:?}", expr),
                }
            } else {
                unimplemented!("Unary op not supported! {:?}", expr);
            }
        }
        Expr::BinaryOp{left, op, right} => {
            let mut lindex : Option<usize> = None;
            let mut rindex : Option<usize> = None;
            let mut lval : Box<dyn Fn(&Vec<Value>) -> Value> = Box::new(|_row| Value::Null);
            let mut rval : Box<dyn Fn(&Vec<Value>) -> Value> = Box::new(|_row| Value::Null);
            match &**left {
                Expr::Identifier(_) => {
                    let (_ltab, lcol) = helpers::expr_to_col(&left);
                    lindex = helpers::get_col_index(&lcol, columns);
                }
                Expr::Value(val) => {
                    let newv = val.clone();
                    lval = Box::new(move |_row| newv.clone());
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            match &**right {
                Expr::Identifier(_) => {
                    let (_rtab, rcol) = helpers::expr_to_col(&right);
                    rindex = helpers::get_col_index(&rcol, columns);
                }
                Expr::Value(val) => {
                    let newv = val.clone();
                    rval = Box::new(move |_row| newv.clone());
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            if let Some(li) = lindex {
                lval = Box::new(move |row| row[li].clone());
            }
            if let Some(ri) = rindex {
                rval = Box::new(move |row| row[ri].clone());
            }
            match op {
                BinaryOperator::Plus => {
                    closure = Some(Box::new(move |row| helpers::plus_parser_vals(&lval(row), &rval(row))));
                }
                BinaryOperator::Minus => {
                    closure = Some(Box::new(move |row| helpers::minus_parser_vals(&lval(row), &rval(row))));
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


/* 
 * Return vectors of columns, rows, and additional computed columns/values
 */
fn get_setexpr_results(views: &HashMap<String, Rc<RefCell<View>>>, se: &SetExpr, order_by: &Vec<OrderByExpr>) 
    -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), std::io::Error> 
{
    

    match se {
        SetExpr::Select(s) => {
            if s.having != None {
                unimplemented!("No support for having queries");
            }
            
            let mut order_by_col = None;
            // order by grouping if it exists
            if s.group_by.len() > 0 {
                // support only one group by for now
                assert!(s.group_by.len() == 1); 
                let (tab, col) = helpers::expr_to_col(&s.group_by[0]);
                order_by_col = Some(format!("{}.{}", tab, col));
            } else if order_by.len() > 0 {
                // otherwise, order by whatever column (note that this ignores ordering by the count---that
                // will have to be done later)
                let (tab, col) = helpers::expr_to_col(&order_by[0].expr);
                order_by_col = Some(format!("{}.{}", tab, col));
            }

            let mut preds : Vec<Vec<predicates::NamedPredicate>> = vec![];
            if let Some(selection) = &s.selection {
                preds = predicates::get_predicate_sets_of_constraint(&selection);
            }
            // TODO don't need to init?
            let mut from_view: Rc<RefCell<View>> = Rc::new(RefCell::new(View::new_with_cols(vec![])));
            
            // special case: we're getting results from only this view
            assert!(s.from.len() <= 1);
            for twj in &s.from {
                from_view = joins::tablewithjoins_to_view(views, &twj, &mut preds);
            }
            debug!("Joined new view is {:?}", from_view);

            // 1) compute any additional cols added by projection 
            // 2) keep track of whether we want to return the count of rows (count)
            // 3) keep track of whether we want to return 1 for each row (select_val)
            // 4) keep track of which columns to keep for which tables (cols_to_keep)
            // 5) get indices of order by (if any)
            //
            // INVARIANT: from_view is not modified during this block
            let from_view = from_view.borrow();
            let table_name = from_view.name.clone();
            let mut columns: Vec<TableColumnDef> = from_view.columns.clone(); 
            let mut cols_to_keep = vec![];
            let mut computed_cols = vec![];
            let mut select_val = None;
            let mut count = false;
            let mut count_alias = Ident::new("count");
            for proj in &s.projection {
                match proj {
                    SelectItem::Wildcard => {
                        // only support wildcards if there are no other projections...
                        assert!(s.projection.len() == 1);
                        cols_to_keep = (0..columns.len()).collect();
                    },
                    SelectItem::Expr {expr, alias} => {
                        // SELECT 1...
                        if let Expr::Value(v) = expr {
                            select_val = Some(v);

                        } else if let Expr::QualifiedWildcard(ids) = expr {
                            // SELECT `tablename.*`: keep all columns of this table
                            
                            let table_to_select = ids.last().unwrap().to_string();
                            for (i, c) in columns.iter().enumerate() {
                                if c.table == table_to_select {
                                    cols_to_keep.push(i);
                                }
                            } 
                        
                        } else if let Expr::Identifier(_ids) = expr {
                            // SELECT `tablename.columname` 
                            
                            let (_tab, col) = helpers::expr_to_col(expr);
                            debug!("{}: selecting {}", s, col);
                            let ci = helpers::get_col_index(&col, &columns).unwrap();
                            cols_to_keep.push(ci);
                            
                            // alias; use in WHERE will match against this alias
                            if let Some(a) = alias {
                                columns[ci].column.name = Ident::new(a.to_string());
                                columns[ci].table = String::new();
                            }

                        } else if let Expr::BinaryOp{..} = expr {
                            // SELECT `col1 - col2 AS alias`
                            assert!(alias.is_some());
                            let a = alias.as_ref().unwrap();
                            let colname = a.to_string();
                            debug!("Adding to computed columns {}: {}", colname, expr);
                            computed_cols.push((colname, expr));
                        } else if let Expr::Function(f) = expr {
                            if f.name.to_string() == "count" && f.args == FunctionArgs::Star {
                                count = true;
                                if let Some(a) = alias {
                                    count_alias = a.clone();
                                }
                            }
                        } else {
                            unimplemented!("No support for projection {:?}", expr);
                        }
                    }
                }
            }

            // filter out rows by where clause
            let mut rptrs_to_keep: RowPtrs;
            // keep all rows if there are no predicates! (join filtered them all out)
            if preds.is_empty() {
                rptrs_to_keep = from_view.rows.borrow()
                    .iter()
                    .map(|(_, rptr)| rptr.clone())
                    .collect();
            } else {
                let (rptrs_to_keep_preds, remainder_preds) = predicates::get_rptrs_matching_preds(&from_view, &columns, &mut preds);
                rptrs_to_keep = rptrs_to_keep_preds.iter().map(|hrptr| hrptr.row().clone()).collect();
                assert!(remainder_preds.is_empty());
            }

            // fast path: return val if select val was issued
            if let Some(val) = select_val {
                let val_row = Rc::new(RefCell::new(vec![val.clone()]));
                // TODO this inserts the value only once?
                let rows : RowPtrs = vec![val_row.clone(); rptrs_to_keep.len()];
                
                // not sure what to put for column in this case but it's probably ok?
                columns = vec![TableColumnDef{
                    table: "".to_string(),
                    colname: "".to_string(),
                    fullname: "".to_string(),
                    column: ColumnDef {
                        name: Ident::new(""),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![],
                    }
                }];
                cols_to_keep = vec![0];
                return Ok((columns, rows, cols_to_keep));
            }
                        
            // add computed cols 
            for (col, expr) in computed_cols.iter() {
                let newcol_index = columns.len();
                cols_to_keep.push(newcol_index);
                columns.push(TableColumnDef{
                    table: table_name.clone(),
                    colname: col.clone(),
                    fullname: "".to_string(),
                    column: ColumnDef{
                        name: Ident::new(col.clone()),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![],

                    },
                });

                // XXX NOTE: WE'RE ACTUALLY MODIFYING THE ROW HERE???
                let ccval_func = get_value_for_row_closure(&expr, &columns);
                for rptr in &rptrs_to_keep {
                    let mut row = rptr.borrow_mut();
                    let val = ccval_func(&row);
                    if row.len() > newcol_index {
                        row[newcol_index] = val;
                    } else {
                        row.push(val);
                    }
                }
            }

            // add the count of selected columns as an extra column if it were projected
            if count {
                
                let newcol_index = columns.len();
                cols_to_keep.push(newcol_index);

                columns.push(TableColumnDef{
                    table: table_name.clone(),
                    colname: count_alias.to_string(),
                    fullname: count_alias.to_string(),
                    column: ColumnDef {
                        name: count_alias,
                        data_type: DataType::Int,
                        collation:None,
                        options: vec![],
                    }
                });

                assert!(s.group_by.len() == 1);
                let (_tab, col) = helpers::expr_to_col(&s.group_by[0]);
                let ci = helpers::get_col_index(&col, &columns).unwrap();

                // get the counts, grouping rows by the specified value
                let mut counts : HashMap<Value, (RowPtr, usize)> = HashMap::new();
                for rptr in &rptrs_to_keep {
                    let row = rptr.borrow();
                    if let Some(row_with_cnt) = counts.get_mut(&row[ci]) {
                        row_with_cnt.1 += 1;
                    } else {
                        counts.insert(row[ci].clone(), (rptr.clone(), 1));
                    }
                }
                // new set of rows to keep!
                rptrs_to_keep.clear(); 
                for (_val, rowcnts) in counts {
                    let mut row = rowcnts.0.borrow_mut();
                    if row.len() > newcol_index {
                        row[newcol_index] = Value::Number(rowcnts.1.to_string());
                    } else {
                        row[newcol_index] = Value::Number(rowcnts.1.to_string());
                    }
                    rptrs_to_keep.push(rowcnts.0.clone());
                }
            }

            debug!("setexpr select: returning {:?}", rptrs_to_keep);
            Ok((columns, rptrs_to_keep, cols_to_keep))
        }
        /*SetExpr::Query(q) => {
            return get_query_results(views, &q);
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            ..
        } => {
            let (mut lcols, mut left_rows, mut lcols_to_keep) = get_setexpr_results(views, &left, order_by)?;
            let (mut rcols, right_rows, mut rcols_to_keep) = get_setexpr_results(views, &right, order_by)?;
            lcols.append(&mut rcols);
            match op {
                // TODO primary keys / unique keys 
                SetOperator::Union => {
                    // TODO currently allowing for duplicates regardless of ALL...
                    rcols_to_keep = rcols_to_keep.iter().map(|ci| lcols.len() + ci).collect();
                    lcols_to_keep.append(&mut rcols_to_keep);
                    left_rows.extend(right_rows);
                    lcols.append(&mut rcols);

                    return Ok((lcols, left_rows, lcols_to_keep));
                }
                _ => unimplemented!("Not supported set operation {}", se),
            }
        }*/
        SetExpr::Values(_vals) => {
            unimplemented!("Shouldn't be getting values when looking up results: {}", se); 
        }
        _ => unimplemented!("Don't support select queries yet {}", se),
    }
}

pub fn get_query_results(views: &HashMap<String, Rc<RefCell<View>>>, q: &Query) -> 
    Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), Error> {
    let (all_cols, mut rptrs_vec, cols_to_keep) = get_setexpr_results(views, &q.body, &q.order_by)?;
    
    // don't support OFFSET or fetches yet
    assert!(q.offset.is_none() && q.fetch.is_none());
    
    let start = time::Instant::now();

    // limit
    let mut limit = rptrs_vec.len();
    if q.limit.is_some() {
        if let Some(Expr::Value(Value::Number(n))) = &q.limit {
            limit = usize::from_str(n).unwrap();
        } else {
            unimplemented!("bad limit! {}", q);
        }
    }
    if q.order_by.len() > 0 {
        // TODO only support at most two order by constraints for now
        assert!(q.order_by.len() < 3); 
        let orderby1 = &q.order_by[0];
        let (_tab, col1) = helpers::expr_to_col(&orderby1.expr);
        let ci1 = helpers::get_col_index(&col1, &all_cols).unwrap();
       
        if q.order_by.len() == 2 {
            let orderby2 = &q.order_by[1];
            let (_tab, col2) = helpers::expr_to_col(&orderby2.expr);
            let ci2 = helpers::get_col_index(&col2, &all_cols).unwrap();
            match orderby1.asc {
                Some(false) => {
                    rptrs_vec.sort_by(|r1, r2| {
                        let res = helpers::parser_vals_cmp(&r2.borrow()[ci1], &r1.borrow()[ci1]);
                        if res == Ordering::Equal {
                            match orderby2.asc {
                                Some(false) => helpers::parser_vals_cmp(&r2.borrow()[ci2], &r1.borrow()[ci2]),
                                Some(true) | None => helpers::parser_vals_cmp(&r1.borrow()[ci2], &r2.borrow()[ci2]),
                            }
                        } else {
                            res
                        }});
                }
                Some(true) | None => {
                    rptrs_vec.sort_by(|r1, r2| {
                        let res = helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1]);
                        if res == Ordering::Equal {
                            match orderby2.asc {
                                Some(false) => helpers::parser_vals_cmp(&r2.borrow()[ci2], &r1.borrow()[ci2]),
                                Some(true) | None => helpers::parser_vals_cmp(&r1.borrow()[ci2], &r2.borrow()[ci2]),
                            }
                        } else {
                            res
                        }});
                }
            }
        } else {
            match orderby1.asc {
                Some(false) => {
                    rptrs_vec.sort_by(|r1, r2| {
                        helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1])});
                    debug!("order by desc! {:?}", rptrs_vec);
                }
                Some(true) | None => {
                    debug!("before sort: order by asc! {:?}", rptrs_vec);
                    rptrs_vec.sort_by(|r1, r2| {
                        helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1])});
                    debug!("order by asc! {:?}", rptrs_vec);
                }
            }
        }
    }
    rptrs_vec.truncate(limit);
    let dur = start.elapsed();
    warn!("order by took {}us", dur.as_micros());

    Ok((all_cols, rptrs_vec, cols_to_keep))
}
