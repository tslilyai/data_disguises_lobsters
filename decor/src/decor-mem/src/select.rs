use crate::views::{View, TableColumnDef, RowPtrs, ViewIndex, HashedRowPtr, HashedRowPtrs};
use crate::{helpers, INIT_CAPACITY, predicates};
use log::{warn, debug};
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::time;
use sql_parser::ast::*;
use std::cell::RefCell;
use std::rc::Rc;

/*
 * Convert table name (with optional alias) to current view
 */
fn tablefactor_to_view(views: &HashMap<String, Rc<RefCell<View>>>, tf: &TableFactor) -> Result<Rc<RefCell<View>>, Error> {
    match tf {
        TableFactor::Table {
            name,
            alias,
        } => {
            let tab = views.get(&name.to_string());
            match tab {
                None => Err(Error::new(ErrorKind::Other, format!("table {:?} does not exist", tf))),
                Some(t) => {
                    if alias.is_some() {
                        unimplemented!("No aliasing of tables for now {}", tf);
                    }
                    /*if let Some(a) = alias {
                        // alias column table names too?
                        assert!(a.columns.is_empty());
                        view.name = a.name.to_string();
                    }*/
                    Ok(t.clone())
                }
            }
        }
        _ => unimplemented!("no derived joins {:?}", tf),
    }
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
fn get_join_on_indexes(e: &Expr, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> (ViewIndex, ViewIndex) {
    let is : (ViewIndex, ViewIndex);
    if let Expr::Nested(binexpr) = e {
        is = get_binop_indexes(binexpr, v1.clone(), v2.clone());
    } else {
        is = get_binop_indexes(e, v1.clone(), v2.clone());
    }
    is
}


fn join_views(jo: &JoinOperator, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> Result<Rc<RefCell<View>>, Error> {
    let start = time::Instant::now();

    let mut new_cols : Vec<TableColumnDef> = v1.borrow().columns.clone();
    new_cols.append(&mut v2.borrow().columns.clone());
    
    // TODO Fix names, indexes
    let mut new_view = View::new_with_cols(new_cols);
    //new_view.indexes 
    
    // assuming that indexes exist?
    let (r1len, r2len) = (v1.borrow().columns.len(), v2.borrow().columns.len());
   
    match jo {
        JoinOperator::Inner(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_indexes(&e, v1.clone(), v2.clone());
            match i1 {
                ViewIndex::Primary(ref i1, _pki) => {
                    for (id1, row1) in i1.borrow().iter() {
                        if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                            for row2 in rows2 {
                                let mut new_row = row1.borrow().clone();
                                new_row.append(&mut row2.row().borrow().clone());
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
                                    new_row.append(&mut row2.row().borrow().clone());
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            }
                        }
                    }
                }
            }
        }
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_indexes(&e, v1.clone(), v2.clone());
            match i1 {
                ViewIndex::Primary(ref i1, _pki) => {
                    for (id1, row1) in i1.borrow().iter() {
                        if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                            for row2 in rows2 {
                                let mut new_row = row1.borrow().clone();
                                new_row.append(&mut row2.row().borrow().clone());
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        } else {
                            let mut new_row = row1.borrow().clone();
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
                                    new_row.append(&mut row2.row().borrow().clone());
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            } else {
                                let mut new_row = row1.row().borrow().clone();
                                new_row.append(&mut vec![Value::Null; r2len]);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        }
                    }
                }
            }
        }
        JoinOperator::RightOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_indexes(&e, v1.clone(), v2.clone());
            match i2 {
                ViewIndex::Primary(ref i2, _pki) => {
                    for (id2, row2) in i2.borrow().iter() {
                        if let Some(rows1) = i1.get_index_rows_of_val(&id2) {
                            for row1 in rows1 {
                                let mut new_row = row2.borrow().clone();
                                new_row.append(&mut row1.row().borrow().clone());
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        } else {
                            let mut new_row = row2.borrow().clone();
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
                                    new_row.append(&mut row1.row().borrow().clone());
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            } else {
                                let mut new_row = row2.row().borrow().clone();
                                new_row.append(&mut vec![Value::Null; r1len]);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        }
                    }
                }
            }
        }
        JoinOperator::FullOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_indexes(&e, v1.clone(), v2.clone());
            match i1 {
                ViewIndex::Primary(ref i1, _pki) => {
                    for (id1, row1) in i1.borrow().iter() {
                        if let Some(rows2) = i2.get_index_rows_of_val(&id1) {
                            for row2 in rows2 {
                                let mut new_row = row1.borrow().clone();
                                new_row.append(&mut row2.row().borrow().clone());
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        } else {
                            let mut new_row = row1.borrow().clone();
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
                                    new_row.append(&mut row2.row().borrow().clone());
                                    new_view.insert_row(Rc::new(RefCell::new(new_row)));
                                }
                            } else {
                                let mut new_row = row1.row().borrow().clone();
                                new_row.append(&mut vec![Value::Null; r2len]);
                                new_view.insert_row(Rc::new(RefCell::new(new_row)));
                            }
                        }
                    }
                }
            }
            // only add null rows for rows that weren't matched
            match i2 {
                ViewIndex::Primary(ref i2, _pki) => {
                    for (id2, row2) in i2.borrow().iter() {
                        if i1.get_index_rows_of_val(&id2).is_none() {
                            let mut new_row = row2.borrow().clone();
                            new_row.append(&mut vec![Value::Null; r1len]);
                            new_view.insert_row(Rc::new(RefCell::new(new_row)));
                        }
                    }
                }
                ViewIndex::Secondary(ref i2) => {
                    for (id2, rows2) in i2.borrow().iter() {
                        for row2 in rows2 {
                            if i1.get_index_rows_of_val(&id2).is_none() {
                                let mut new_row = row2.row().borrow().clone();
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
    let dur = start.elapsed();
    warn!("Join views took: {}us", dur.as_micros());

    Ok(Rc::new(RefCell::new(new_view)))
}

fn tablewithjoins_to_view(views: &HashMap<String, Rc<RefCell<View>>>, twj: &TableWithJoins) -> Result<Rc<RefCell<View>>, Error> {
    let mut joined_views = tablefactor_to_view(views, &twj.relation)?;
    
    for j in &twj.joins {
        let view2 = tablefactor_to_view(views, &j.relation)?;
        joined_views = join_views(&j.join_operator, joined_views, view2)?;
    }
    Ok(joined_views)
}

/*
 * Turn expression into a value for row
 */
pub fn get_value_for_row_closure(e: &Expr, 
                         columns: &Vec<TableColumnDef>)
-> Box<dyn Fn(&Vec<Value>) -> Value> {
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
    -> Result<(Vec<TableColumnDef>, HashSet<HashedRowPtr>, Vec<usize>), std::io::Error> {
    match se {
        SetExpr::Select(s) => {
            if s.having != None {
                unimplemented!("No support for having queries");
            }

            let mut preds = vec![];
            if let Some(selection) = &s.selection {
                preds = predicates::get_predicate_sets_of_constraint(&selection);
            } else {
                preds = vec![vec![predicates::NamedPredicate::Bool(true)]]
            }

            // TODO don't need to init?
            let mut from_view: Rc<RefCell<View>> = Rc::new(RefCell::new(View::new_with_cols(vec![])));
            
            // special case: we're getting results from only this view
            assert!(s.from.len() <= 1);
            for twj in &s.from {
                from_view = tablewithjoins_to_view(views, &twj)?;
                // TODO correctly update primary index---right now there can be duplicates from
                // different tables
                // TODO support multiple joins
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

            // fast path: return val if select val was issued
            if let Some(val) = select_val {
                let mut rows : HashSet<HashedRowPtr> = HashSet::with_capacity(INIT_CAPACITY);
                let val_row = HashedRowPtr::new(Rc::new(RefCell::new(vec![val.clone()])), 0);
                // TODO this inserts the value only once?
                rows.insert(val_row);
                
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

            // filter out rows by where clause
            let rptrs_to_keep : HashedRowPtrs;
            if let Some(selection) = &s.selection {
                rptrs_to_keep = predicates::get_rptrs_matching_constraint(&selection, &from_view, &columns);
                debug!("Where: Keeping rows {}: \n\t{:?}", selection, rptrs_to_keep);
            } else {
                rptrs_to_keep = from_view.rows.borrow().iter().map(
                    |(_pk, rptr)| HashedRowPtr::new(rptr.clone(), from_view.primary_index)).collect();
                warn!("get all ptrs for NONE selection {}", se);
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
                for rptr in rptrs_to_keep.iter() {
                    let mut row = rptr.row().borrow_mut();
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

                let num = Value::Number(rptrs_to_keep.len().to_string());
                for rptr in &rptrs_to_keep {
                    let mut row = rptr.row().borrow_mut();
                    if row.len() > newcol_index {
                        row[newcol_index] = num.clone();
                    } else {
                        row.push(num.clone());
                    }
                }
            }


            debug!("setexpr select: returning {:?}", rptrs_to_keep);
            Ok((columns, rptrs_to_keep, cols_to_keep))
        }
        /*SetExpr::Query(q) => {
            return get_query_results(views, &q);
        }*/
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
        }
        SetExpr::Values(_vals) => {
            unimplemented!("Shouldn't be getting values when looking up results: {}", se); 
        }
        _ => unimplemented!("Don't support select queries yet {}", se),
    }
}

pub fn get_query_results(views: &HashMap<String, Rc<RefCell<View>>>, q: &Query) -> 
    Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), Error> {
    let (all_cols, rptrs, cols_to_keep) = get_setexpr_results(views, &q.body, &q.order_by)?;
    // don't support OFFSET or fetches yet
    assert!(q.offset.is_none() && q.fetch.is_none());

    // limit
    let mut limit = rptrs.len();
    if q.limit.is_some() {
        if let Some(Expr::Value(Value::Number(n))) = &q.limit {
            limit = usize::from_str(n).unwrap();
        } else {
            unimplemented!("bad limit! {}", q);
        }
    }

    let start = time::Instant::now();
    let mut rptrs_vec: RowPtrs = rptrs.iter().map(|r| r.row().clone()).collect();
    let dur = start.elapsed();
    warn!("Collecting hashset of {} rptrs to vec: {}us", rptrs_vec.len(), dur.as_micros());
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
                    debug!("order by desc! {:?}", rptrs);
                }
                Some(true) | None => {
                    debug!("before sort: order by asc! {:?}", rptrs);
                    rptrs_vec.sort_by(|r1, r2| {
                        helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1])});
                    debug!("order by asc! {:?}", rptrs);
                }
            }
        }
    }
    rptrs_vec.truncate(limit);
    let dur = start.elapsed();
    warn!("order by took {}us", dur.as_micros());

    Ok((all_cols, rptrs_vec, cols_to_keep))
}
