use crate::views::{View, TableColumnDef, RowPtrs};
use crate::helpers;
use log::warn;
use std::collections::{HashMap};
use std::cmp::Ordering;
use std::io::{Error, ErrorKind};
use std::str::FromStr;
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
                    /*let mut view = t.clone();
                    if let Some(a) = alias {
                        // only alias table name..
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

fn get_binop_indices(e: &Expr, v1: &View, v2: &View) -> Option<(usize, usize)> {
    let i1: Option<usize>; 
    let i2 : Option<usize>; 
    if let Expr::BinaryOp{left, op, right} = e {
        if let BinaryOperator::Eq = op {
            let (tab1, col1) = expr_to_col(left);
            let (tab2, col2) = expr_to_col(right);
            warn!("Join got tables and columns {}.{} and {}.{} from expr {:?}", tab1, col1, tab2, col2, e);

            // note: view 1 may not have name attached any more because it was from a prior join.
            // the names are embedded in the columns of the view, so we should compare the entire
            // name of the new column/table
            if v2.name == tab2 {
                i1 = v1.columns.iter().position(|c| tablecolumn_matches_col(c, &format!("{}.{}", tab1, col1)));
                i2 = v2.columns.iter().position(|c| tablecolumn_matches_col(c, &col2));
            } else if v2.name == tab1 {
                i1 = v2.columns.iter().position(|c| tablecolumn_matches_col(c, &col1));
                i2 = v1.columns.iter().position(|c| tablecolumn_matches_col(c, &format!("{}.{}", tab2, col2)));
            } else {
                warn!("Join: no matching tables for {}/{} and {}/{}", v1.name, tab1, v2.name, tab2);
                return None;
            }
            if i1 == None || i2 == None {
                warn!("No columns found! {:?} {:?}", v1.columns, v2.columns);
                return None;
            }
            return Some((i1.unwrap(), i2.unwrap()));
        }
    }
    None
}

/*
 * Only handle join constraints of form "table.col = table'.col'"
 */
fn get_join_on_col_indices(e: &Expr, v1: &View, v2: &View) -> Result<(usize, usize), Error> {
    let is : Option<(usize, usize)>;
    if let Expr::Nested(binexpr) = e {
        is = get_binop_indices(binexpr, v1, v2);
    } else {
        is = get_binop_indices(e, v1, v2);
    }

    match is {
        None => unimplemented!("Unsupported join_on {:?}, {}, {}", e, v1.name, v2.name),
        Some((i1, i2)) => Ok((i1, i2)),
    }
}

fn join_views(jo: &JoinOperator, v1: Rc<RefCell<View>>, v2: Rc<RefCell<View>>) -> Result<Rc<RefCell<View>>, Error> {
    //warn!("Joining views {} and {}", v1.name, v2.name);
    let mut new_cols : Vec<TableColumnDef> = v1.borrow().columns.clone();
    new_cols.append(&mut v2.borrow().columns.clone());
    let new_view = View::new_with_cols(new_cols);
    /*match jo {
        JoinOperator::Inner(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            // this seems very very inefficient
            for (_id, row1) in v1.rows.iter() {
                for row2 in v2.get_rptrs_of_col(i2, &row1[i1]) {
                    // remove duplicate from row
                    // TODO
                    //row2.remove(i2);
                    let mut new_row = row1.clone();
                    new_row.append(&mut row2);
                    new_view.insert_row(new_row);
                }
            }
        }
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            for (_, row1) in v1.rows.iter() {
                let mut found = false;
                for mut row2 in v2.get_rows_of_col(i2, &row1[i1]) {
                    // remove duplicatte from row
                    row2.remove(i2);
                    let mut new_row = row1.clone();
                    new_row.append(&mut row2);
                    new_view.insert_row(new_row);
                    found = true;
                }
                if !found {
                    let mut new_row = row1.clone();
                    new_row.append(&mut vec![Value::Null; v2.columns.len()]);
                    new_view.insert_row(new_row);
                }
            }
        }
        JoinOperator::RightOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            for (_, row2) in v2.rows.iter() {
                let mut found = false;
                for mut row1 in v1.get_rows_of_col(i2, &row2[i1]) {
                    // remove duplicatte from row
                    row1.remove(i2);
                    let mut new_row = row2.clone();
                    new_row.append(&mut row1);
                    new_view.insert_row(new_row);
                    found = true;
                }
                if !found {
                    let mut new_row = row2.clone();
                    new_row.append(&mut vec![Value::Null; v1.columns.len()]);
                    new_view.insert_row(new_row);
                }
            }            
        }
        JoinOperator::FullOuter(JoinConstraint::On(e)) => {
            let (i1, i2) = get_join_on_col_indices(&e, v1, v2)?;
            for (_, row1) in v1.rows.iter() {
                let mut found = false;
                for mut row2 in v2.get_rows_of_col(i2, &row1[i1]) {
                    // remove duplicatte from row
                    row2.remove(i2);
                    let mut new_row = row1.clone();
                    new_row.append(&mut row2);
                    new_view.insert_row(new_row);
                    found = true;
                }
                if !found {
                    let mut new_row = row1.clone();
                    new_row.append(&mut vec![Value::Null; v2.columns.len()]);
                    new_view.insert_row(new_row);
                }
            }
            // only add null rows for rows that weren't matched
            for (_, row2) in v2.rows.iter() {
                let mut found = false;
                if !v1.get_rows_of_col(i2, &row2[i1]).is_empty() {
                    found = true;
                }
                if !found {
                    let mut new_row = row2.clone();
                    new_row.append(&mut vec![Value::Null; v1.columns.len()]);
                    new_view.insert_row(new_row);
                }
            }            
        }
        _ => unimplemented!("No support for join type {:?}", jo),
    }*/
    Ok(Rc::new(RefCell::new(new_view)))
}

fn tablewithjoins_to_view(views: &HashMap<String, Rc<RefCell<View>>>, twj: &TableWithJoins) -> Result<Rc<RefCell<View>>, Error> {
    // TODO only do expensive copy if there is an actual join
    // TODO copy indices when joining?
    let mut joined_views = tablefactor_to_view(views, &twj.relation)?;
    
    for j in &twj.joins {
        let view2 = tablefactor_to_view(views, &j.relation)?;
        joined_views = join_views(&j.join_operator, joined_views, view2)?;
    }
    Ok(joined_views)
}

// return table name and optionally column if not wildcard
fn expr_to_col(e: &Expr) -> (String, String) {
    //warn!("expr_to_col: {:?}", e);
    match e {
        // only support form column or table.column
        Expr::Identifier(ids) => {
            if ids.len() > 2 || ids.len() < 1 {
                unimplemented!("expr needs to be of form table.column {}", e);
            }
            if ids.len() == 2 {
                return (ids[0].to_string(), ids[1].to_string());
            }
            return ("".to_string(), ids[0].to_string());
        }
        _ => unimplemented!("expr_to_col {} not supported", e),
    }
}

pub fn tablecolumn_matches_col(c: &TableColumnDef, col: &str) -> bool {
    c.column.name.to_string() == col || c.name() == col
}

/*
 * Turn expression into a value for row
 */
pub fn get_value_for_row_closure(e: &Expr, 
                         columns: &Vec<TableColumnDef>,
                         aliases: Option<&HashMap<String, usize>>, 
                         computed_opt: Option<&HashMap<String, &Expr>>)
-> Box<dyn Fn(&Vec<Value>) -> Value> {
    match &e {
        Expr::Identifier(_) => {
            let (_tab, col) = expr_to_col(&e);
            warn!("Identifier column {}", col);

            let ci = match columns.iter().position(|c| tablecolumn_matches_col(c, &col)) {
                Some(ci) => Some(ci),
                None => match aliases {
                    Some(a) => match a.get(&col) {
                        Some(ci) => Some(*ci),
                        None => None
                    }
                    None => None,
                }
            };
            if let Some(ci) = ci {
                return Box::new(move |row| row[ci].clone());
            } else if let Some(computed) = computed_opt {
                // if this col is a computed col, check member in list and return
                if let Some(e) = computed.get(&col) {
                    let computed_func = get_value_for_row_closure(&e, columns, aliases, Some(computed));
                    return Box::new(move |row| computed_func(row));
                }
            }
            unimplemented!("No value?");
        }
        Expr::Value(val) => {
            let newv = val.clone();
            return Box::new(move |_row| newv.clone());
        }
        Expr::UnaryOp{op, expr} => {
            if let Expr::Value(ref val) = **expr {
                match op {
                    UnaryOperator::Minus => {
                        let n = -1.0 * helpers::parser_val_to_f64(&val);
                        return Box::new(move |_row| Value::Number(n.to_string()));
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
                    let (_ltab, lcol) = expr_to_col(&left);
                    lindex = get_col_index_with_aliases(&lcol, columns, aliases);
                    if lindex.is_none() {
                        if let Some(computed) = computed_opt {
                            if let Some(e) = computed.get(&lcol) {
                                lval = get_value_for_row_closure(e, columns, aliases, Some(computed));
                            }
                        }
                    }
                }
                Expr::Value(val) => {
                    let newv = val.clone();
                    lval = Box::new(move |_row| newv.clone());
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            match &**right {
                Expr::Identifier(_) => {
                    let (_rtab, rcol) = expr_to_col(&right);
                    rindex = get_col_index_with_aliases(&rcol, columns, aliases);
                    if rindex.is_none() {
                        if let Some(computed) = computed_opt {
                            if let Some(e) = computed.get(&rcol) {
                                rval = get_value_for_row_closure(e, columns, aliases, Some(computed));
                            }
                        }
                    }
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
                    return Box::new(move |row| helpers::plus_parser_vals(&lval(row), &rval(row)));
                }
                BinaryOperator::Minus => {
                    return Box::new(move |row| helpers::minus_parser_vals(&lval(row), &rval(row)));
                }
                _ => unimplemented!("op {} not supported to get value", op),
            }
        }
        _ => unimplemented!("get value not supported {}", e),
    }
}

fn get_col_index_with_aliases(col: &str, columns: &Vec<TableColumnDef>, aliases: Option<&HashMap<String, usize>>) -> Option<usize> {
    match columns.iter().position(|c| tablecolumn_matches_col(c, col)) {
        Some(ci) => Some(ci),
        None => match aliases {
            Some(a) => match a.get(col) {
                Some(ci) => Some(*ci),
                None => None
            }
            None => None,
        }
    }
}

/* 
 * returns (negated, MatchingPtrs) 
 * 
 * */
pub fn get_rptrs_matching_constraint(e: &Expr, v: &View, 
                                    aliases: Option<&HashMap<String, usize>>, 
                                    computed: Option<&HashMap<String, &Expr>>)
    -> (bool, RowPtrs)
{
    let mut matching_rows = vec![];
    match e {
        Expr::Value(Value::Boolean(b)) => {
            return (*b, matching_rows);
        } 
        Expr::InList { expr, list, negated } => {
            let list_vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (_tab, col) = expr_to_col(&expr);
              
            if let Some(ci) = get_col_index_with_aliases(&col, &v.columns, aliases) {
                for lv in &list_vals {
                    matching_rows.append(&mut v.get_rptrs_of_col(ci, lv));
                }
            } else if let Some(computed) = computed {
                // if this col is a computed col, check member in list and return
                if let Some(e) = computed.get(&col) {
                    let ccval_func = get_value_for_row_closure(&e, &v.columns, aliases, Some(&computed));
                    for (_pk, row) in &v.rows {
                        let ccval = ccval_func(&row.borrow());
                        let in_list = list_vals.iter().any(|lv| helpers::parser_vals_cmp(&ccval, &lv) == Ordering::Equal);
                        if in_list {
                            matching_rows.push(row.clone());
                        }
                    }
                }
            }
            return (*negated, matching_rows);
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = expr_to_col(&expr);
            if let Some(ci) = get_col_index_with_aliases(&col, &v.columns, aliases) {
                matching_rows.append(&mut v.get_rptrs_of_col(ci, &Value::Null));
            } else if let Some(computed) = computed {
                // if this col is a computed col, check if null and return
                if let Some(e) = computed.get(&col) {
                    let ccval_func = get_value_for_row_closure(&e, &v.columns, aliases, Some(&computed));
                    for (_ri, row) in &v.rows {
                        let ccval = ccval_func(&row.borrow());
                        if ccval.to_string() == Value::Null.to_string() {
                            matching_rows.push(row.clone());
                        }
                    }
                }
            }
            return (*negated, matching_rows);
        }
        Expr::BinaryOp {left, op, right} => {
            let (lnegated, mut lptrs) = get_rptrs_matching_constraint(left, v, aliases, computed);
            let (rnegated, mut rptrs) = get_rptrs_matching_constraint(right, v, aliases, computed);
            match op {
                BinaryOperator::And => {
                    // if both are negated or not negated, return (negated?, combo of ptrs)
                    if lnegated == rnegated {
                        matching_rows = v.intersect_rptrs(&mut lptrs, &mut rptrs);
                        return (lnegated, matching_rows);
                    } else {
                        if lnegated {
                            // only lefthandside negated, return (false, all rptrs - lptrs)
                            matching_rows = v.minus_rptrs(&mut rptrs, &mut lptrs);
                        } else {
                            // only right negated, return (false, all lptrs - rptrs)
                            matching_rows = v.minus_rptrs(&mut lptrs, &mut rptrs);
                        } 
                        return (false, matching_rows);
                    }
                }
                BinaryOperator::Or => {
                    if lnegated == rnegated {
                        matching_rows.append(&mut lptrs);
                        matching_rows.append(&mut rptrs);
                        matching_rows.sort_by(|r1, r2| helpers::parser_vals_cmp(
                                &r1.borrow()[v.primary_index], 
                                &r2.borrow()[v.primary_index]));
                        matching_rows.dedup();
                        return (lnegated, matching_rows);
                    } else {
                        if lnegated {
                            // only lefthandside negated, return (true, lptrs - rptrs)
                            matching_rows = v.minus_rptrs(&mut lptrs, &mut rptrs);
                        } else {
                            // only righthandside negated, return (left, all rptrs - lptrs)
                            matching_rows = v.minus_rptrs(&mut rptrs, &mut lptrs);
                        }
                        return (true, matching_rows);
                    }
                }
                _ => {
                    // special case: use index to perform comparisons against 
                    // fixed value on the RHS
                    if let Expr::Identifier(_) = **left {
                        if let Expr::Value(ref val) = **right {
                            if *op == BinaryOperator::Eq || *op == BinaryOperator::NotEq {
                                let (_tab, col) = expr_to_col(&left);
                                
                                if let Some(ci) = get_col_index_with_aliases(&col, &v.columns, aliases) {
                                    warn!("fastpath equal expression: {} =? {}", col, val);
                                    matching_rows.append(&mut v.get_rptrs_of_col(ci, &val));
                                    return (*op == BinaryOperator::NotEq, matching_rows);
                                } else {
                                    warn!("fastpath equal expression: checking if computed col {} =? {}", col, val);
                                    // if this col is a computed col, check if null and return
                                    if let Some(computed) = computed {
                                        if let Some(e) = computed.get(&col) {
                                            let ccval_func = get_value_for_row_closure(&e, &v.columns, aliases, Some(&computed));
                                            for (_pk, row) in &v.rows {
                                                let ccval = ccval_func(&row.borrow());
                                                let cmp = helpers::parser_vals_cmp(&ccval, &val);
                                                if (*op == BinaryOperator::NotEq && cmp != Ordering::Equal) ||
                                                    (*op == BinaryOperator::Eq && cmp == Ordering::Equal) {
                                                    matching_rows.push(row.clone());
                                                }
                                            }
                                            return (false, matching_rows);
                                        }
                                    }
                                } 
                            }
                        }
                    }
                    let left_fn = get_value_for_row_closure(&left, &v.columns, aliases, computed);
                    let right_fn = get_value_for_row_closure(&right, &v.columns, aliases, computed);

                    for (_pk, row) in &v.rows {
                        let left_val = left_fn(&row.borrow());
                        let right_val = right_fn(&row.borrow());
                        let cmp = helpers::parser_vals_cmp(&left_val, &right_val);
                        match op {
                            BinaryOperator::Eq => {
                                if cmp == Ordering::Equal {
                                    matching_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::NotEq => {
                                if cmp != Ordering::Equal {
                                    matching_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::Lt => {
                                if cmp == Ordering::Less {
                                    matching_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::Gt => {
                                if cmp == Ordering::Greater {
                                    matching_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::LtEq => {
                                if cmp != Ordering::Greater {
                                    matching_rows.push(row.clone());
                                }
                            }
                            BinaryOperator::GtEq => {
                                if cmp != Ordering::Less {
                                    matching_rows.push(row.clone());
                                }
                            }
                            _ => unimplemented!("binop constraint not supported {:?}", e),
                        }
                    }
                    return (false, matching_rows);
                }
            }
        }
        _ => unimplemented!("Constraint not supported {:?}", e),
    }
}

/* 
 * Return vectors of columns, rows, and additional computed columns/values
 */
fn get_setexpr_results(views: &HashMap<String, Rc<RefCell<View>>>, se: &SetExpr, order_by: &Vec<OrderByExpr>) 
    -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), std::io::Error> {
    match se {
        SetExpr::Select(s) => {
            if s.having != None {
                unimplemented!("No support for having queries");
            }

            let new_view = Rc::new(RefCell::new(View::new_with_cols(vec![])));
            let mut source_view : Option<Rc<RefCell<View>>> = None;
            // new name for column at index 
            let mut column_aliases : HashMap<String, usize> = HashMap::new();
            // additional columns and their values
            let mut computed_columns : HashMap<String, &Expr> = HashMap::new();
            
            // special case: we're getting results from only this view
            // INVARIANT: if source_view is Some, new_view does not have any rows
            // TODO fix this so that we only ever refer to one view...
            if s.from.len() == 1 && s.from[0].joins.is_empty() {
                let tab_ref = tablefactor_to_view(views, &s.from[0].relation)?;
                let tab = tab_ref.borrow();
                
                new_view.borrow_mut().primary_index = tab.primary_index;
                
                source_view = Some(tab_ref.clone());
            } else {
                // otherwise, it's a join
                // INVARIANT: new_view after a join is populated with all the rows
                /*for twj in &s.from {
                    // TODO if this is a join, how to handle indices and names?
                    // TODO don't copy unless necessary?
                    let v = tablewithjoins_to_view(views, &twj)?.borrow();
                    new_view.borrow_mut().columns.append(&mut v.columns);
                    // TODO correctly update primary index---right now there can be duplicates from
                    // different tables
                    for (k, r) in v.rows {
                        new_view.borrow_mut().rows.insert(k, r);
                    }
                }*/
            }

            // 1) compute any additional rows added by projection 
            // 2) compute aliases prior to where or order_by clause filtering 
            // 2) keep track of whether we want to return the count of rows (count)
            // 3) keep track of whether we want to return 1 for each row (select_val)
            // 4) keep track of which columns to keep for which tables (cols_to_keep)
            //
            // INVARIANT: new_view is not modified during this block
            let mut cols_to_keep = vec![];
            let mut select_val = None;
            let mut count = false;
            let mut count_alias = Ident::new("count");
            let mut columns: Vec<TableColumnDef> = new_view.borrow_mut().columns.clone();
            let table_name = new_view.borrow_mut().name.clone();

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
                            
                            let (_tab, col) = expr_to_col(expr);
                            warn!("{}: selecting {}", s, col);
                            let ci = columns.iter().position(|c| tablecolumn_matches_col(c, &col)).unwrap();
                            cols_to_keep.push(ci);
                            
                            // alias; use in WHERE will match against this alias
                            if let Some(a) = alias {
                                column_aliases.insert(a.to_string(), ci);
                            }

                        } else if let Expr::BinaryOp{..} = expr {
                            // SELECT `col1 - col2 AS alias`
                            assert!(alias.is_some());
                            let a = alias.as_ref().unwrap();
                            computed_columns.insert(a.to_string(), expr);
                            warn!("Adding to computed columns {:?}", computed_columns)
                        
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
            // and actually add these to the new view (this is the last time we'll use source_view)
            let mut rptrs_to_keep : RowPtrs;
            let source : Rc<RefCell<View>>;
            if let Some(source_view) = source_view {
                source = source_view.clone(); 
            } else {
                source = new_view;
            }
            if let Some(selection) = &s.selection {
                let (negated, mut matching_rptrs) = 
                    get_rptrs_matching_constraint(&selection, &source.borrow(), Some(&column_aliases), Some(&computed_columns));
                if negated {
                    let mut all_rptrs : RowPtrs = source.borrow().rows.iter().map(|(_pk, rptr)| rptr.clone()).collect();
                    matching_rptrs = source.borrow().minus_rptrs(&mut all_rptrs, &mut matching_rptrs);
                }
                rptrs_to_keep = matching_rptrs;
                warn!("Where: Keeping rows {:?} {:?}", selection, rptrs_to_keep);
            } else {
                rptrs_to_keep = source.borrow().rows.iter().map(|(_pk, rptr)| rptr.clone()).collect();
            }

            // fast path: return val if select val was issued
            if let Some(val) = select_val {
                let rows : RowPtrs;
                let val_row = Rc::new(RefCell::new(vec![val.clone()]));
                rows = vec![val_row; rptrs_to_keep.len()];
                
                // not sure what to put for column in this case but it's probably ok?
                columns = vec![TableColumnDef{
                    table: "".to_string(),
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

            // deal with present column aliases
            for (alias, ci) in column_aliases {
                columns[ci].column.name = Ident::new(alias);
                columns[ci].table = String::new();
            }
 
            // add the computed values to the rows with the appropriate aliases
            for (colname, expr) in computed_columns {
                warn!("Adding computed column {}", colname);
                let newcol_index = columns.len();
                cols_to_keep.push(newcol_index);
                
                columns.push(TableColumnDef{
                    table: table_name.clone(),
                    column: ColumnDef{
                        name: Ident::new(colname),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![],

                    },
                });
                
                let val_func = get_value_for_row_closure(expr, &columns, None, None);
                // XXX NOTE: WE'RE ACTUALLY MODIFYING THE ROW HERE???
                for rptr in &rptrs_to_keep {
                    let mut row = rptr.borrow_mut();
                    let val = val_func(&row);
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
                    column: ColumnDef {
                        name: count_alias,
                        data_type: DataType::Int,
                        collation:None,
                        options: vec![],
                    }
                });

                let num = Value::Number(rptrs_to_keep.len().to_string());
                for rptr in &rptrs_to_keep {
                    let mut row = rptr.borrow_mut();
                    if row.len() > newcol_index {
                        row[newcol_index] = num.clone();
                    } else {
                        row.push(num.clone());
                    }
                }
            }

            // order rows if necessary
            // do before performing projection because column ordering by may not be selected
            if order_by.len() > 0 {
                
                // TODO only support at most two order by constraints for now
                assert!(order_by.len() < 3); let orderby1 = &order_by[0];
                let (_tab, col1) = expr_to_col(&orderby1.expr);
                let ci1 = columns.iter().position(|c| tablecolumn_matches_col(c, &col1)).unwrap();
               
                if order_by.len() == 2 {
                    let orderby2 = &order_by[1];
                    let (_tab, col2) = expr_to_col(&orderby2.expr);
                    let ci2 = columns.iter().position(|c| tablecolumn_matches_col(c, &col2)).unwrap();
                    match orderby1.asc {
                        Some(false) => {
                            rptrs_to_keep.sort_by(|r1, r2| {
                                let res = helpers::parser_vals_cmp(&r2.borrow()[ci1], &r1.borrow()[ci1]);
                                if res == Ordering::Equal {
                                    match orderby2.asc {
                                        Some(false) => helpers::parser_vals_cmp(&r2.borrow()[ci2], &r1.borrow()[ci2]),
                                        Some(true) | None => helpers::parser_vals_cmp(&r1.borrow()[ci2], &r2.borrow()[ci2]),
                                    }
                                } else {
                                    res
                                }
                            });
                        }
                        Some(true) | None => {
                            rptrs_to_keep.sort_by(|r1, r2| {
                                let res = helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1]);
                                if res == Ordering::Equal {
                                    match orderby2.asc {
                                        Some(false) => helpers::parser_vals_cmp(&r2.borrow()[ci2], &r1.borrow()[ci2]),
                                        Some(true) | None => helpers::parser_vals_cmp(&r1.borrow()[ci2], &r2.borrow()[ci2]),
                                    }
                                } else {
                                    res
                                }
                            });
                        }
                    }
                } else {
                    match orderby1.asc {
                        Some(false) => {
                            rptrs_to_keep.sort_by(|r1, r2| {
                                helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1])
                            });
                            warn!("order by desc! {:?}", rptrs_to_keep);
                        }
                        Some(true) | None => {
                            warn!("before sort: order by asc! {:?}", rptrs_to_keep);
                            rptrs_to_keep.sort_by(|r1, r2| {
                                helpers::parser_vals_cmp(&r1.borrow()[ci1], &r2.borrow()[ci1])
                            });
                            warn!("order by asc! {:?}", rptrs_to_keep);
                        }
                    }
                }
            }
            warn!("setexpr select: returning {:?}", rptrs_to_keep);
            Ok((columns, rptrs_to_keep, cols_to_keep))
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
            let (mut lcols, mut left_rows, mut lcols_to_keep) = get_setexpr_results(views, &left, order_by)?;
            let (mut rcols, mut right_rows, mut rcols_to_keep) = get_setexpr_results(views, &right, order_by)?;
            lcols.append(&mut rcols);
            match op {
                // TODO primary keys / unique keys 
                SetOperator::Union => {
                    // TODO currently allowing for duplicates regardless of ALL...
                    rcols_to_keep = rcols_to_keep.iter().map(|ci| lcols_to_keep.len() + ci).collect();
                    lcols_to_keep.append(&mut rcols_to_keep);
                    left_rows.append(&mut right_rows);

                    return Ok((lcols, left_rows, lcols_to_keep));
                }
                _ => unimplemented!("Not supported set operation {}", se),
            }
        }
        SetExpr::Values(_vals) => {
            unimplemented!("Shouldn't be getting values when looking up results: {}", se); 
        }
    }
}

pub fn get_query_results(views: &HashMap<String, Rc<RefCell<View>>>, q: &Query) -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), Error> {
    let (all_cols, mut rptrs, cols_to_keep) = get_setexpr_results(views, &q.body, &q.order_by)?;
    // don't support OFFSET or fetches yet
    assert!(q.offset.is_none() && q.fetch.is_none());

    // limit
    if q.limit.is_some() {
        if let Some(Expr::Value(Value::Number(n))) = &q.limit {
            let limit = usize::from_str(n).unwrap();
            rptrs.truncate(limit);
        } else {
            unimplemented!("bad limit! {}", q);
        }
    }

    Ok((all_cols, rptrs, cols_to_keep))
}
