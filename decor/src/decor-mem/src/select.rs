use crate::views::{View, TableColumnDef};
use crate::helpers;
use log::warn;
use std::collections::{HashMap, hash_set::HashSet};
use std::cmp::Ordering;
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use sql_parser::ast::*;

/*
 * Convert table name (with optional alias) to current view
 */
fn tablefactor_to_view<'a>(views: &'a HashMap<String, View>, tf: &TableFactor) -> Result<&'a View, Error> {
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
                    Ok(&t)
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

fn join_views(jo: &JoinOperator, v1: &View, v2: &View) -> Result<View, Error> {
    warn!("Joining views {} and {}", v1.name, v2.name);
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
    // TODO only do expensive copy if there is an actual join
    // TODO copy indices when joining?
    let mut joined_views = tablefactor_to_view(views, &twj.relation)?.clone();
    
    for j in &twj.joins {
        let view2 = tablefactor_to_view(views, &j.relation)?;
        joined_views = join_views(&j.join_operator, &joined_views, view2)?;
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

fn tablecolumn_matches_col(c: &TableColumnDef, col: &str) -> bool {
    c.column.name.to_string() == col || c.name() == col
}

/*
 * Turn expression into a value, one for each row in the view
 */
pub fn get_value_for_rows(e: &Expr, v: &View, 
                         aliases: Option<&HashMap<String, usize>>, 
                         computed: Option<&HashMap<String, Vec<Value>>>,
                         which_rows: Option<&Vec<usize>>) 
-> Vec<Value> {
    let ris : Vec<_> = match which_rows {
        Some(ris) => ris.clone(),
        None => (0..v.rows.len()).collect(),
    };
    let mut res = vec![];
    match e {
        Expr::Identifier(_) => {
            let (_tab, col) = expr_to_col(&e);
            warn!("Identifier column {}", col);

            let ci = match v.columns.iter().position(|c| tablecolumn_matches_col(c, &col)) {
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
                for ri in ris {
                    res.push(v.rows[ri][ci].clone());
                }
            } else {
                // if this col is a computed col, check member in list and return
                if let Some(computed) = computed {
                    if let Some(vals) = computed.get(&col) {
                        for ri in ris {
                            res.push(vals[ri].clone());
                        }
                    }
                }
            }
        }
        Expr::Value(val) => {
            for _ in ris {
                res.push(val.clone());
            }
        }
        Expr::UnaryOp{op, expr} => {
            if let Expr::Value(ref val) = **expr {
                match op {
                    UnaryOperator::Minus => {
                        let n = -1.0 * helpers::parser_val_to_f64(&val);
                        for _ in ris {
                            res.push(Value::Number(n.to_string()));
                        }
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
            let mut lval = Value::Null; 
            let mut rval = Value::Null;
            let mut lcomputed = None;
            let mut rcomputed = None;
            match &**left {
                Expr::Identifier(_) => {
                    let (_ltab, lcol) = expr_to_col(&left);
                    lindex = get_col_index_with_aliases(&lcol, &v.columns, aliases);
                    if lindex.is_none() {
                        if let Some(computed) = computed {
                            lcomputed = computed.get(&lcol);
                        }
                    }
                }
                Expr::Value(val) => {
                    lval = val.clone()
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            match &**right {
                Expr::Identifier(_) => {
                    let (_rtab, rcol) = expr_to_col(&right);
                    rindex = get_col_index_with_aliases(&rcol, &v.columns, aliases);
                    if rindex.is_none() {
                        if let Some(computed) = computed {
                            rcomputed = computed.get(&rcol);
                        }
                    }
                }
                Expr::Value(val) => {
                    rval = val.clone()
                }
                _ => unimplemented!("must be id or value: {}", e),
            }
            for ri in ris {
                if let Some(li) = lindex {
                    lval = v.rows[ri][li].clone();
                } else if let Some(lcomputed) = lcomputed {
                    lval = lcomputed[ri].clone();
                }
                if let Some(i) = rindex {
                    rval = v.rows[ri][i].clone()
                } else if let Some(rcomputed) = rcomputed {
                    rval = rcomputed[ri].clone();
                }
                match op {
                    BinaryOperator::Plus => {
                        res.push(helpers::plus_parser_vals(&lval, &rval));
                    }
                    BinaryOperator::Minus => {
                        res.push(helpers::minus_parser_vals(&lval, &rval));
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
 * Returns the indexes of the values that match the given value
 */
fn get_indices_of_values(vals: &Vec<Value>, col_vals: &Vec<Value>) -> HashSet<usize> {
    let mut ris = HashSet::new();
    for ri in 0..vals.len() {
        if col_vals.iter().any(|cv| cv.to_string() == vals[ri].to_string()) {
            ris.insert(ri);
        }
    }
    ris
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
 * returns the indices into the view rows where these rows reside
 * 
 * */
pub fn get_rows_matching_constraint(e: &Expr, v: &View, 
                                    aliases: Option<&HashMap<String, usize>>, 
                                    computed: Option<&HashMap<String, Vec<Value>>>)
    -> HashSet<usize> 
{
    let mut row_indices = HashSet::new();
    match e {
        Expr::InList { expr, list, negated } => {
            let list_vals : Vec<Value> = list.iter()
                .map(|e| match e {
                    Expr::Value(v) => v.clone(),
                    _ => unimplemented!("list can only contain values: {:?}", list),
                })
                .collect();
            let (_tab, col) = expr_to_col(&expr);
            if *negated {
                row_indices = (0..v.rows.len()).collect();
            }  

            if let Some(ci) = get_col_index_with_aliases(&col, &v.columns, aliases) {
                for lv in &list_vals {
                    for ri in v.get_row_indices_of_col(ci, lv) {
                        if *negated {
                            row_indices.remove(&ri);
                        } else {
                            row_indices.insert(ri);
                        }
                    }
                }
            } else {
                // if this col is a computed col, check member in list and return
                if let Some(computed) = computed {
                    if let Some(vals) = computed.get(&col) {
                        for ri in get_indices_of_values(&vals, &list_vals) {
                            if *negated {
                                row_indices.remove(&ri);
                            } else {
                                row_indices.insert(ri);
                            }
                        }
                    }
                }
            }
        }
        Expr::IsNull { expr, negated } => {
            let (_tab, col) = expr_to_col(&expr);
            if *negated {
                row_indices = (0..v.rows.len()).collect();
            }

            if let Some(ci) = get_col_index_with_aliases(&col, &v.columns, aliases) {
               for ri in v.get_row_indices_of_col(ci, &Value::Null) {
                    if *negated {
                        row_indices.remove(&ri);
                    } else {
                        warn!("Inserting {} into row indices!", ri);
                        row_indices.insert(ri);
                    }
                }            
            } else {
                // if this col is a computed col, check if null and return
                if let Some(computed) = computed {
                    if let Some(vals) = computed.get(&col) {
                        for ri in get_indices_of_values(&vals, &vec![Value::Null]) {
                            if *negated {
                                row_indices.remove(&ri);
                            } else {
                                row_indices.insert(ri);
                            }
                        }
                    }
                }
            }
        }
        Expr::BinaryOp {left, op, right} => {
            // TODO can split up into two fxns, one to get rows, other to get indices...
            match op {
                BinaryOperator::And => {
                    let lindices = get_rows_matching_constraint(left, v, aliases, computed);
                    let rindices = get_rows_matching_constraint(right, v, aliases, computed);
                    for i in lindices.intersection(&rindices) {
                        row_indices.insert(*i as usize);
                    }
                }
                BinaryOperator::Or => {
                    let lindices = get_rows_matching_constraint(left, v, aliases, computed);
                    let rindices = get_rows_matching_constraint(right, v, aliases, computed);
                    for i in lindices.union(&rindices) {
                        row_indices.insert(*i as usize);
                    }                
                }
                _ => {
                    // special case: use index to perform comparisons against 
                    // fixed value on the RHS
                    let mut fastpath = false;
                    if let Expr::Identifier(_) = **left {
                        if let Expr::Value(ref val) = **right {
                            if *op == BinaryOperator::Eq || *op == BinaryOperator::NotEq {
                                fastpath = true;
                                let (_tab, col) = expr_to_col(&left);
                                if *op == BinaryOperator::NotEq {
                                    row_indices = (0..v.rows.len()).collect();
                                }

                                if let Some(ci) = get_col_index_with_aliases(&col, &v.columns, aliases) {
                                    for ri in v.get_row_indices_of_col(ci, &val) {
                                        if *op == BinaryOperator::Eq {
                                            row_indices.insert(ri);
                                        } else if *op == BinaryOperator::NotEq {
                                            row_indices.remove(&ri);
                                        }
                                    }
                                } else {
                                    // if this col is a computed col, check if null and return
                                    if let Some(computed) = computed {
                                        if let Some(vals) = computed.get(&col) {
                                            for ri in get_indices_of_values(&vals, &vec![val.clone()]) {
                                                if *op == BinaryOperator::NotEq{
                                                    row_indices.remove(&ri);
                                                } else {
                                                    row_indices.insert(ri);
                                                }
                                            }
                                        }
                                    }
                                }
                            } 
                        }
                    }
                    if !fastpath {
                        let left_vals = get_value_for_rows(&left, v, aliases, computed, None);
                        let right_vals = get_value_for_rows(&right, v, aliases, computed, None);
                        for i in 0..v.rows.len() {
                            let cmp = helpers::parser_vals_cmp(&left_vals[i], &right_vals[i]);
                            match op {
                                BinaryOperator::Eq => {
                                    if cmp == Ordering::Equal {
                                        row_indices.insert(i);
                                    }
                                }
                                BinaryOperator::NotEq => {
                                    if cmp != Ordering::Equal {
                                        row_indices.insert(i);
                                    }
                                }
                                BinaryOperator::Lt => {
                                    if cmp == Ordering::Less {
                                        row_indices.insert(i);
                                    }
                                }
                                BinaryOperator::Gt => {
                                    if cmp == Ordering::Greater {
                                        row_indices.insert(i);
                                    }
                                }
                                BinaryOperator::LtEq => {
                                    if cmp != Ordering::Greater {
                                        row_indices.insert(i);
                                    }
                                }
                                BinaryOperator::GtEq => {
                                    if cmp != Ordering::Less {
                                        row_indices.insert(i);
                                    }
                                }
                                _ => unimplemented!("Constraint not supported {}", e),
                            }
                        }
                    }
                }
            }
        }
        _ => unimplemented!("Constraint not supported {}", e),
    }
    warn!("Get rows matching constraint: {:?}, {:?}", e, row_indices);
    row_indices
}

fn get_setexpr_results(views: &HashMap<String, View>, se: &SetExpr, order_by: &Vec<OrderByExpr>) -> Result<View, Error> {
    match se {
        SetExpr::Select(s) => {
            let mut new_view = View::new_with_cols(vec![]);
            if s.having != None {
                unimplemented!("No support for having queries");
            }

            let mut source_view : Option<&View> = None;
            // new name for column at index 
            let mut column_aliases : HashMap<String, usize> = HashMap::new();
            // additional columns and their values
            let mut computed_columns : HashMap<String, Vec<Value>> = HashMap::new();
            
            // special case: we're getting results from only this view
            // INVARIANT: if source_view is Some, new_view does not have any rows
            if s.from.len() == 1 && s.from[0].joins.is_empty() {
                source_view = Some(tablefactor_to_view(views, &s.from[0].relation)?);
            
                // TODO if this is a join, how to handle indices and names?
                // this only works if there is only one table...
                new_view.name = source_view.unwrap().name.clone();
                new_view.columns = source_view.unwrap().columns.clone();

            } else {
                // otherwise, it's a join
                // INVARIANT: new_view after a join is populated with all the rows
                for twj in &s.from {
                    let mut v = tablewithjoins_to_view(views, &twj)?;
                    new_view.columns.append(&mut v.columns);
                    new_view.rows.append(&mut v.rows);
                }
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
            for proj in &s.projection {
                match proj {
                    SelectItem::Wildcard => {
                        // only support wildcards if there are no other projections...
                        assert!(s.projection.len() == 1);
                        cols_to_keep = (0..new_view.columns.len()).collect();
                    },
                    SelectItem::Expr {expr, alias} => {
                        // SELECT 1...
                        if let Expr::Value(v) = expr {
                            
                            select_val = Some(v);
                        }

                        if let Expr::QualifiedWildcard(ids) = expr {
                            // SELECT `tablename.*`: keep all columns of this table
                            
                            let table_to_select = ids.last().unwrap().to_string();
                            for (i, c) in new_view.columns.iter().enumerate() {
                                if c.table == table_to_select {
                                    cols_to_keep.push(i);
                                }
                            } 
                        } else if let Expr::Identifier(_ids) = expr {
                            // SELECT `tablename.columname` 
                            
                            let (_tab, col) = expr_to_col(expr);
                            warn!("{}: selecting {}", s, col);
                            let ci = new_view.columns.iter().position(|c| tablecolumn_matches_col(c, &col)).unwrap();
                            cols_to_keep.push(ci);
                            
                            // alias; use in WHERE will match against this alias
                            if let Some(a) = alias {
                                column_aliases.insert(a.to_string(), ci);
                                
                            }

                        } else if let Expr::BinaryOp{..} = expr {
                            // SELECT `col1 - col2 AS alias`
                            assert!(alias.is_some());
                            let a = alias.as_ref().unwrap();

                            // this selects using the indices from the original view, if any exist
                            if let Some(source_view) = source_view {
                                let vals = get_value_for_rows(expr, source_view, None, None, None);
                                computed_columns.insert(a.to_string(), vals);
                            } else {
                                // otherwise just get the value from the (joined) new view
                                let vals = get_value_for_rows(expr, &new_view, None, None, None);
                                computed_columns.insert(a.to_string(), vals);
                            }

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
            if let Some(selection) = &s.selection {
                
                let ris_to_keep : HashSet<usize>;
                if let Some(source_view) = source_view {
                    ris_to_keep = get_rows_matching_constraint(&selection, source_view, Some(&column_aliases), Some(&computed_columns));
                } else {
                    ris_to_keep = get_rows_matching_constraint(&selection, &new_view, Some(&column_aliases), Some(&computed_columns));
                }
                
                warn!("Where: Keeping rows {:?} {:?}", selection, ris_to_keep);
                let mut rows_to_keep : Vec<Vec<Value>> = vec![];
                for ri in ris_to_keep {
                    if let Some(source_view) = source_view {
                        rows_to_keep.push(source_view.rows[ri].clone());
                    } else {
                        rows_to_keep.push(new_view.rows[ri].clone());
                    }
                }
                new_view.rows = rows_to_keep;

                // add the computed values to the rows with the appropriate aliases
                for (colname, vals) in computed_columns {
                    new_view.columns.push(TableColumnDef{
                        table: new_view.name.clone(),
                        column: ColumnDef{
                            name: Ident::new(colname),
                            data_type: DataType::Int,
                            collation: None,
                            options: vec![],

                        },
                    });
                    for ri in 0..new_view.rows.len() {
                        new_view.rows[ri].push(vals[ri].clone());
                    }   
                }

                // deal with aliases
                for (alias, ci) in column_aliases {
                    new_view.columns[ci].column.name = Ident::new(alias);
                    new_view.columns[ci].table = String::new();
                }
            } 
            // no selection, so we just copy all the rows from the source (if we haven't already
            // via the join)
            else if let Some(source_view) = source_view {
                new_view.rows = source_view.rows.clone();
            }
            
            // return val if select val was issued 
            if let Some(v) = select_val {
                for r in &mut new_view.rows {
                    *r = vec![v.clone()];
                }
                // not sure what to put for column in this case but it's probably ok?
                new_view.columns = vec![TableColumnDef{
                    table: "".to_string(),
                    column: ColumnDef {
                        name: Ident::new(""),
                        data_type: DataType::Int,
                        collation: None,
                        options: vec![],
                    }
                }];
                return Ok(new_view)
            }

            // add the count of selected columns as a column if it were projected
            if count {
                let count = new_view.rows.len();
                for r in &mut new_view.rows {
                    r.push(Value::Number(count.to_string()));
                }
                new_view.columns.push(TableColumnDef{
                    table:new_view.name.clone(),
                    column: ColumnDef {
                        name: count_alias,
                        data_type: DataType::Int,
                        collation:None,
                        options: vec![],
                    }
                });
            }

            // order rows if necessary
            // do before performing projection because column ordering by may not be selected
            if order_by.len() > 0 {
                
                // TODO only support at most two order by constraints for now
                assert!(order_by.len() < 3);
                let orderby1 = &order_by[0];
                let (_tab, col1) = expr_to_col(&orderby1.expr);
                let ci1 = new_view.columns.iter().position(|c| tablecolumn_matches_col(c, &col1)).unwrap();
               
                if order_by.len() == 2 {
                    let orderby2 = &order_by[1];
                    let (_tab, col2) = expr_to_col(&orderby2.expr);
                    let ci2 = new_view.columns.iter().position(|c| tablecolumn_matches_col(c, &col2)).unwrap();
                    match orderby1.asc {
                        Some(false) => {
                            new_view.rows.sort_by(|r1, r2| {
                                let res = helpers::parser_vals_cmp(&r2[ci1], &r1[ci1]);
                                if res == Ordering::Equal {
                                    match orderby2.asc {
                                        Some(false) => helpers::parser_vals_cmp(&r2[ci2], &r1[ci2]),
                                        Some(true) | None => helpers::parser_vals_cmp(&r1[ci2], &r2[ci2]),
                                    }
                                } else {
                                    res
                                }
                            });
                        }
                        Some(true) | None => {
                            new_view.rows.sort_by(|r1, r2| {
                                let res = helpers::parser_vals_cmp(&r2[ci1], &r1[ci1]);
                                if res == Ordering::Equal {
                                    match orderby2.asc {
                                        Some(false) => helpers::parser_vals_cmp(&r2[ci2], &r1[ci2]),
                                        Some(true) | None => helpers::parser_vals_cmp(&r1[ci2], &r2[ci2]),
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
                            new_view.rows.sort_by(|r1, r2| {
                                helpers::parser_vals_cmp(&r2[ci1], &r1[ci1])
                            });
                        }
                        Some(true) | None => {
                            new_view.rows.sort_by(|r1, r2| {
                                helpers::parser_vals_cmp(&r1[ci1], &r2[ci1])
                            });
                        }
                    }
                }
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
            warn!("columns {:?}", new_view.columns);

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
            let left_view = get_setexpr_results(views, &left, order_by)?;
            let right_view = get_setexpr_results(views, &right, order_by)?;
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
    // XXX ORDER BY not supported for union/except/intersect atm
    // order_by: do first because column ordering by may not be selected
    /*if q.order_by.len() > 0 {
        // only support one order by constraint for now
        assert!(q.order_by.len() < 2);
        let orderby = &q.order_by[0];
        let (_tab, col) = expr_to_col(&orderby.expr);
        let ci = new_view.columns.iter().position(|c| tablecolumn_matches_col(c, &col));
        match ci {
            None => {
                return Err(Error::new(ErrorKind::Other, format!("No matching column for order by: {}", q)));
            }
            Some(ci) => 
            match orderby.asc {
                Some(false) => {
                    new_view.rows.sort_by(|r1, r2| helpers::parser_vals_cmp(&r2[ci], &r1[ci]));
                }
                Some(true) | None => {
                    new_view.rows.sort_by(|r1, r2| helpers::parser_vals_cmp(&r1[ci], &r2[ci]));
                }
            }
        }
    }*/

    let mut new_view = get_setexpr_results(views, &q.body, &q.order_by)?;
    // don't support OFFSET or fetches yet
    assert!(q.offset.is_none() && q.fetch.is_none());

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
