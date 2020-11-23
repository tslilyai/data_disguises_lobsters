use sql_parser::ast::*;
use std::collections::{HashSet, HashMap};
use std::cmp::Ordering;
use crate::{select, helpers, ghosts_map};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::io::{Error, Write};
use std::rc::Rc;
use log::{warn};
use msql_srv::{QueryResultWriter, Column, ColumnFlags};

pub type Row = Vec<Value>;
pub type RowPtrs = Vec<Rc<RefCell<Row>>>;

#[derive(Eq, PartialEq, Debug)]
pub struct HashedRowPtr(pub Rc<RefCell<Row>>);
impl Hash for HashedRowPtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.borrow().hash(state);
    }
}

#[derive(Debug, Clone)]
pub enum ViewIndex { 
    Primary(Rc<RefCell<HashMap<String, Rc<RefCell<Row>>>>>),
    Secondary(Rc<RefCell<HashMap<String, RowPtrs>>>),
}

impl ViewIndex {
    pub fn get_index_rows_of_val(&self, val: &str) -> Option<RowPtrs> {
        match self {
            ViewIndex::Primary(index) => {
                let index = index.borrow();
                match index.get(val) {
                    Some(r) => {
                        let rows = vec![r.clone()];
                        Some(rows)
                    }
                    None => None,
                }
            }
            ViewIndex::Secondary(index) => {
                let index = index.borrow();
                match index.get(val) {
                    Some(rows) => Some(rows.clone()),
                    _ => None,
                }
            }
        } 
    }
}

#[derive(Debug, Clone)]
pub struct TableColumnDef {
    pub table: String,
    pub column: ColumnDef,
}
impl TableColumnDef {
    pub fn name(&self) -> String {
        if !self.table.is_empty() {
            format!("{}.{}", self.table, self.column.name)
        } else {
            self.column.name.to_string()
        }
    } 
}

#[derive(Debug, Clone)]
pub struct View {
    pub name: String,
    // schema column definitions
    pub columns: Vec<TableColumnDef>,
    // table rows: primary key to row
    pub rows: Rc<RefCell<HashMap<String, Rc<RefCell<Row>>>>>,
    // Hashmap of secondary indexes (by column): column val(string) to row pointers
    pub indexes: HashMap<String, Rc<RefCell<HashMap<String, RowPtrs>>>>,
    // Primary key column position
    pub primary_index: usize,
    // optional autoinc column (index) and current value
    // invariant: autoinc_col.1 is always the *next* value that should be used
    pub autoinc_col: Option<(usize, u64)>,
}

pub fn view_cols_rows_to_answer_rows<W: Write>(cols: &Vec<TableColumnDef>, rows: RowPtrs, cols_to_keep: &Vec<usize>, 
                                               results: QueryResultWriter<W>)
    -> Result<(), mysql::Error> 
{
    let cols : Vec<_> = cols_to_keep.iter()
        .map(|&ci| {
            let c = &cols[ci];
            let mut flags = ColumnFlags::empty();
            for opt in &c.column.options {
                match opt.option {
                    ColumnOption::AutoIncrement => flags.insert(ColumnFlags::AUTO_INCREMENT_FLAG),
                    ColumnOption::NotNull => flags.insert(ColumnFlags::NOT_NULL_FLAG),
                    ColumnOption::Unique {is_primary} => {
                        if is_primary {
                            flags.insert(ColumnFlags::PRI_KEY_FLAG)
                        } else {
                            flags.insert(ColumnFlags::UNIQUE_KEY_FLAG)
                        }
                    }
                    _ => (),
                }
            }
            Column {
                table : c.table.clone(),
                column : c.column.name.to_string(),
                coltype : helpers::get_parser_coltype(&c.column.data_type),
                colflags: flags,
            }
        })
        .collect();
    let mut writer = results.start(&cols)?;
    for row in &rows {
        let row = row.borrow();
        for ci in cols_to_keep {
            writer.write_col(helpers::parser_val_to_common_val(&row[*ci]))?;
        }
        writer.end_row()?;
    }
    writer.finish()?;
    Ok(())
}

impl View {
    pub fn insert_row(&mut self, row: Rc<RefCell<Row>>) {
        self.rows.borrow_mut().insert(row.borrow()[self.primary_index].to_string(), row.clone());
    }

    pub fn minus_rptrs(&self, a: &mut RowPtrs, b: &mut RowPtrs) -> RowPtrs {
        a.sort_by(|r1, r2| helpers::parser_vals_cmp(&r1.borrow()[self.primary_index], &r2.borrow()[self.primary_index]));
        b.sort_by(|r1, r2| helpers::parser_vals_cmp(&r1.borrow()[self.primary_index], &r2.borrow()[self.primary_index]));
        let mut minus = vec![];
        let mut b_iter = b.iter();
        if let Some(mut current_b) = b_iter.next() {
            for current_a in a {
                while helpers::parser_vals_cmp(
                        &current_b.borrow()[self.primary_index], 
                        &current_a.borrow()[self.primary_index]) 
                    == Ordering::Less 
                {
                    current_b = match b_iter.next() {
                        Some(current_b) => current_b,
                        None => return minus,
                    };
                }
                // only push current_a if the next value in b was not equivalent
                if helpers::parser_vals_cmp(&current_b.borrow()[self.primary_index], &current_a.borrow()[self.primary_index]) 
                    != Ordering::Equal
                {
                    minus.push(current_a.clone());
                }
            }
        }
        minus
    }

    pub fn intersect_rptrs(&self, a: &mut RowPtrs, b: &mut RowPtrs) -> RowPtrs {
        //a.sort_by(|r1, r2| helpers::parser_vals_cmp(&r1.borrow()[self.primary_index], &r2.borrow()[self.primary_index]));
        //b.sort_by(|r1, r2| helpers::parser_vals_cmp(&r1.borrow()[self.primary_index], &r2.borrow()[self.primary_index]));
        let mut intersection = vec![];
        let mut b_iter = b.iter();
        if let Some(mut current_b) = b_iter.next() {
            for current_a in a {
                while helpers::parser_vals_cmp(
                        &current_b.borrow()[self.primary_index], 
                        &current_a.borrow()[self.primary_index]) 
                    == Ordering::Less 
                {
                    current_b = match b_iter.next() {
                        Some(current_b) => current_b,
                        None => return intersection,
                    };
                }
                if helpers::parser_vals_cmp(&current_b.borrow()[self.primary_index], &current_a.borrow()[self.primary_index]) 
                    == Ordering::Equal
                {
                    intersection.push(current_b.clone());
                }
            }
        }
        intersection
    }

    pub fn new_with_cols(columns: Vec<TableColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: Rc::new(RefCell::new(HashMap::new())),
            indexes: HashMap::new(),
            primary_index: 0,
            autoinc_col: None,
        }
    }

    pub fn new(name: String, view_columns: &Vec<ColumnDef>, indexes: &Vec<IndexDef>, constraints: &Vec<TableConstraint>) -> Self {
        // create autoinc column if doesn't exist
        let autoinc_col = match view_columns.iter().position(
            |c| c.options
            .iter()
            .any(|opt| opt.option == ColumnOption::AutoIncrement)
        ) {
            Some(ci) => Some((ci, 1)),
            None => None,
        };

        // save where the primary index is
        let mut primary_index = None;
        // create indexes for any explicit indexes
        let mut indexes_map = HashMap::new();
        if !indexes.is_empty() {
            for i in indexes {
                for key in &i.key_parts {
                    // TODO just create a separate index for each key part for now rather than
                    // nesting
                    indexes_map.insert(key.to_string(), Rc::new(RefCell::new(HashMap::new())));
                    warn!("{}: Created index for column {}", name, key.to_string());
                }
            }
        }; 

        // add an index for any unique column
        for ci in 0..view_columns.len() {
            let c = &view_columns[ci];
            for opt in &c.options {
                if let ColumnOption::Unique{is_primary} = opt.option {
                    if is_primary {
                        primary_index = Some(ci);
                    } else {
                        indexes_map.insert(c.name.to_string(), Rc::new(RefCell::new(HashMap::new())));
                        warn!("{}: Created unique index for column {}", name, c.name.to_string());
                    }
                    break;
                }
            }
        }
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique{columns, is_primary, ..} =>  {
                    if *is_primary {
                        assert!(columns.len() == 1);
                        let ci = view_columns.iter().position(|vc| vc.name.to_string() == columns[0].to_string()).unwrap();
                        primary_index = Some(ci);
                    } else {
                        for c in columns {
                            indexes_map.insert(c.to_string(), Rc::new(RefCell::new(HashMap::new())));
                            warn!("{}: Created unique index for column {}", name, c.to_string());
                        }
                    }
                }
                _ => (),
            }
        }
        let view = View {
            name: name.clone(),
            columns: view_columns.iter()
                .map(|c| TableColumnDef{ table: name.clone(), column: c.clone() })
                .collect(),
            rows: Rc::new(RefCell::new(HashMap::new())),
            indexes: indexes_map,
            primary_index: primary_index.unwrap(),
            autoinc_col: autoinc_col,
        };
        warn!("created new view {:?}", view);
        view
    }

    pub fn get_index_of_view(&self, col_name: &str) -> Option<ViewIndex> {
        if let Some(i) = self.indexes.get(col_name) {
            warn!("Found index of view {} for col {}", self.name, col_name);
            return Some(ViewIndex::Secondary(i.clone()));
        } else if select::tablecolumn_matches_col(&self.columns[self.primary_index], col_name) {
            warn!("Found primary index of view {} for col {}", self.name, col_name);
            return Some(ViewIndex::Primary(self.rows.clone()));
        }
        warn!("No index of view {} for col {}", self.name, col_name);
        None
    }

    pub fn get_rptrs_of_col(&self, col_index: usize, col_val: &str) -> RowPtrs {
        let mut rptrs : RowPtrs = vec![];
        if let Some(index) = self.indexes.get(&self.columns[col_index].column.name.to_string()) {
            if let Some(rptrs) = index.borrow().get(col_val) {
                warn!("get_rows: found rows for col {} val {}!", self.columns[col_index].name(), col_val);
                return rptrs.clone();
            } 
            return rptrs;
        }
        warn!("get_rows: no index for col {} val {}!", self.columns[col_index].name(), col_val);
        for (_pk, row) in self.rows.borrow().iter() {
            if row.borrow()[col_index].to_string() == col_val {
                rptrs.push(row.clone());
            }
        }
        warn!("get_rows: {} returns {:?}", self.name, rptrs);
        rptrs
    }
    
    pub fn insert_into_index(&mut self, row: Rc<RefCell<Row>>, col_index: usize) {
        if let Some(index) = self.indexes.get_mut(&self.columns[col_index].column.name.to_string()) {
            let col_val = &row.borrow()[col_index];
            warn!("INDEX {}: inserting {}) into index", self.columns[col_index].name(), col_val);
            // insert into the new indexed ris 
            let mut index = index.borrow_mut();
            if let Some(rptrs) = index.get_mut(&col_val.to_string()) {
                rptrs.push(row.clone());
            } else {
                let mut rptrs = Vec::new();
                rptrs.push(row.clone());
                index.insert(col_val.to_string(), rptrs);
            }
        }
    }
 
    pub fn update_index(&mut self, rptr: Rc<RefCell<Row>>, col_index: usize, col_val: Option<&Value>) {
        let row = rptr.borrow();
        let old_val = &row[col_index];
        let pk = self.primary_index;
        warn!("{}: updating {:?} from {:?}", self.columns[col_index].name(), col_val, old_val);

        if let Some(index) = self.indexes.get_mut(&self.columns[col_index].column.name.to_string()) {
            // get the old indexed row_indexes if they existed for this column value
            // remove this row!
            if let Some(old_ris) = index.borrow_mut().get_mut(&old_val.to_string()) {
                warn!("{}: removing {:?} (row {:?}) from ris {:?}", self.columns[col_index].name(), old_val, rptr, old_ris);
                old_ris.retain(|oldrp| oldrp.borrow()[pk] != rptr.borrow()[pk]);
            }
            // insert into the new indexed ris but only if we are updating to a new
            // value (otherwise we're just deleting)
            if let Some(col_val) = col_val {
                let mut index = index.borrow_mut();
                if let Some(new_ris) = index.get_mut(&col_val.to_string()) {
                    new_ris.push(rptr.clone());
                } else {
                    let mut rptrs = Vec::new();
                    rptrs.push(rptr.clone());
                    index.insert(col_val.to_string(), rptrs);
                }
            }
        }
    }
}


pub struct Views {
    views: HashMap<String, Rc<RefCell<View>>>,
}

impl Views {
    pub fn new() -> Self {
        Views {
            views: HashMap::new(),
        }
    }
    
    pub fn get_view(&self, name: &str) -> Option<Rc<RefCell<View>>> {
        match self.views.get(name) {
            None => None,
            Some(v) => Some(v.clone()),
        }
    }

    pub fn add_view(&mut self, name: String, columns: &Vec<ColumnDef>, indexes: &Vec<IndexDef>, constraints: &Vec<TableConstraint>) {
        self.views.insert(name.clone(), Rc::new(RefCell::new(View::new(name, columns, indexes, constraints))));
    }

    pub fn remove_views(&mut self, names: &Vec<ObjectName>) {
        for name in names {
            self.views.remove(&name.to_string());
        }
    }
    
    pub fn query_iter(&self, query: &Query) -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), Error> {
        select::get_query_results(&self.views, query)
    }
 
    pub fn insert(&mut self, table_name: &ObjectName, columns: &Vec<Ident>, val_rows: &RowPtrs) -> Result<(), Error> {
        let mut view = self.views.get(&table_name.to_string()).unwrap().borrow_mut();

        warn!("{}: insert rows {:?} into {}", view.name, val_rows, table_name);
        // initialize the rows to insert
        // insert rows with non-specified columns set as NULL for now (TODO)
        let mut insert_rows = vec![];
        for _ in 0..val_rows.len() {
            insert_rows.push(Rc::new(RefCell::new(vec![Value::Null; view.columns.len()])));
        }
        let mut cis : Vec<usize>;
        if columns.is_empty() {
            // update all columns
            cis = (0..columns.len()).collect();
        } else {
            cis = columns.iter()
                .map(|c| view.columns.iter().position(|vc| vc.column.name == *c).unwrap())
                .collect();
        }
         
        // if there is an autoincrement column, we should 
        // (1) see if the table is actually inserting a value for that column (found) 
        // (2) update the self.latest_uid appropriately and insert the value for that column
        if let Some(autoinc_col) = view.autoinc_col {
            let col_index = autoinc_col.0;
            let col = &view.columns[autoinc_col.0];
            let id_val = autoinc_col.1;
            let num_insert : u64 = val_rows.len() as u64;
            
            let mut found = false;
            for c in columns {
                if *c == col.column.name {
                    // get the values of the uid col being inserted, update autoinc
                    let mut max = id_val;
                    for row in val_rows {
                        let row = row.borrow();
                        match &row[col_index] {
                            Value::Number(n) => {
                                let n = n.parse::<u64>().unwrap();
                                // only update if it's a UID!!!
                                if n < ghosts_map::GHOST_ID_START {
                                    max = u64::max(max, n);
                                }
                            }
                            _ => (),
                        }
                    }
                    // TODO ensure self.latest_uid never goes above GID_START
                    view.autoinc_col = Some((col_index, max+1));
                    found = true;
                    break;
                }
            }
            if !found {
                // put self.latest_uid + N as the id col values 
                let cur_uid = id_val; 
                for i in 0..num_insert {
                    let mut row = val_rows[i as usize].borrow_mut();
                    row.push(Value::Number(format!("{}", cur_uid + i)));
                }
                
                // add id column to update
                // first, ensure that it wasn't included to begin with (if columns were empty)
                cis.retain(|&ci| ci != col_index);

                // then add it to the end!
                cis.push(col_index);
                view.autoinc_col = Some((col_index, id_val + val_rows.len() as u64));
            }
        }

        // update with the values to insert
        for (i, row) in val_rows.iter().enumerate() {
            let mut irow = insert_rows[i].borrow_mut();
            warn!("views::insert: insert_rows {} is {:?}", i, irow);
            let row = row.borrow();
            for (val_index, ci) in cis.iter().enumerate() {
                // update the right column ci with the value corresponding 
                // to that column to update
                warn!("views::insert: setting insert_row col {} to {}", ci, row[val_index]);
                irow[*ci] = row[val_index].clone();
            }
            warn!("views::insert: insert_rows {} is {:?}", i, irow);
        }
        warn!("views::insert: insert_rows are {:?}", insert_rows);

        for row in &insert_rows {
            for ci in 0..view.columns.len() {
                // update with default (not null) values
                for opt in &view.columns[ci].column.options {
                    if let ColumnOption::Default(Expr::Value(v)) = &opt.option {
                        warn!("views::insert: Updating col {} with default value {}", view.columns[ci].name(), v);
                        if row.borrow()[ci] == Value::Null {
                            row.borrow_mut()[ci] = v.clone();
                        } 
                    }  
                    if let ColumnOption::NotNull = &opt.option {
                        assert!(row.borrow()[ci] != Value::Null);
                    }
                }

                // insert all values (even if null) into indices
                warn!("views::insert: Attempt insert into index: col {} with value {}", view.columns[ci].name(), row.borrow()[ci]);
                // make sure to actually insert into the right index!!!
                view.insert_into_index(row.clone(), ci);
            }
        }

        warn!("views::insert {}: Appending rows: {:?}", view.name, insert_rows);
        for row in insert_rows {
            view.insert_row(row);
        }
        Ok(())
    }

    pub fn update(&mut self, 
          table_name: &ObjectName, 
          assignments: &Vec<Assignment>, 
          selection: &Option<Expr>, 
          assign_vals: &Vec<Expr>) 
        -> Result<(), Error> 
    {
        let mut view = self.views.get_mut(&table_name.to_string()).unwrap().borrow_mut();
        warn!("{}: update {:?} with vals {:?}", view.name, assignments, assign_vals);

        // if the table has an autoincrement column, we should 
        // (1) see if the table is actually updating a value for that column and
        // (2) update the self.latest_uid appropriately 
        if let Some(autoinc_col) = view.autoinc_col {
            let col_index = autoinc_col.0;
            let col = &view.columns[autoinc_col.0];
            let id_val = autoinc_col.1;

            for i in 0..assignments.len() {
                if assignments[i].id == col.column.name {
                    match &assign_vals[i] {
                        Expr::Value(Value::Number(n)) => {
                            let n = n.parse::<u64>().unwrap();
                            if n < ghosts_map::GHOST_ID_START {
                                view.autoinc_col = Some((col_index, u64::max(id_val, n+1)));
                            }
                        }
                        
                        _ => (),
                    }
                    break;
                }
            }
        }

        let mut cis = vec![];
        for a in assignments {
            cis.push(view.columns.iter().position(|vc| vc.column.name == a.id).unwrap());
        }

        let mut rptrs: Option<HashSet<HashedRowPtr>> = None;
        if let Some(s) = selection {
            let (neg, matching) = select::get_rptrs_matching_constraint(s, &view, None, None);
            // we should do the inverse here, I guess...
            if neg {
                let mut all_rptrs : HashSet<HashedRowPtr> = view.rows.borrow().iter().map(|(_pk, rptr)| HashedRowPtr(rptr.clone())).collect();
                for rptr in matching {
                    all_rptrs.remove(&rptr);
                }
                rptrs = Some(all_rptrs);
            } else {
                rptrs = Some(matching);
            }
        }

        warn!("{}: update columns of indices {:?}", view.name, cis);
        // update the rows!
        for (assign_index, ci) in cis.iter().enumerate() {
            match &assign_vals[assign_index] {
                Expr::Value(v) => {
                    if let Some(ref rptrs) = rptrs {
                        for rptr in rptrs {
                            view.update_index(rptr.0.clone(), *ci, Some(&v));
                            rptr.0.borrow_mut()[*ci] = v.clone();
                        }
                    } else {
                        let mut rptrs = vec![];
                        for (_pk, rptr) in view.rows.borrow().iter() {
                            rptrs.push(rptr.clone()); 
                        };
                        for rptr in &rptrs {
                            view.update_index(rptr.clone(), *ci, Some(&v));
                            rptr.borrow_mut()[*ci] = v.clone();
                        }
                    }
                }
                _ => {
                    let assign_vals_fn = select::get_value_for_row_closure(&assign_vals[assign_index], &view.columns, None, None);
                    if let Some(ref rptrs) = rptrs {
                        for rptr in rptrs {
                            let v = assign_vals_fn(&rptr.0.borrow());
                            view.update_index(rptr.0.clone(), *ci, Some(&v));
                            rptr.0.borrow_mut()[*ci] = v.clone();
                        }
                    } else {
                        let mut rptrs = vec![];
                        for (_pk, rptr) in view.rows.borrow().iter() {
                            rptrs.push(rptr.clone()); 
                        };
                        for rptr in &rptrs {
                            let v = assign_vals_fn(&rptr.borrow());
                            view.update_index(rptr.clone(), *ci, Some(&v));
                            rptr.borrow_mut()[*ci] = v.clone();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, 
          table_name: &ObjectName, 
          selection: &Option<Expr>)
        -> Result<(), Error> 
    {
        let mut view = self.views.get(&table_name.to_string()).unwrap().borrow_mut();

        let mut rptrs: Option<HashSet<HashedRowPtr>> = None;
        if let Some(s) = selection {
            let (neg, matching) = select::get_rptrs_matching_constraint(s, &view, None, None);
            if neg {
                let mut all_rptrs : HashSet<HashedRowPtr> = view.rows.borrow().iter().map(|(_pk, rptr)| HashedRowPtr(rptr.clone())).collect();
                for rptr in matching {
                    all_rptrs.remove(&rptr);
                }
                rptrs = Some(all_rptrs);
            } else {
                rptrs = Some(matching);
            }
        }

        if let Some(ref rptrs) = rptrs {
            for rptr in rptrs {
                for ci in 0..view.columns.len() {
                    // all the row indices have to change too..
                    view.update_index(rptr.0.clone(), ci, None);
                }
                let pk = view.primary_index;
                view.rows.borrow_mut().remove(&rptr.0.borrow()[pk].to_string());
            }
        } else {
            let mut pks = vec![];
            let mut rptrs = vec![];
            for (pk, rptr) in view.rows.borrow().iter() {
                rptrs.push(rptr.clone()); 
                pks.push(pk.clone()); 
            };
            for rptr in rptrs {
                for ci in 0..view.columns.len() {
                    view.update_index(rptr.clone(), ci, None);
                }
            }
            for pk in pks {
                view.rows.borrow_mut().remove(&pk);
            }
        }
        Ok(())
    }
}
