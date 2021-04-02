use sql_parser::ast::*;
use std::collections::{HashSet, HashMap};
use crate::{select, helpers, graph, guises};
use crate::types::*;
use std::io::{Error, Write};
use std::cmp::Ordering;
use std::*;
use log::{warn};
use msql_srv::{QueryResultWriter, Column, ColumnFlags};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub enum ViewIndex { 
    Primary(Rc<RefCell<HashMap<String, Rc<RefCell<Row>>>>>, usize),
    Secondary(Rc<RefCell<HashMap<String, HashedRowPtrs>>>),
}

impl ViewIndex {
    pub fn get_index_rows_of_val(&self, val: &str) -> Option<HashedRowPtrs> {
        match self {
            ViewIndex::Primary(index, pki) => {
                let index = index.borrow();
                match index.get(val) {
                    Some(r) => {
                        let mut rows = HashSet::new();
                        rows.insert(HashedRowPtr::new(r.clone(), *pki));
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumnDef {
    pub table: String,
    pub colname: String,
    pub fullname: String,
    pub column: ColumnDef,
}

#[derive(Debug, Clone)]
pub struct View {
    pub name: String,
    // schema column definitions
    pub columns: Vec<TableColumnDef>,
    // table rows: primary key to row
    pub rows: Rc<RefCell<HashMap<String, Rc<RefCell<Row>>>>>,
    // Hashmap of secondary indexes (by column): column val(string) to row pointers
    pub indexes: HashMap<String, Rc<RefCell<HashMap<String, HashedRowPtrs>>>>,
    // Primary key column position
    pub primary_index: usize,
    // optional autoinc column (index) and current value
    // invariant: autoinc_col.1 is always the *next* value that should be used
    pub autoinc_col: Option<(usize, u64)>,
    // columns that hold pointers to parent keys + parent view name
    pub parent_cols: Vec<(usize, String)>,
}

pub fn view_cols_rows_to_answer_rows<W: Write>(
    cols: &Vec<TableColumnDef>, 
    rows: RowPtrs, 
    cols_to_keep: &Vec<usize>, 
    results: QueryResultWriter<W>)
    -> Result<(), mysql::Error> 
{
    let start = time::Instant::now();
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
    let dur = start.elapsed();
    warn!("view_cols_to_answer_rows: {}us", dur.as_micros());
    Ok(())
}

impl View {
    pub fn insert_row(&mut self, row: Rc<RefCell<Row>>) {
        self.rows.borrow_mut().insert(row.borrow()[self.primary_index].to_string(), row.clone());
    }
    
    pub fn get_rows_of_ids(&self, ids: &Vec<u64>) -> RowPtrs {
        ids.iter().map(|id| self.get_row_of_id(*id)).collect()
    }
 
    pub fn get_row_of_id(&self, id: u64) -> RowPtr {
        match self.rows.borrow().get(&id.to_string()) {
            None => unimplemented!("View {}: No row for id {}", self.name, id),
            Some(idrow) => idrow.clone(),
        }
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
            parent_cols: vec![],
        }
    }

    pub fn new(name: String, 
               view_columns: &Vec<ColumnDef>, 
               indexes: &Vec<IndexDef>, 
               constraints: &Vec<TableConstraint>,
               parent_cols: &Vec<(usize, String)>) 
        -> Self 
    {
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
                        warn!("{}: Created primary index for column {}", name, c.name);
                        primary_index = Some(ci);
                    } else {
                        indexes_map.insert(c.name.to_string(), Rc::new(RefCell::new(HashMap::new())));
                        warn!("{}: Created unique index for column {}", name, c.name);
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
                .map(|c| TableColumnDef{ 
                    table: name.clone(), 
                    colname: c.name.to_string(),
                    fullname: format!("{}.{}", name, c.name),
                    column: c.clone() })
                .collect(),
            rows: Rc::new(RefCell::new(HashMap::new())),
            indexes: indexes_map,
            primary_index: primary_index.unwrap(),
            autoinc_col: autoinc_col,
            parent_cols: parent_cols.clone(),
        };
        warn!("created new view {:?}", view);
        view
    }

    pub fn get_index_of_view(&self, col_name: &str) -> Option<ViewIndex> {
        if let Some(i) = self.indexes.get(col_name) {
            warn!("Found index of view {} for col {}", self.name, col_name);
            return Some(ViewIndex::Secondary(i.clone()));
        } else if helpers::tablecolumn_matches_col(&self.columns[self.primary_index], col_name) {
            warn!("Found primary index of view {} for col {}", self.name, col_name);
            return Some(ViewIndex::Primary(self.rows.clone(), self.primary_index));
        }
        warn!("No index of view {} for col {}", self.name, col_name);
        None
    }

    pub fn is_indexed_col(&self, col_index: usize) -> bool {
        col_index == self.primary_index || self.indexes.get(&self.columns[col_index].colname.to_string()).is_some()
    }

    pub fn get_indexed_rptrs_of_col(&self, col_index: usize, col_val: &str) -> Option<HashedRowPtrs> {
        let mut hs = HashSet::with_capacity(1);
        if col_index == self.primary_index {
            match self.rows.borrow().get(col_val) {
                Some(r) => {
                    warn!("get rptrs of col: found 1 primary row for col {} val {}!", self.columns[col_index].fullname, col_val);
                    hs.insert(HashedRowPtr::new(r.clone(), self.primary_index));
                }
                None => {
                    warn!("get rptrs of primary: no rows for col {} val {}!", self.columns[col_index].fullname, col_val);
                }
            }
            return Some(hs);
        } else if let Some(index) = self.indexes.get(&self.columns[col_index].colname.to_string()) {
            if let Some(rptrs) = index.borrow().get(col_val) {
                warn!("get rptrs of col: found {} rows for col {} val {}!", rptrs.len(), self.columns[col_index].fullname, col_val);
                return Some(rptrs.clone());
            } else {
                warn!("get rptrs of col: no rows for col {} val {}!", self.columns[col_index].fullname, col_val);
                return Some(hs);
            }
        } 
        None
    }

    pub fn get_primary_rptr_of_val(&self, val: &str) -> HashedRowPtr {
        match self.rows.borrow().get(val) {
            Some(r) => {
                warn!("get primary_rptr_of_val: found 1 primary row val {}!", val);
                HashedRowPtr::new(r.clone(), self.primary_index)
            }
            None => unimplemented!("primary rptr value must exist for parent!"),
        }
    }
 
    pub fn get_rptrs_of_col(&self, col_index: usize, col_val: &str, all_rptrs: &mut HashSet<HashedRowPtr>) {
        let start = time::Instant::now();
        if col_index == self.primary_index {
            match self.rows.borrow().get(col_val) {
                Some(r) => {
                    warn!("get rptrs of col: found 1 primary row for col {} val {}!", self.columns[col_index].fullname, col_val);
                    all_rptrs.insert(HashedRowPtr::new(r.clone(), self.primary_index));
                }
                None => {
                    warn!("get rptrs of primary: no rows for col {} val {}!", self.columns[col_index].fullname, col_val);
                }
            }
        } else if let Some(index) = self.indexes.get(&self.columns[col_index].colname.to_string()) {
            if let Some(rptrs) = index.borrow().get(col_val) {
                warn!("get rptrs of col: found {} rows for col {} val {}!", rptrs.len(), self.columns[col_index].fullname, col_val);
                all_rptrs.extend(rptrs.clone());
            } else {
                warn!("get rptrs of col: no rows for col {} val {}!", self.columns[col_index].fullname, col_val);
            }
        } else {
            warn!("get rptrs of col: no index for col {} val {}!", self.columns[col_index].fullname, col_val);
            for (_pk, row) in self.rows.borrow().iter() {
                if row.borrow()[col_index].to_string() == col_val {
                    all_rptrs.insert(HashedRowPtr::new(row.clone(), self.primary_index));
                }
            }
        }
        warn!("get_rptrs_of_col: {} returns {:?}", self.name, all_rptrs);
        let dur = start.elapsed();
        warn!("get rptrs of col {} took: {}us", col_val, dur.as_micros());
    }
    
    pub fn insert_into_index(&mut self, row: Rc<RefCell<Row>>, col_index: usize) {
        let start = time::Instant::now();
        if let Some(index) = self.indexes.get_mut(&self.columns[col_index].colname.to_string()) {
            let col_val = row.borrow()[col_index].to_string();
            warn!("INDEX {}: inserting {} into index", self.columns[col_index].fullname, col_val);
            // insert into the new indexed ris 
            let mut index = index.borrow_mut();
            if let Some(rptrs) = index.get_mut(&col_val) {
                rptrs.insert(HashedRowPtr::new(row.clone(), self.primary_index));
                let dur = start.elapsed();
                warn!("insert into index {} size {} took: {}us", self.columns[col_index].fullname, index.len(), dur.as_micros());
            } else {
                let mut rptrs = HashSet::new();
                rptrs.insert(HashedRowPtr::new(row.clone(), self.primary_index));
                index.insert(col_val, rptrs);
                let dur = start.elapsed();
                warn!("insert new hashmap index {} took: {}us", self.columns[col_index].fullname, dur.as_micros());
            }
        } else {
            let dur = start.elapsed();
            warn!("no insert index {}, col {} took: {}us", self.columns[col_index].fullname, col_index, dur.as_micros());
        }
    }
 
    fn update_index_and_row(&mut self, rptr: Rc<RefCell<Row>>, col_index: usize, col_val: Option<&Value>) {
        let start = time::Instant::now();
        let old_val = rptr.borrow()[col_index].to_string();
        let mut index_len = 0;
        warn!("{}: updating {:?} to {:?}", self.columns[col_index].fullname, old_val, col_val);

        // don't actually do anything if we're just updating to the same value!
        let mut col_val_str = String::new(); 
        if let Some(v) = col_val {
            col_val_str = v.to_string();
            if col_val_str == old_val {
                let dur = start.elapsed();
                warn!("Update index {}, equal val took: {}us", self.columns[col_index].fullname, dur.as_micros());
                return;
            } else {
                // actually update row if we're changing
                rptr.borrow_mut()[col_index] = v.clone();
            }
        } else {
            // delete if we're not updating the value to anything
            let pk = self.primary_index;
            self.rows.borrow_mut().remove(&rptr.borrow()[pk].to_string());
        }

        if let Some(index) = self.indexes.get_mut(&self.columns[col_index].colname.to_string()) {
            let mut index = index.borrow_mut();
            // get the old indexed row_indexes if they existed for this column value
            // remove this row!
            if let Some(old_ris) = index.get_mut(&old_val) {
                let innerstart = time::Instant::now();
                old_ris.remove(&HashedRowPtr::new(rptr.clone(), self.primary_index));
                let durinner = innerstart.elapsed();
                warn!("{}: removing {:?} (indexlen {:?}) took {}us", 
                      self.columns[col_index].fullname, old_val, old_ris.len(), durinner.as_micros());
            }
            // insert into the new indexed ris but only if we are updating to a new
            // value (otherwise we're just deleting)
            if col_val.is_some() {
                let innerstart = time::Instant::now();
                if let Some(new_ris) = index.get_mut(&col_val_str) {
                    warn!("{}: inserting {:?} (indexlen {:?})", self.columns[col_index].fullname, col_val_str, new_ris.len());
                    new_ris.insert(HashedRowPtr::new(rptr.clone(), self.primary_index));
                } else {
                    warn!("{}: new hashset {}", self.columns[col_index].fullname, col_val_str);
                    let mut rptrs = HashSet::new();
                    rptrs.insert(HashedRowPtr::new(rptr.clone(), self.primary_index));
                    index.insert(col_val_str, rptrs);
                }
                let durinner = innerstart.elapsed();
                warn!("{}: inserting {:?} {}us", self.columns[col_index].fullname, col_val, durinner.as_micros());
            }
            index_len = index.len();
        }
        let dur = start.elapsed();
        warn!("Update index {} with {} elements took: {}us", self.columns[col_index].fullname, index_len, dur.as_micros());
    }
}


pub struct Views {
    views: HashMap<String, Rc<RefCell<View>>>,
    pub graph: graph::ObjectGraph,
}

impl Views {
    pub fn new() -> Self {
        Views {
            views: HashMap::new(),
            graph: graph::ObjectGraph::new(),
        }
    }
    
    pub fn get_row_of_id(&self, name: &str, id: u64) -> RowPtr {
        let view_ptr = self.views.get(name).unwrap();
        match view_ptr.borrow().rows.borrow().get(&id.to_string()) {
            None => unimplemented!("View {}: No row for id {}", view_ptr.borrow().name, id),
            Some(idrow) => idrow.clone(),
        }
    }
 
    pub fn get_table_names(&self) -> Vec<String> {
        self.views.iter().map(|(k, _)| k.to_string()).collect()
    }
    
    pub fn get_view(&self, name: &str) -> Option<Rc<RefCell<View>>> {
        match self.views.get(name) {
            None => None,
            Some(v) => Some(v.clone()),
        }
    }

    pub fn get_view_pi(&self, name: &str) -> usize {
        let view_ptr = self.get_view(&name).unwrap();
        let pi = view_ptr.borrow().primary_index;
        pi
    }

    pub fn get_view_columns(&self, name: &str) -> Vec<Ident> {
        let view_ptr = self.get_view(&name).unwrap();
        let columns = view_ptr.borrow().columns.iter().map(|tcd| Ident::new(tcd.colname.clone())).collect();
        columns
    }

    pub fn get_view_colname(&self, name: &str, ci: usize) -> String {
        let view_ptr = self.get_view(&name).unwrap();
        let col = view_ptr.borrow().columns[ci].colname.clone(); col
    }

    pub fn add_view(&mut self, 
                    name: String, 
                    columns: &Vec<ColumnDef>, 
                    indexes: &Vec<IndexDef>, 
                    constraints: &Vec<TableConstraint>,
                    parent_cols: &Vec<(usize,String)>) 
    {
        self.views.insert(name.clone(), Rc::new(RefCell::new(View::new(name, columns, indexes, constraints, parent_cols))));
    }

    pub fn remove_views(&mut self, names: &Vec<ObjectName>) {
        for name in names {
            self.views.remove(&name.to_string());
        }
    }
    
    pub fn query_iter(&self, query: &Query) -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), Error> {
        select::get_query_results(&self.views, query)
    }
 
    pub fn insert(&mut self, table_name: &str, columns_opt: Option<&Vec<Ident>>, val_rows: &RowPtrs) -> Result<(), Error> {
        let mut view = self.views.get(table_name).unwrap().borrow_mut();

        warn!("{}: insert rows {:?} into {}", view.name, val_rows, table_name);
        // initialize the rows to insert
        // insert rows with non-specified columns set as NULL for now (TODO)
        let mut insert_rows = vec![];
        for _ in 0..val_rows.len() {
            insert_rows.push(Rc::new(RefCell::new(vec![Value::Null; view.columns.len()])));
        }
        let mut cis : Vec<usize>;
        let columns : Vec<String>;
        if let Some(cols) = columns_opt {
            cis = cols.iter()
                .map(|c| view.columns.iter().position(|vc| vc.column.name == *c).unwrap())
                .collect();
            columns = cols.iter().map(|c| c.to_string()).collect();
        } else {
            // update all columns
            cis = (0..view.columns.len()).collect();
            columns = view.columns.iter().map(|tcd| tcd.colname.clone()).collect();
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
                if *c == col.colname {
                    // get the values of the uid col being inserted, update autoinc
                    let mut max = id_val;
                    for row in val_rows {
                        let row = row.borrow();
                        match &row[col_index] {
                            Value::Number(n) => {
                                let n = n.parse::<u64>().unwrap();
                                if !guises::is_guise_oid(n) {
                                    max = u64::max(max, n);
                                }
                            }
                            _ => (),
                        }
                    }
                    // TODO ensure self.latest_uid never goes above GID_START
                    if max > id_val {
                        view.autoinc_col = Some((col_index, max+1));
                        warn!("Autoinc col of view {} is set to {:?}", view.name, view.autoinc_col);
                    } 
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
                warn!("Autoinc col of view {} is set to {:?}", view.name, view.autoinc_col);
            }
        }

        // update with the values to insert
        for (i, row) in val_rows.iter().enumerate() {
            let mut irow = insert_rows[i].borrow_mut();
            let row = row.borrow();
            warn!("views::insert: insert_rows {} is {:?}, val row is {:?}, cis are {:?}", i, irow, row, cis);
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
                        warn!("views::insert: Updating col {} with default value {}", view.columns[ci].fullname, v);
                        if row.borrow()[ci] == Value::Null {
                            row.borrow_mut()[ci] = v.clone();
                        } 
                    }  
                    if let ColumnOption::NotNull = &opt.option {
                        assert!(row.borrow()[ci] != Value::Null);
                    }
                }

                // insert all values (even if null) into indices
                warn!("views::insert: Attempt insert into index: col {} with value {}", view.columns[ci].fullname, row.borrow()[ci]);
                // make sure to actually insert into the right index!!!
                view.insert_into_index(row.clone(), ci);
            }
            view.insert_row(row.clone());
        }

        warn!("views::insert {}: Appending rows: {:?}", view.name, insert_rows);

        // add edges to graph
        for row in insert_rows {
            for (ci, parent_table) in &view.parent_cols {
                let poid = helpers::parser_val_to_u64_opt(&row.borrow()[*ci]);
                if let Some(poid) = poid {
                    warn!("Adding graph edge {}, {}", poid, parent_table);
                    self.graph.add_edge(
                        HashedRowPtr::new(row.clone(), view.primary_index), 
                        &view.name, 
                        ObjectIdentifier {
                            table: parent_table.clone(), 
                            oid: poid,
                        }, *ci);
                }
            }
        }
        warn!("views::insert {}: finished inserting {} rows", view.name, val_rows.len());
        Ok(())
    }

    pub fn update(&mut self, 
          table_name: &str, 
          assignments: &Vec<Assignment>, 
          selection: &Option<Expr>, 
          assign_vals: &Vec<Expr>) 
        -> Result<(), Error> 
    {
        let start = time::Instant::now();
        let mut view = self.views.get_mut(table_name).unwrap().borrow_mut();
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
                            if !guises::is_guise_oid(n) {
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

        let mut rptrs: Vec<HashedRowPtr> = vec![];
        if let Some(s) = selection {
            rptrs = select::get_rptrs_matching_constraint(s, &view, &view.columns).iter().cloned().collect();
        } else {
            for (_pk, rptr) in view.rows.borrow().iter() {
                rptrs.push(HashedRowPtr::new(rptr.clone(), view.primary_index)); 
            }
        };

        warn!("{}: update columns of indices {:?}", view.name, cis);
        // update the rows!
        for (assign_index, ci) in cis.iter().enumerate() {
            match &assign_vals[assign_index] {
                Expr::Value(v) => {
                    for rptr in &rptrs {
                        // update graph
                        for (pci, parent_table) in &view.parent_cols {
                            if *pci == *ci {
                                let poid = &rptr.row().borrow()[*ci];
                                if *poid == Value::Null {
                                    continue;
                                }
                                let old_parent = ObjectIdentifier {
                                    table: parent_table.clone(),
                                    oid: helpers::parser_val_to_u64(poid),
                                };
                                let new = helpers::parser_val_to_u64_opt(&v);
                                let new_parent = match new {
                                    Some(oid) => 
                                        Some(ObjectIdentifier {
                                            table: parent_table.clone(),
                                            oid: oid,
                                        }),
                                    _ => None
                                };
                                self.graph.update_edge(
                                    &view.name, rptr.clone(), 
                                    old_parent, new_parent, *ci);
                                break;
                            }
                        }
                        // update view
                        view.update_index_and_row(rptr.row().clone(), *ci, Some(&v));
                    }
                }
                _ => {
                    let assign_vals_fn = select::get_value_for_row_closure(&assign_vals[assign_index], &view.columns);
                    for rptr in &rptrs {
                        let v = assign_vals_fn(&rptr.row().borrow());
                        // update graph
                        for (pci, parent_table) in &view.parent_cols {
                            if *pci == *ci {
                                let poid = &rptr.row().borrow()[*ci];
                                if *poid == Value::Null {
                                    continue;
                                }
                                let old_parent = ObjectIdentifier {
                                    table: parent_table.clone(),
                                    oid: helpers::parser_val_to_u64(poid),
                                };
                                let new = helpers::parser_val_to_u64_opt(&v);
                                let new_parent = match new {
                                    Some(oid) => 
                                        Some(ObjectIdentifier {
                                            table: parent_table.clone(),
                                            oid: oid,
                                        }),
                                    _ => None
                                };
                                self.graph.update_edge(
                                    &view.name, rptr.clone(), 
                                    old_parent, new_parent, *ci);
                                break;
                            }
                        }
                        // update view 
                        view.update_index_and_row(rptr.row().clone(), *ci, Some(&v));
                    }
                }
            }
       }

        let dur = start.elapsed();
        warn!("Update view {} took: {}us", view.name, dur.as_micros());
        Ok(())
    }

    pub fn update_index_and_row_of_view(&mut self, table: &str, rptr: Rc<RefCell<Row>>, ci: usize, col_val: Option<&Value>) {
        let mut view = self.views.get(table).unwrap().borrow_mut();
        for (pci, parent_table) in &view.parent_cols {
            if *pci == ci {
                let poid = &rptr.borrow()[ci];
                if *poid == Value::Null {
                    continue;
                }
                let old_parent = ObjectIdentifier {
                    table: parent_table.clone(),
                    oid: helpers::parser_val_to_u64(poid),
                };
                let new_parent = match col_val {
                    None => None,
                    Some(v) => match helpers::parser_val_to_u64_opt(&v) {
                        Some(oid) => Some(ObjectIdentifier {
                                            table: parent_table.clone(),
                                            oid: oid,
                                     }),
                        _ => None,
                    }
                };
                self.graph.update_edge(&view.name, HashedRowPtr(rptr.clone(), view.primary_index), 
                                       old_parent, new_parent, ci);
                break;
            }
        }
        // update view
        view.update_index_and_row(rptr.clone(), ci, col_val);
    }

    pub fn delete_rptrs_with_ids(&mut self, 
          table_name: &str, 
          ids: &Vec<u64>)
        -> Result<(), Error> 
    {
        let mut view = self.views.get(table_name).unwrap().borrow_mut();
        let rptrs = view.get_rows_of_ids(ids);
        let len = view.columns.len();
        for rptr in rptrs {
            for ci in 0..len {
                for (pci, parent_table) in &view.parent_cols {
                    if *pci == ci {
                        let poid = &rptr.borrow()[ci];
                        if *poid == Value::Null {
                            continue;
                        }
                        let old_parent = ObjectIdentifier {
                            table: parent_table.clone(),
                            oid: helpers::parser_val_to_u64(poid),
                        };
                        self.graph.update_edge(
                            &view.name, HashedRowPtr(rptr.clone(), view.primary_index), 
                            old_parent, None, ci);
                        break;
                    }
                }

                // all the row indices have to change too..
                view.update_index_and_row(rptr.clone(), ci, None);
            }
        }
        Ok(())
    }

    pub fn delete_rptrs(&mut self, 
          table_name: &str, 
          rptrs: &RowPtrs)
        -> Result<(), Error> 
    {
        let mut view = self.views.get(table_name).unwrap().borrow_mut();
        let len = view.columns.len();
        for rptr in rptrs {
            for ci in 0..len {
                for (pci, parent_table) in &view.parent_cols {
                    if *pci == ci {
                        let poid = &rptr.borrow()[ci];
                        if *poid == Value::Null {
                            continue;
                        }
                        let old_parent = ObjectIdentifier {
                            table: parent_table.clone(),
                            oid: helpers::parser_val_to_u64(poid),
                        };
                        self.graph.update_edge(&view.name, 
                                               HashedRowPtr::new(rptr.clone(), view.primary_index), 
                                               old_parent, None, ci);
                        break;
                    }
                }

                // all the row indices have to change too..
                view.update_index_and_row(rptr.clone(), ci, None);
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, 
          table_name: &str, 
          selection: &Option<Expr>)
        -> Result<(), Error> 
    {
        let mut view = self.views.get(table_name).unwrap().borrow_mut();

        let mut rptrs: Vec<HashedRowPtr> = vec![];
        if let Some(s) = selection {
            rptrs = select::get_rptrs_matching_constraint(s, &view, &view.columns).iter().cloned().collect();
        } else {
            for (_pk, rptr) in view.rows.borrow().iter() {
                rptrs.push(HashedRowPtr::new(rptr.clone(), view.primary_index)); 
            }
        };

        let len = view.columns.len();
        for rptr in &rptrs {
            for ci in 0..len {
                for (pci, parent_table) in &view.parent_cols {
                    if *pci == ci {
                        let poid = &rptr.row().borrow()[ci];
                        if *poid == Value::Null {
                            continue;
                        }
                        let old_parent = ObjectIdentifier {
                            table: parent_table.clone(),
                            oid: helpers::parser_val_to_u64(poid),
                        };
                        self.graph.update_edge(&view.name, rptr.clone(), old_parent, None, ci);
                        break;
                    }
                }

                // all the row indices have to change too..
                view.update_index_and_row(rptr.row().clone(), ci, None);
            }
        }
        Ok(())
    }
}