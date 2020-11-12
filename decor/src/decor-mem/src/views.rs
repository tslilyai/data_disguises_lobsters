use std::iter::FromIterator;
use sql_parser::ast::*;
use std::collections::{HashMap, hash_set::HashSet};
use crate::{select, config};
use std::io::{Error};

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
    // values stored in table
    pub rows: Vec<Vec<Value>>,
    // List of indices (by column): column val(string, and only INT type for now) to row
    pub indices: Option<HashMap<String, HashMap<String, Vec<usize>>>>,
    // optional autoinc column (index) and current value
    pub autoinc_col: Option<(usize, u64)>,
}

impl View {
    pub fn new_with_cols(columns: Vec<TableColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: vec![],
            indices: None,
            autoinc_col: None,
        }
    }

    pub fn new(name: String, columns: Vec<ColumnDef>, indexes: &Vec<IndexDef>) -> Self {
        // create autoinc column if doesn't exist
        let autoinc_col = match columns.iter().position(
            |c| c.options
            .iter()
            .any(|opt| opt.option == ColumnOption::AutoIncrement)
        ) {
            Some(ci) => Some((ci, 1)),
            None => None,
        };

        // create indices for any explicit indexes
        let mut indices = if !indexes.is_empty() {
            let mut map = HashMap::new();
            for i in indexes {
                if i.key_parts.len() > 1 {
                    unimplemented!("no multi-column indices yet");
                }
                map.insert(i.key_parts[0].to_string(), HashMap::new());
            }
            Some(map)
        } else {
            None
        };

        // add an index for any unique column
        for c in &columns {
            for opt in &c.options {
                if let ColumnOption::Unique{..} = opt.option {
                    match indices {
                        Some(ref mut is_map) => {
                            is_map.insert(c.name.to_string(), HashMap::new());
                        }
                        None => {
                            let mut map = HashMap::new();
                            map.insert(c.name.to_string(), HashMap::new());
                            indices = Some(map);
                        }
                    }
                    break;
                }
            }
        }

        View {
            name: name.clone(),
            columns: columns.iter()
                .map(|c| TableColumnDef{ table: name.clone(), column: c.clone() })
                .collect(),
            rows: vec![],
            indices: indices,
            autoinc_col: autoinc_col,
        }
    }

    pub fn contains_row(&self, r: &Vec<Value>) -> bool {
        self.rows.iter().any(|row| {
            let mut eq = true;
            for i in 0..row.len() {
                eq = eq && (row[i] == r[i]);
            }
            eq
        })
    }
    pub fn get_rows_of_col(&self, col_index: usize, val: &Value) -> Vec<Vec<Value>> {
        let mut rows = vec![];
        let mut indexed = false;
        if let Some(indices) = &self.indices {
            if let Some(index) = indices.get(&self.columns[col_index].column.name.to_string()) {
                if let Some(row_indices) = index.get(&val.to_string()) {
                    for i in row_indices {
                        rows.push(self.rows[*i].clone());
                    }
                }
                indexed = true;
            }
        } 
        if !indexed {
            for row in &self.rows {
                match &row[col_index] {
                    Value::Number(v) => if *v == val.to_string() {
                        rows.push(row.clone());
                    }
                    _ => unimplemented!("Must be a number!")
                } 
            }
        }
        rows
    }
    
    pub fn insert_into_index(&mut self, row_index: usize, col_index: usize, new_val: &Value) {
        if let Some(indices) = &mut self.indices {
            if let Some(index) = indices.get_mut(&self.columns[col_index].column.name.to_string()) {
                // insert into the new indexed row_indices 
                if let Some(new_row_indices) = index.get_mut(&new_val.to_string()) {
                    new_row_indices.push(row_index);
                } else {
                    index.insert(new_val.to_string(), vec![row_index]);
                }
            }
        }
    }
 
    pub fn update_index(&mut self, row_index: usize, col_index: usize, new_val: Option<&Value>) {
        let old_val = &self.rows[row_index][col_index];
        if let Some(indices) = &mut self.indices {
            if let Some(index) = indices.get_mut(&self.columns[col_index].column.name.to_string()) {
                // get the old indexed row_indices if they existed for this column value
                // remove this row!
                if let Some(old_row_indices) = index.get_mut(&old_val.to_string()) {
                    old_row_indices.retain(|&ri| ri != row_index);
                }
                // insert into the new indexed row_indices but only if we are updating to a new
                // value (otherwise we're just deleting)
                if let Some(new_val) = new_val {
                    if let Some(new_row_indices) = index.get_mut(&new_val.to_string()) {
                        new_row_indices.push(row_index);
                    } else {
                        index.insert(new_val.to_string(), vec![row_index]);
                    }
                }
            }
        }
    }
}

pub struct Views {
    cfg : config::Config,
    views: HashMap<String, View>,
}

impl Views {
    pub fn new(cfg: config::Config) -> Self {
        Views {
            cfg: cfg,
            views: HashMap::new(),
        }
    }

    pub fn add_view(&mut self, name: String, columns: Vec<ColumnDef>, indexes: &Vec<IndexDef>) {
        self.views.insert(name.clone(), View::new(name, columns, indexes));
    }
    
    pub fn query_iter(&self, query: &Query) -> Result<View, Error> {
        select::get_query_results(&self.views, query)
    }
 
    pub fn insert(&mut self, table_name: &ObjectName, columns: &Vec<Ident>, values: &mut Vec<Vec<Value>>) -> Result<(), Error> {
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        
        // initialize the rows to insert
        // insert rows with non-specified columns set as NULL for now (TODO)
        let mut insert_rows = vec![vec![Value::Null; view.columns.len()]; values.len()];
        
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
            let num_insert : u64 = values.len() as u64;
            
            let mut found = false;
            for c in columns {
                if *c == col.column.name {
                    // get the values of the uid col being inserted, update autoinc
                    let mut max = id_val;
                    for vv in values.into_iter() {
                        match &vv[col_index] {
                            Value::Number(n) => {
                                let n = n.parse::<u64>().unwrap();
                                max = u64::max(max, n);
                            }
                            _ => (),
                        }
                    }
                    // TODO ensure self.latest_uid never goes above GID_START
                    view.autoinc_col = Some((col_index, max));
                    found = true;
                    break;
                }
            }
            if !found {
                // put self.latest_uid + N as the id col values 
                let cur_uid = id_val + num_insert; 
                for i in 0..num_insert {
                    values[i as usize].push(Value::Number(format!("{}", cur_uid + i + 1)));
                }
                
                // add id column to update
                // first, ensure that it wasn't included to begin with (if columns were empty)
                cis.retain(|&ci| ci != col_index);

                // then add it to the end!
                cis.push(col_index);
                view.autoinc_col = Some((col_index, id_val + values.len() as u64));
            }
        }

        // update with the values to insert
        for (val_index, ci) in cis.iter().enumerate() {
            for (i, row) in values.iter().enumerate() {
                // update the right column ci with the value corresponding 
                // to that column to update
                view.insert_into_index(i, *ci, &row[val_index]);
                insert_rows[i][*ci] = row[val_index].clone();
            }
        }

        view.rows.append(&mut insert_rows);
        Ok(())
    }

    pub fn update(&mut self, 
          table_name: &ObjectName, 
          assignments: &Vec<Assignment>, 
          selection: &Option<Expr>, 
          assign_vals: &Vec<Value>) 
        -> Result<(), Error> 
    {
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        let row_indices : HashSet<usize>;
        if let Some(s) = selection {
            row_indices = select::get_rows_matching_constraint(s, &view).1;
        } else {
            row_indices = (0..view.rows.len()).collect();
        }
        
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
                        Value::Number(n) => {
                            let n = n.parse::<u64>().unwrap();
                            view.autoinc_col = Some((col_index, u64::max(id_val, n)));
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

        // update the rows!
        for ri in row_indices {
            for ci in &cis {
                view.update_index(ri, *ci, Some(&assign_vals[*ci]));
                view.rows[ri][*ci] = assign_vals[*ci].clone();
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, 
          table_name: &ObjectName, 
          selection: &Option<Expr>)
        -> Result<(), Error> 
    {
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        let row_indices : HashSet<usize>;
        if let Some(s) = selection {
            row_indices = select::get_rows_matching_constraint(s, &view).1;
        } else {
            row_indices = (0..view.rows.len()).collect();
        }
       
        // remove the rows!
        // pretty expensive, but oh well... (can optimize later?)
        let mut ris : Vec<usize> = Vec::from_iter(row_indices);
        ris.sort_by(|a, b| b.cmp(a));
        for ri in ris {
            for ci in 0..view.columns.len() {
                view.update_index(ri, ci, None);
            }
            view.rows.remove(ri);
        }
        Ok(())
    }
}
