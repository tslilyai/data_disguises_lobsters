use std::iter::FromIterator;
use sql_parser::ast::*;
use std::collections::{HashMap, hash_set::HashSet};
use crate::{select};
use std::io::{Error};
use log::{warn};

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
    pub indices: Option<HashMap<String, HashMap<String, HashSet<usize>>>>,
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
                    warn!("no multi-column indices yet");
                    //unimplemented!("no multi-column indices yet");
                } else {
                    map.insert(i.key_parts[0].to_string(), HashMap::new());
                    warn!("{}: Created index for column {}", name, i.key_parts[0].to_string());
                }
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
                            warn!("{}: Created unique index for column {}", name, c.name.to_string());
                        }
                        None => {
                            let mut map = HashMap::new();
                            map.insert(c.name.to_string(), HashMap::new());
                            warn!("{}: Created unique index for column {}", name, c.name.to_string());
                            indices = Some(map);
                        }
                    }
                    break;
                }
            }
        }

        let view = View {
            name: name.clone(),
            columns: columns.iter()
                .map(|c| TableColumnDef{ table: name.clone(), column: c.clone() })
                .collect(),
            rows: vec![],
            indices: indices,
            autoinc_col: autoinc_col,
        };
        warn!("created new view {:?}", view);
        view
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

    pub fn get_rows_of_col(&self, col_index: usize, col_val: &Value) -> Vec<Vec<Value>> {
        let mut rows = vec![];
        if let Some(indices) = &self.indices {
            if let Some(index) = indices.get(&self.columns[col_index].column.name.to_string()) {
                if let Some(row_indices) = index.get(&col_val.to_string()) {
                    for i in row_indices {
                        rows.push(self.rows[*i].clone());
                    }
                } 
                warn!("get_rows: found {} rows for col {} val {}!", rows.len(), self.columns[col_index].name(), col_val);
                return rows;
            }
        } 
        warn!("{}'s indices are {:?}", self.name, self.indices);
        warn!("get_rows: no index for col {} val {}!", self.columns[col_index].name(), col_val);
        for row in &self.rows {
            if row[col_index].to_string() == col_val.to_string() {
                rows.push(row.clone());
            }
        }
        rows
    }

    pub fn get_row_indices_of_col(&self, col_index: usize, col_val: &Value) -> HashSet<usize> {
        if let Some(indices) = &self.indices {
            if let Some(index) = indices.get(&self.columns[col_index].column.name.to_string()) {
                if let Some(row_indices) = index.get(&col_val.to_string()) {
                    warn!("Found ris {:?} for col val {}!", row_indices, col_val);
                    return row_indices.clone();
                } else {
                    warn!("get_row_indices: Did not find rows for col {} val {}!", self.columns[col_index].name(), col_val);
                    return HashSet::new();
                }
            }
        } 
        //warn!("{}'s indices are {:?}", self.name, self.indices);
        warn!("get_row_indices: no index for col {} val {}!", self.columns[col_index].name(), col_val);
        let mut ris = HashSet::new();
        for ri in 0..self.rows.len() {
            //warn!("{}: checking for {:?} val {:?}", self.name, self.rows[ri][col_index], col_val);
            if self.rows[ri][col_index].to_string() == col_val.to_string() {
                ris.insert(ri);
            }
        }
        ris
    }
    
    pub fn insert_into_index(&mut self, row_index: usize, col_index: usize, new_val: &Value) {
        if let Some(indices) = &mut self.indices {
            if let Some(index) = indices.get_mut(&self.columns[col_index].column.name.to_string()) {
                warn!("{}: inserting {} into index", self.columns[col_index].name(), new_val);
                // insert into the new indexed row_indices 
                if let Some(new_row_indices) = index.get_mut(&new_val.to_string()) {
                    new_row_indices.insert(row_index);
                } else {
                    let mut hs = HashSet::new();
                    hs.insert(row_index);
                    index.insert(new_val.to_string(), hs);
                }
            }
        }
    }
 
    pub fn update_index(&mut self, row_index: usize, col_index: usize, new_val: Option<&Value>) {
        warn!("{}: updating {:?})", self.columns[col_index].name(), new_val);
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
                        new_row_indices.insert(row_index);
                    } else {
                        let mut hs = HashSet::new();
                        hs.insert(row_index);
                        index.insert(new_val.to_string(), hs);
                    }
                }
            }
        }
    }
}


pub struct Views {
    views: HashMap<String, View>,
}

impl Views {
    pub fn new() -> Self {
        Views {
            views: HashMap::new(),
        }
    }

    pub fn add_view(&mut self, name: String, columns: Vec<ColumnDef>, indexes: &Vec<IndexDef>) {
        self.views.insert(name.clone(), View::new(name, columns, indexes));
    }

    pub fn remove_views(&mut self, names: &Vec<ObjectName>) {
        for name in names {
            self.views.remove(&name.to_string());
        }
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
                let cur_uid = id_val; 
                for i in 0..num_insert {
                    values[i as usize].push(Value::Number(format!("{}", cur_uid + i)));
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
                insert_rows[i][*ci] = row[val_index].clone();
            }
        }

        for i in 0..insert_rows.len() {
            for ci in 0..view.columns.len() {
                let row = &mut insert_rows[i];
                
                // update with default (not null) values
                for opt in &view.columns[ci].column.options {
                    if let ColumnOption::Default(Expr::Value(v)) = &opt.option {
                        warn!("Updating col {} with default value {}", view.columns[ci].name(), v);
                        if row[ci] == Value::Null {
                            row[ci] = v.clone();
                        } 
                    }  
                    if let ColumnOption::NotNull = &opt.option {
                        assert!(row[ci] != Value::Null);
                    }
                }

                // insert all values (even if null) into indices
                warn!("Attempt insert into index: col {} with value {}", view.columns[ci].name(), row[ci]);
                view.insert_into_index(i, ci, &row[ci]);
            }
        }

        warn!("{}: Appending rows: {:?}", view.name, insert_rows);
        view.rows.append(&mut insert_rows);
        Ok(())
    }

    pub fn update(&mut self, 
          table_name: &ObjectName, 
          assignments: &Vec<Assignment>, 
          selection: &Option<Expr>, 
          assign_vals: &Vec<Expr>) 
        -> Result<(), Error> 
    {
        //let views = self.views.clone();
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        warn!("{}: update {:?} with vals {:?}", view.name, assignments, assign_vals);

        let row_indices : Vec<usize>;
        if let Some(s) = selection {
            row_indices = select::get_rows_matching_constraint(s, &view, None, None).into_iter().collect();
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
                        Expr::Value(Value::Number(n)) => {
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

        warn!("{}: update columns of indices {:?}", view.name, cis);
        // update the rows!
        for (assign_index, ci) in cis.iter().enumerate() {
            match &assign_vals[assign_index] {
                Expr::Value(v) => {
                    for ri in &row_indices {
                        view.update_index(*ri, *ci, Some(&v));
                        view.rows[*ri][*ci] = v.clone();
                    }
                }
                _ => {
                    let val_for_rows = select::get_value_for_rows(&assign_vals[assign_index], &view, None, None, Some(&row_indices));
                    for i in 0..row_indices.len() {
                        view.rows[row_indices[i]][*ci] = val_for_rows[i].clone();
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
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        let row_indices : HashSet<usize>;
        if let Some(s) = selection {
            row_indices = select::get_rows_matching_constraint(s, &view, None, None);
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
