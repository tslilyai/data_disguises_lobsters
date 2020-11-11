use sql_parser::ast::*;
use std::collections::HashMap;
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
    // List of indices (by column) INDEX of column (only INT type for now) to row
    pub indices: Option<HashMap<String, HashMap<String, Vec<usize>>>>,
    // optional autoinc column (index) and current value
    pub autoinc_col: Option<(usize, u64)>,
}

impl View {
    pub fn new(columns: Vec<TableColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: vec![],
            indices: None,
            autoinc_col: None,
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
    
    pub fn query_iter(&self, query: &Query) -> Result<View, Error> {
        select::get_query_results(&self.views, query)
    }
 
    pub fn insert(&mut self, table_name: &ObjectName, columns: &Vec<Ident>, values: &mut Vec<Vec<Value>>) -> Result<(), Error> {
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        
        // initialize the rows to insert
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
                cis.push(col_index);
                view.autoinc_col = Some((col_index, id_val + values.len() as u64));
            }
        }

        // update indices

        // insert rows with non-speified columns set as NULL for now (TODO)
        for row in values {
             
        }
        view.rows.append(&mut insert_rows);
        Ok(())
    }

                // if the user table has an autoincrement column, we should 
                // (1) see if the table is actually updating a value for that column and
                // (2) update the self.latest_uid appropriately 
                /*if table_name.to_string() == self.cfg.user_table.name && self.cfg.user_table.is_autoinc {
                    for i in 0..assignments.len() {
                        if assignments[i].id.to_string() == self.cfg.user_table.id_col {
                            match &assign_vals[i] {
                                Expr::Value(Value::Number(n)) => {
                                    let n = n.parse::<u64>().map_err(|e| mysql::Error::IoError(io::Error::new(
                                                    io::ErrorKind::Other, format!("{}", e))))?;
                                    self.latest_uid.fetch_max(n, Ordering::SeqCst);
                                }
                                _ => (),
                            }
                        }
                    }
                }*/
}
