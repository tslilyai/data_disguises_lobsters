use std::iter::FromIterator;
use sql_parser::ast::*;
use std::collections::{HashMap, hash_set::HashSet};
use crate::{select, helpers, ghosts_map};
use std::io::{Error, Write};
use log::{warn};
use msql_srv::{QueryResultWriter, Column, ColumnFlags};

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
    pub rows: HashMap<usize, Vec<Value>>,
    // List of indexes (by column): column val(string, and only INT type for now) to row
    pub indexes: Option<HashMap<String, HashMap<String, HashSet<usize>>>>,
    // Current row-index value
    pub current_index: usize,
    // optional autoinc column (index) and current value
    // invariant: autoinc_col.1 is always the *next* value that should be used
    pub autoinc_col: Option<(usize, u64)>,
}

pub fn view_cols_rows_to_answer_rows<W: Write>(cols: &Vec<TableColumnDef>, rows: &Vec<Vec<Value>>, results: QueryResultWriter<W>)
    -> Result<(), mysql::Error> 
{
    let cols : Vec<_> = cols.iter()
        .map(|c| {
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
    for row in rows {
        for v in row {
            writer.write_col(helpers::parser_val_to_common_val(&v))?;
        }
        writer.end_row()?;
    }
    writer.finish()?;
    Ok(())
}

impl View {
    pub fn insert_row(&mut self, row: Vec<Value>) {
        let new_index = self.current_index;
        self.current_index += 1;
        self.rows.insert(new_index, row);
    }

    pub fn new_with_cols(columns: Vec<TableColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: HashMap::new(),
            indexes: None,
            current_index: 0,
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

        // create indexes for any explicit indexes
        let mut indexes = if !indexes.is_empty() {
            let mut map = HashMap::new();
            for i in indexes {
                for key in &i.key_parts {
                    // TODO just create a separate index for each key part for now rather than
                    // nesting
                    map.insert(key.to_string(), HashMap::new());
                    warn!("{}: Created index for column {}", name, key.to_string());
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
                    match indexes {
                        Some(ref mut is_map) => {
                            is_map.insert(c.name.to_string(), HashMap::new());
                            warn!("{}: Created unique index for column {}", name, c.name.to_string());
                        }
                        None => {
                            let mut map = HashMap::new();
                            map.insert(c.name.to_string(), HashMap::new());
                            warn!("{}: Created unique index for column {}", name, c.name.to_string());
                            indexes = Some(map);
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
            rows: HashMap::new(),
            indexes: indexes,
            current_index: 0,
            autoinc_col: autoinc_col,
        };
        warn!("created new view {:?}", view);
        view
    }

    pub fn contains_row(&self, r: &Vec<Value>) -> bool {
        self.rows.iter().any(|(_, row)| {
            let mut eq = true;
            for i in 0..row.len() {
                eq = eq && (row[i] == r[i]);
            }
            eq
        })
    }

    pub fn get_rows_of_col(&self, col_index: usize, col_val: &Value) -> Vec<Vec<Value>> {
        let mut rows = vec![];
        if let Some(indexes) = &self.indexes {
            if let Some(index) = indexes.get(&self.columns[col_index].column.name.to_string()) {
                if let Some(ris) = index.get(&col_val.to_string()) {
                    for i in ris {
                        if let Some(row) = self.rows.get(i) {
                            rows.push(row.clone());
                        }
                    }
                } 
                warn!("get_rows: found {} rows for col {} val {}!", rows.len(), self.columns[col_index].name(), col_val);
                return rows;
            }
        } 
        warn!("{}'s indexes are {:?}", self.name, self.indexes);
        warn!("get_rows: no index for col {} val {}!", self.columns[col_index].name(), col_val);
        for (_, row) in &self.rows {
            if row[col_index].to_string() == col_val.to_string() {
                rows.push(row.clone());
            }
        }
        rows
    }

    pub fn get_ris_of_col(&self, col_index: usize, col_val: &Value) -> HashSet<usize> {
        if let Some(indexes) = &self.indexes {
            if let Some(index) = indexes.get(&self.columns[col_index].column.name.to_string()) {
                if let Some(ris) = index.get(&col_val.to_string()) {
                    warn!("Found ris {:?} for col val {}!", ris, col_val);
                    return ris.clone();
                } else {
                    warn!("get_ris: Did not find rows for col {} val {}!", self.columns[col_index].name(), col_val);
                    return HashSet::new();
                }
            }
        } 
        warn!("get_ris: no index for col {} val {}!", self.columns[col_index].name(), col_val);
        let mut ris = HashSet::new();
        for (ri, row) in self.rows.iter() {
            //warn!("{}: checking for {:?} val {:?}", self.name, self.rows[ri][col_index], col_val);
            if row[col_index].to_string() == col_val.to_string() {
                ris.insert(*ri);
            }
        }
        ris
    }
    
    pub fn insert_into_index(&mut self, row_index: usize, col_index: usize, new_val: &Value) {
        if let Some(indexes) = &mut self.indexes {
            if let Some(index) = indexes.get_mut(&self.columns[col_index].column.name.to_string()) {
                warn!("{}: inserting ({}, row {}) into index", self.columns[col_index].name(), new_val, row_index);
                // insert into the new indexed ris 
                if let Some(new_ris) = index.get_mut(&new_val.to_string()) {
                    new_ris.insert(row_index);
                } else {
                    let mut hs = HashSet::new();
                    hs.insert(row_index);
                    index.insert(new_val.to_string(), hs);
                }
            }
        }
    }
 
    pub fn update_index(&mut self, row_index: usize, col_index: usize, new_val: Option<&Value>) {
        let mut old_val : &Value = &Value::Null;
        if let Some(row) = self.rows.get(&row_index) {
            old_val = &row[col_index];
        } else {
            assert!(false, "Value in index but not view?");
        }
        warn!("{}: updating {:?} from {:?}", self.columns[col_index].name(), new_val, old_val);
        if let Some(indexes) = &mut self.indexes {
            if let Some(index) = indexes.get_mut(&self.columns[col_index].column.name.to_string()) {
                // get the old indexed row_indexes if they existed for this column value
                // remove this row!
                if let Some(old_ris) = index.get_mut(&old_val.to_string()) {
                    warn!("{}: removing {:?} (row {}) from ris {:?}", self.columns[col_index].name(), old_val, row_index, old_ris);
                    old_ris.retain(|&ri| ri != row_index);
                }
                // insert into the new indexed ris but only if we are updating to a new
                // value (otherwise we're just deleting)
                if let Some(new_val) = new_val {
                    if let Some(new_ris) = index.get_mut(&new_val.to_string()) {
                        new_ris.insert(row_index);
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
    
    pub fn get_mut_view<'a>(&'a mut self, name: &str) -> Option<&'a mut View> {
        self.views.get_mut(name)
    }

    pub fn add_view(&mut self, name: String, columns: Vec<ColumnDef>, indexes: &Vec<IndexDef>) {
        self.views.insert(name.clone(), View::new(name, columns, indexes));
    }

    pub fn remove_views(&mut self, names: &Vec<ObjectName>) {
        for name in names {
            self.views.remove(&name.to_string());
        }
    }
    
    pub fn query_iter(&self, query: &Query) -> Result<(Vec<TableColumnDef>, Vec<Vec<Value>>), Error> {
        select::get_query_results(&self.views, query)
    }
 
    pub fn insert(&mut self, table_name: &ObjectName, columns: &Vec<Ident>, values: &mut Vec<Vec<Value>>) -> Result<(), Error> {
        let view = self.views.get_mut(&table_name.to_string()).unwrap();

        warn!("{}: insert values {:?} into {}", view.name, values, table_name);
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
                warn!("insert: setting insert_row col {} to {}", ci, row[val_index]);
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
                warn!("views::insert: Attempt insert into index: col {} with value {}", view.columns[ci].name(), row[ci]);
                // make sure to actually insert into the right index!!!
                view.insert_into_index(view.current_index+i, ci, &row[ci]);
            }
        }

        warn!("{}: Appending rows: {:?}", view.name, insert_rows);
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
        //let views = self.views.clone();
        let view = self.views.get_mut(&table_name.to_string()).unwrap();
        warn!("{}: update {:?} with vals {:?}", view.name, assignments, assign_vals);

        let ris : Vec<usize>;
        if let Some(s) = selection {
            ris = select::get_ris_matching_constraint(s, &view, None, None).into_iter().collect();
        } else {
            ris = (0..view.rows.len()).collect();
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

        warn!("{}: update columns of indices {:?}", view.name, cis);
        // update the rows!
        for (assign_index, ci) in cis.iter().enumerate() {
            match &assign_vals[assign_index] {
                Expr::Value(v) => {
                    for ri in &ris {
                        view.update_index(*ri, *ci, Some(&v));
                        view.rows.get_mut(ri).unwrap()[*ci] = v.clone();
                    }
                }
                _ => {
                    let val_for_rows = select::get_value_for_rows(&assign_vals[assign_index], &view, None, None, Some(&ris));
                    for (ri, val) in val_for_rows {
                        view.rows.get_mut(&ri).unwrap()[*ci] = val.clone();
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
        let ris : HashSet<usize>;
        if let Some(s) = selection {
            ris = select::get_ris_matching_constraint(s, &view, None, None);
        } else {
            ris = (0..view.rows.len()).collect();
        }
       
        // remove the rows!
        // pretty expensive, but oh well... (can optimize later?)
        let mut ris : Vec<usize> = Vec::from_iter(ris);
        ris.sort_by(|a, b| b.cmp(a));
        for ri in ris {
            for ci in 0..view.columns.len() {
                // TODO all the row indices have to change too..
                view.update_index(ri, ci, None);
            }
            view.rows.remove(&ri);
        }
        Ok(())
    }
}
