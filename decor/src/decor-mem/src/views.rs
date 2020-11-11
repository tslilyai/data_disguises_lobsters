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
    pub latest_id: u64,
}

impl View {
    pub fn new(columns: Vec<TableColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: vec![],
            indices: None,
            latest_id: 0,
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

    pub fn stmt_iter(&mut self, stmt: &Statement) -> Result<View, Error> {
        let mut view_res : View = View::new(vec![]);
        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                view_res = self.query_iter(&query)?;
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                // if the user table has an autoincrement column, we should 
                // (1) see if the table is actually inserting a value for that column (found) 
                // (2) update the self.latest_uid appropriately and insert the value for that column
                /*if table_name.to_string() == self.cfg.user_table.name && self.cfg.user_table.is_autoinc {
                    let mut found = false;
                    for (i, col) in columns.iter().enumerate() {
                        if col.to_string() == self.cfg.user_table.id_col {
                            // get the values of the uid col being inserted and update as
                            // appropriate
                            let mut max = self.latest_id.load(Ordering::SeqCst);
                            for vv in &values{
                                match &vv[i] {
                                    Expr::Value(Value::Number(n)) => {
                                        let n = n.parse::<u64>().map_err(|e| mysql::Error::IoError(io::Error::new(
                                                        io::ErrorKind::Other, format!("{}", e))))?;
                                        max = cmp::max(max, n);
                                    }
                                    _ => (),
                                }
                            }
                            // TODO ensure self.latest_uid never goes above GID_START
                            self.latest_uid.fetch_max(max, Ordering::SeqCst);
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        // put self.latest_uid + N as the id col values 
                        let cur_uid = self.latest_uid.fetch_add(values.len() as u64, Ordering::SeqCst);
                        for i in 0..values.len() {
                            values[i].push(Expr::Value(Value::Number(format!("{}", cur_uid + (i as u64) + 1))));
                        }
                        // add id column to update
                        mv_cols.push(Ident::new(self.cfg.user_table.id_col.clone()));
                    }
                    // update source with new vals_vec
                    if let Some(mut nq) = new_q {
                        nq.body = SetExpr::Values(Values(values.clone()));
                        new_source = InsertSource::Query(nq);
                    }
                }*/
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
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
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
            }
            Statement::CreateView(CreateViewStatement{
                name,
                columns,
                with_options,
                query,
                if_exists,
                temporary,
                materialized,
            }) => {
            }
            Statement::CreateTable(CreateTableStatement{
                name,
                columns,
                constraints,
                indexes,
                with_options,
                if_not_exists,
                engine,
            }) => {
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
            }
            /* TODO Handle Statement::Explain(stmt) => f.write_node(stmt), ShowObjects
             *
             * TODO Currently don't support alterations that reset autoincrement counters
             * Assume that deletions leave autoincrement counters as monotonically increasing
             *
             * Don't handle CreateSink, CreateSource, Copy,
             *  ShowCreateSource, ShowCreateSink, Tail, Explain
             * 
             * Don't modify queries for CreateSchema, CreateDatabase, 
             * ShowDatabases, ShowCreateTable, DropDatabase, Transactions,
             * ShowColumns, SetVariable (mysql exprs in set var not supported yet)
             *
             * XXX: ShowVariable, ShowCreateView and ShowCreateIndex will return 
             *  queries that used the materialized views, rather than the 
             *  application-issued tables. This is probably not a big issue, 
             *  since these queries are used to create the table again?
             *
             * XXX: SHOW * from users will not return any ghost users in ghostusersMV
             * */
            _ => {
            }
        }
        Ok(view_res)
    }
}
