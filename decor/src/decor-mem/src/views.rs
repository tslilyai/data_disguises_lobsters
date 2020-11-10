use sql_parser::ast::*;
use std::collections::HashMap;
use crate::select;
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
}

impl View {
    pub fn new(columns: Vec<TableColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: vec![],
            indices: None,
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
    views: HashMap<String, View>,
}

impl Views {
    pub fn new() -> Self {
        Views {
            views: HashMap::new(),
        }
    }

    pub fn query_view(&self, stmt: &Statement) -> Result<(View, bool), Error> {
        let mut results : View = View::new(vec![]);
        let mut is_write = false;
        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                // ignore ctes for now
                results = select::get_query_results(&self.views, &query)?;
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                is_write = true;
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                is_write = true;
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                is_write = true;
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
                is_write = true;
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
                is_write = true;
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
                is_write = true;
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
                is_write = true;
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
        Ok((results, is_write))
    }
}
