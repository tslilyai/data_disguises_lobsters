use sql_parser::ast::*;

#[derive(Clone)]
pub struct View {
    // table name
    name: ObjectName,
    // schema column definitions
    columns: Vec<ColumnDef>,
    // values stored in table
    rows: Vec<Vec<Value>>,
}

impl View {
    pub fn new(name: ObjectName, columns: Vec<ColumnDef>) -> Self {
        View {
            name: name,
            columns: columns,
            rows: vec![],
        }
    }
}

pub struct Views(Vec<View>);

impl Views {
    pub fn new() -> Self {
        Views(vec![])
    }

    pub fn update_view(&mut self, stmt: &Statement) -> bool {
        false
    }
}
