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
        let mut is_write = false;
        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                /*Query{
                    ctes: vec![],
                    body: 
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    fetch: None,
                })),*/

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
        is_write
    }
}
