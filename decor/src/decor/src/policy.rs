use std::collections::{HashMap};
use crate::types::*;

pub type TableName = String;

pub struct TableInfo {
    pub referencers: Vec<ForeignKeyCol>,
    pub id_col_info: TableCol,
    pub guise_modifications: GuiseModifications,
}

pub struct SchemaConfig {
    pub user_table: String,
    pub table_info: HashMap<TableName, TableInfo>,
}
