use std::collections::{HashMap};
use crate::types::*;

pub enum PolicyName {
    Modify,
    Delete,
    SplitOff,
}

pub struct Policy {
    pub predicate: Box<dyn Fn(&Vec<(TableCol, String)>) -> bool>,
    pub policy: PolicyName,
}

pub struct TableInfo {
    pub referencers: Vec<ForeignKeyCol>,
    pub id_col_info: TableCol,
    pub columns: Vec<TableCol>,
    pub guise_modifications: GuiseModifications,
}

pub struct SchemaConfig {
    pub user_table: String,
    pub table_info: HashMap<TableName, TableInfo>,
    pub single_policies: HashMap<TableName, Policy>,
    pub pair_policies: HashMap<TableNamePair, Policy>,
}


