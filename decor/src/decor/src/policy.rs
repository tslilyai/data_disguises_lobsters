use std::collections::{HashMap};
use crate::types::*;
use crate::disguises::*;

pub type TableName = String;

pub struct SchemaConfig {
    pub referencers : HashMap<TableName, Vec<ForeignKeyCol>>,
    pub id_cols: HashMap<TableName, TableCol>,
    pub disguise: Vec<Action>,
}
