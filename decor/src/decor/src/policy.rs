use std::collections::{HashMap};
use crate::types::*;
use crate::disguises::*;

pub type TableName = String;

pub struct SchemaConfig {
    pub referencers : HashMap<TableName, Vec<ForeignKeyCol>>,
    pub id_cols: HashMap<TableName, TableCol>,
}

pub fn leave_disguise(target: ID, schema_config: &SchemaConfig, db: &mut mysql::Conn) {
    perform_action(Action::ModifyGuise(target, vec![]), schema_config, db);
}
