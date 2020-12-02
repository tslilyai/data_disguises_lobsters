use std::*;
use std::collections::HashMap;
use crate::views::{RowPtrs, HashedRowPtr, TableColumnDef};
use sql_parser::ast::Value;
use std::cell::RefCell;
use std::rc::Rc;

pub type ColumnName = String; // column name
pub type EntityName = String; // table name, or foreign key

pub enum GeneratePolicy {
    Random,
    Default(String),
    Custom(Box<dyn Fn(String) -> String>), // column value -> column value
    ForeignKey,
}
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(GeneratePolicy),
    Generate(GeneratePolicy),
}
pub type GhostPolicy = HashMap<ColumnName, GhostColumnPolicy>;
pub type EntityGhostPolicies = HashMap<EntityName, GhostPolicy>;

pub fn generate_new_entities_from(
    gp: &GhostPolicy,
    from_cols: &Vec<TableColumnDef>,
    from_vals: &HashedRowPtr, 
    num_entities: usize, 
    with_eids: Option<&Vec<Value>>) 
    -> RowPtrs 
{
    use GhostColumnPolicy::*;
    let mut new_vals : RowPtrs = vec![
        Rc::new(RefCell::new(vec![Value::Null; from_cols.len()])); 
        num_entities
    ];
    for (i, col) in from_cols.iter().enumerate() {
        let col_policy = gp.get(&col.colname).unwrap();
        match col_policy {
            CloneAll => {

            }
            CloneOne(gen) => {

            }
            Generate(gen) => {

            }
        }
    } 
    new_vals
}

#[derive(Clone, Debug, PartialEq)]
pub enum DecorrelationPolicy {
    NoDecorRemove,
    NoDecorRetain,
    NoDecorSensitivity(f64),
    Decor,
}
#[derive(Clone, Debug)]
pub struct KeyRelationship {
    pub child: EntityName,
    pub parent: EntityName,
    pub column_name: ColumnName,
    pub decorrelation_policy: DecorrelationPolicy,
}

pub struct ApplicationPolicy {
    pub entity_type_to_decorrelate: EntityName,
    pub ghost_policies: EntityGhostPolicies, 
    pub edge_policies: Vec<KeyRelationship>,
}

pub struct Config {
    pub entity_type_to_decorrelate: String,
    // table and table columns+type that will be decorrelated (store GIDs)
    pub ghosted_tables: HashMap<String, Vec<(String, String)>>,
    // table and table columns+type for which sensitivity should fall below specified threshold
    pub sensitive_tables: HashMap<String, Vec<(String, String, f64)>>,
}

pub fn policy_to_config(policy: &ApplicationPolicy) -> Config {
    let mut gdts: HashMap<String, Vec<(String, String)>>= HashMap::new();
    let mut sdts: HashMap<String, Vec<(String, String, f64)>>= HashMap::new();
    for kr in &policy.edge_policies {
        let tablename = kr.child.clone();
        let parent = kr.parent.clone();
        let columname = kr.column_name.clone();
        match kr.decorrelation_policy {
            DecorrelationPolicy::Decor => {
                if let Some(ghost_cols) = gdts.get_mut(&tablename) {
                    ghost_cols.push((columname, parent));
                } else {
                    gdts.insert(tablename, vec![(columname, parent)]);
                }
            } 
            DecorrelationPolicy::NoDecorRemove => {
                if let Some(ghost_cols) = sdts.get_mut(&tablename) {
                    ghost_cols.push((columname, parent, 0.0));
                } else {
                    sdts.insert(tablename, vec![(columname, parent, 0.0)]);
                }        
            } 
            DecorrelationPolicy::NoDecorSensitivity(s) => {
                if let Some(ghost_cols) = sdts.get_mut(&tablename) {
                    ghost_cols.push((columname, parent, s));
                } else {
                    sdts.insert(tablename, vec![(columname, parent, s)]);
                }        
            }
            DecorrelationPolicy::NoDecorRetain => {
                if let Some(ghost_cols) = sdts.get_mut(&tablename) {
                    ghost_cols.push((columname, parent, 1.0));
                } else {
                    sdts.insert(tablename, vec![(columname, parent, 1.0)]);
                }
            } 
        }
    }
    
    Config {
        entity_type_to_decorrelate: policy.entity_type_to_decorrelate.clone(), 
        ghosted_tables: gdts,
        sensitive_tables: sdts,
    }
}
