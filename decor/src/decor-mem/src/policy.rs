use std::*;
use std::collections::HashMap;

pub type ColumnName<'a> = &'a str; // column name
pub type EntityName<'a> = &'a str; // table name, or foreign key

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
pub type GhostPolicy<'a> = HashMap<ColumnName<'a>, GhostColumnPolicy>;
pub type EntityGhostPolicies<'a> = HashMap<EntityName<'a>, GhostPolicy<'a>>;
   
#[derive(Clone, Debug, PartialEq)]
pub enum DecorrelationPolicy {
    NoDecorRemove,
    NoDecorRetain,
    NoDecorSensitivity(f64),
    Decor,
}
#[derive(Clone, Debug)]
pub struct KeyRelationship<'a> {
    pub child: EntityName<'a>,
    pub parent: EntityName<'a>,
    pub column_name: &'a str,
    pub decorrelation_policy: DecorrelationPolicy,
}
pub struct ApplicationPolicy<'a> {
    pub entity_type_to_decorrelate: EntityName<'a>,
    pub ghost_policies: EntityGhostPolicies<'a>, 
    pub edge_policies: Vec<KeyRelationship<'a>>,
}

pub fn policy_to_ghosted_tables(policy: &ApplicationPolicy) -> (String, HashMap<String, Vec<String>>) {
    let mut gdts: HashMap<String, Vec<String>>= HashMap::new();
    for kr in &policy.edge_policies {
        if kr.decorrelation_policy == DecorrelationPolicy::Decor {
            let tablename = kr.child.to_string();
            if let Some(ghost_cols) = gdts.get_mut(&tablename) {
                ghost_cols.push(kr.column_name.to_string());
            } else {
                gdts.insert(tablename, vec![kr.column_name.to_string()]);
            }
        }
    }
    (policy.entity_type_to_decorrelate.to_string(), gdts)
}
