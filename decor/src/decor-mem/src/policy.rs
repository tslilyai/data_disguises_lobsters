use std::*;
use std::collections::HashMap;

pub type Column<'a> = &'a str; // column name
pub type Entity<'a> = &'a str; // table name, or foreign key

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
pub type GhostPolicy<'a> = HashMap<Column<'a>, GhostColumnPolicy>;
pub type EntityGhostPolicies<'a> = HashMap<Entity<'a>, GhostPolicy<'a>>;
   
#[derive(Clone, Debug, PartialEq)]
pub enum DecorrelationPolicy {
    NoDecorRemove,
    NoDecorRetain,
    NoDecorSensitivity(f64),
    Decor,
}
#[derive(Clone, Debug)]
pub struct KeyRelationship<'a> {
    pub child: Entity<'a>,
    pub parent: Entity<'a>,
    pub column_name: &'a str,
    pub decorrelation_policy: DecorrelationPolicy,
}
pub struct ApplicationPolicy<'a> {
    pub ghost_policies: EntityGhostPolicies<'a>, 
    pub edge_policies: Vec<KeyRelationship<'a>>,
}

pub fn policy_to_ghosted_tables(policy: &ApplicationPolicy) -> HashMap<String, Vec<String>> {
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
    gdts
}
