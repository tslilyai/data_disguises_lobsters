use std::*;
use std::collections::{HashMap};
use std::rc::Rc;

pub type ColumnName = String; // column name
pub type EntityName = String; // table name, or foreign key

pub enum GeneratePolicy {
    Random,
    Default(String),
    Custom(Box<dyn Fn(&str) -> String>), // column value -> column value
    ForeignKey(EntityName),
}
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(GeneratePolicy),
    Generate(GeneratePolicy),
}
pub type GhostPolicy = HashMap<ColumnName, GhostColumnPolicy>;
pub type EntityGhostPolicies = HashMap<EntityName, Rc<GhostPolicy>>;

#[derive(Clone, Debug, PartialEq)]
pub enum UnsubscribePolicy{
    Decorrelate(f64),
    Delete(f64),
    Retain,
}
#[derive(Clone, Debug)]
pub struct EdgePolicy {
    pub child: EntityName,
    pub parent: EntityName,
    pub column: ColumnName,
    pub pc_policy: UnsubscribePolicy,
    pub cp_policy: UnsubscribePolicy,
}

pub struct ApplicationPolicy {
    pub entity_type_to_decorrelate: EntityName,
    pub ghost_policies: EntityGhostPolicies, 
    pub edge_policies: Vec<EdgePolicy>,
}

pub struct Config {
    pub decor_etype: String,
    pub table2policies: HashMap<String, Vec<EdgePolicy>>,
    pub ghost_policies: EntityGhostPolicies, 
}

pub fn policy_to_config(policy: &ApplicationPolicy) -> Config {
    let mut table2policies: HashMap<String, Vec<EdgePolicy>>= HashMap::new();
    for ep in &policy.edge_policies {
        if let Some(policies) = table2policies.get_mut(&ep.child) {
            policies.push(ep.clone());
        } else {
            table2policies.insert(ep.child.clone(), vec![ep.clone()]);
        }
    }
    Config {
        decor_etype : policy.entity_type_to_decorrelate.clone(), 
        table2policies: table2policies,
        ghost_policies: policy.ghost_policies.clone(),
    }
}
