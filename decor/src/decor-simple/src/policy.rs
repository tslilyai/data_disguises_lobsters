use std::*;
use std::collections::{HashMap};
use std::rc::Rc;

pub type ColumnName = String; // column name
pub type EntityName = String; // table name, or foreign key

pub enum GeneratePolicy {
    Random,
    Default(String),
    //Custom(Box<dyn Fn(&str) -> String>), // column value -> column value
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
pub enum EdgePolicyType {
    Decorrelate(f64),
    Delete(f64),
    Retain,
}
#[derive(Clone, Debug)]
pub struct EdgePolicy {
    pub parent: EntityName,
    pub column: ColumnName,
    pub pc_policy: EdgePolicyType,
    pub cp_policy: EdgePolicyType,
}

pub struct ApplicationPolicy {
    pub unsub_entity_type: EntityName,
    pub ghost_policies: EntityGhostPolicies, 
    // child to parent edges
    pub edge_policies: HashMap<EntityName, Rc<Vec<EdgePolicy>>>,
}
