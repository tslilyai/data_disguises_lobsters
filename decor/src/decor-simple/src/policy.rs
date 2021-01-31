use std::*;
use std::collections::{HashMap};
use std::rc::Rc;

pub type ColumnName = String; // column name
pub type ObjectName = String; // table name, or foreign key

pub enum GeneratePolicy {
    Random,
    Default(String),
    Custom(Box<dyn Fn(&str) -> String>), // column value -> column value
    ForeignKey(ObjectName),
}
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(GeneratePolicy),
    Generate(GeneratePolicy),
}
pub type ObjectGhostPolicy = HashMap<ColumnName, GhostColumnPolicy>;
pub type ObjectGhostPolicies = HashMap<ObjectName, Rc<ObjectGhostPolicy>>;

#[derive(Clone, Debug, PartialEq)]
pub enum EdgePolicyType {
    Decorrelate(f64),
    Delete(f64),
    Retain,
}
#[derive(Clone, Debug)]
pub struct EdgePolicy {
    pub parent: ObjectName,
    pub column: ColumnName,
    pub pc_policy: EdgePolicyType,
    pub cp_policy: EdgePolicyType,
}

pub struct MaskPolicy {
    pub unsub_object_type: ObjectName,
    pub pc_ghost_policies: ObjectGhostPolicies, 
    pub cp_ghost_policies: ObjectGhostPolicies, 
    // child to parent edges
    pub edge_policies: HashMap<ObjectName, Rc<Vec<EdgePolicy>>>,
}
