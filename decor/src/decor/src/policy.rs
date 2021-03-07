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
pub enum GuiseColumnPolicy {
    CloneAll,
    CloneOne(GeneratePolicy),
    Generate(GeneratePolicy),
}
pub type ObjectGuisePolicy = HashMap<ColumnName, GuiseColumnPolicy>;
pub type ObjectGuisePolicies = HashMap<ObjectName, Rc<ObjectGuisePolicy>>;

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
    pub pc_guise_policies: ObjectGuisePolicies, 
    pub cp_guise_policies: ObjectGuisePolicies, 
    // child to parent edges
    pub edge_policies: HashMap<ObjectName, Rc<Vec<EdgePolicy>>>,
}
