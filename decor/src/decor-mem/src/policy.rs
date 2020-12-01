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
   
#[derive(Clone, Debug)]
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
