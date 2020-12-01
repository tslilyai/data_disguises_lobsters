pub type Column: String; // column name
pub type Entity: String; // table name, or foreign key

enum GeneratePolicy {
        Random,
        Default,
        Custom<F>(f: F) // where F: FnMut(Column) -> Column,
        ForeignKey,
    }
pub enum GhostColumnPolicy {
        CloneAll,
        CloneOne(gp: GeneratePolicy),
        Generate(gp: GeneratePolicy),
    }
pub type GhostPolicy = HashMap<Column, GhostColumnPolicy>;
pub type EntityGhostPolicies = HashMap<Entity, GhostPolicy>;
   
pub enum DecorrelationPolicy {
        NoDecorRemove,
        NoDecorRetain,
        NoDecorSensitivity(f64),
        Decor,
}
pub struct KeyRelationship {
    child: Entity,
    parent: Entity,
    column_name: String,
    decorrelation_policy: DecorrelationPolicy,
}
pub type ApplicationPolicy = (EntityGhostPolicies, Vec<KeyRelationship>);
