use std::*;
use std::collections::HashMap;

pub type ColumnName = String; // column name
pub type EntityName = String; // table name, or foreign key

#[derive(Clone, Debug, PartialEq)]
pub enum GeneratePolicy {
    Random,
    Default(String),
    //Custom(Box<dyn Fn(&str) -> String>), // column value -> column value
    ForeignKey(EntityName),
}
#[derive(Clone, Debug, PartialEq)]
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(GeneratePolicy),
    Generate(GeneratePolicy),
}
pub type GhostPolicy = HashMap<ColumnName, GhostColumnPolicy>;
pub type EntityGhostPolicies = HashMap<EntityName, GhostPolicy>;

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
    pub parent_child_decorrelation_policy: DecorrelationPolicy,
    pub child_parent_decorrelation_policy: DecorrelationPolicy,
}

pub struct ApplicationPolicy {
    pub entity_type_to_decorrelate: EntityName,
    pub ghost_policies: EntityGhostPolicies, 
    pub edge_policies: Vec<KeyRelationship>,
}

pub struct Config {
    pub entity_type_to_decorrelate: String,

    // table and which columns(+parent type) that correspond to ghosts (edges that are decorrelated and store GIDs)
    // created when parent->child edges are decorrelated
    pub parent_child_ghosted_tables: HashMap<String, Vec<(String, String)>>,
    // table and which columns(+parent type) for which sensitivity to this parent should fall below specified threshold
    pub parent_child_sensitive_tables: HashMap<String, Vec<(String, String, f64)>>,
  
    // table and which columns taht correspond to ghosts (edges that are decorrelated and store
    // GIDs), created when child->parent edges are decorrelated
    // NOTE: this is usually a subset of parent_child_ghosted_tables: an edge type that is
    // decorrelated when a child is sensitive should also be decorrelated when both the parent and
    // child are sensitive
    pub child_parent_ghosted_tables: HashMap<String, Vec<(String, String)>>,
    // table and which columns(+parent type) for which sensitivity to this parent should fall below specified threshold
    pub child_parent_sensitive_tables: HashMap<String, Vec<(String, String, f64)>>,
}

pub fn policy_to_config(policy: &ApplicationPolicy) -> Config {
    let mut pc_gdts: HashMap<String, Vec<(String, String)>>= HashMap::new();
    let mut pc_sdts: HashMap<String, Vec<(String, String, f64)>>= HashMap::new();

    let mut cp_gdts: HashMap<String, Vec<(String, String)>>= HashMap::new();
    let mut cp_sdts: HashMap<String, Vec<(String, String, f64)>>= HashMap::new();
    for kr in &policy.edge_policies {
        match kr.parent_child_decorrelation_policy {
            DecorrelationPolicy::Decor => {
                if let Some(ghost_cols) = pc_gdts.get_mut(&kr.child) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone()));
                } else {
                    pc_gdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone())]);
                }
            } 
            DecorrelationPolicy::NoDecorRemove => {
                if let Some(ghost_cols) = pc_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 0.0));
                } else {
                    pc_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 0.0)]);
                }        
            } 
            DecorrelationPolicy::NoDecorSensitivity(s) => {
                if let Some(ghost_cols) = pc_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), s));
                } else {
                    pc_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), s)]);
                }        
            }
            DecorrelationPolicy::NoDecorRetain => {
                if let Some(ghost_cols) = pc_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 1.0));
                } else {
                    pc_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 1.0)]);
                }
            } 
        }
        match kr.child_parent_decorrelation_policy {
            DecorrelationPolicy::Decor => {
                if let Some(ghost_cols) = cp_gdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone()));
                } else {
                    cp_gdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone())]);
                }
            } 
            DecorrelationPolicy::NoDecorRemove => {
                if let Some(ghost_cols) = cp_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 0.0));
                } else {
                    cp_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 0.0)]);
                }        
            } 
            DecorrelationPolicy::NoDecorSensitivity(s) => {
                if let Some(ghost_cols) = cp_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), s));
                } else {
                    cp_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), s)]);
                }        
            }
            DecorrelationPolicy::NoDecorRetain => {
                if let Some(ghost_cols) = cp_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 1.0));
                } else {
                    cp_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 1.0)]);
                }
            } 
        }
    }
    
    Config {
        entity_type_to_decorrelate: policy.entity_type_to_decorrelate.clone(), 
        parent_child_ghosted_tables: pc_gdts,
        parent_child_sensitive_tables: pc_sdts,

        child_parent_ghosted_tables: cp_gdts,
        child_parent_sensitive_tables: cp_sdts,
    }
}
