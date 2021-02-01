use std::*;
use std::collections::{HashMap, HashSet};
use crate::types::{ForeignKey, HashedRowPtrs, HashedRowPtr, ObjectIdentifier};
use log::{warn};

/* object type (child table+col index to parent) => children rptrs */
pub type ObjectTypeRows = HashMap<ForeignKey, HashedRowPtrs>;

/* parent oid value to children rows */
pub struct ObjectGraph(HashMap<ObjectIdentifier, ObjectTypeRows>);

impl ObjectGraph {
    pub fn new() -> Self {
        ObjectGraph(HashMap::new())
    }

    pub fn add_edge(&mut self, 
            childrptr: HashedRowPtr, 
            child_table: &str,  
            parent_name: ObjectIdentifier,
            parent_col_index: usize) {
        warn!("Adding edge from {:?} col {} to {} val {:?}", parent_name, parent_col_index, child_table, childrptr);
        let fk = ForeignKey{
            child_table: child_table.to_string(), 
            col_index: parent_col_index,
            parent_table: parent_name.table.to_string(), 
        };
        if let Some(typ2rows) = self.0.get_mut(&parent_name) {
            if let Some(rows) = typ2rows.get_mut(&fk) {
                rows.insert(childrptr);
            } else {
                let mut hs = HashSet::new();
                hs.insert(childrptr);
                typ2rows.insert(fk, hs);
            }
        } else {
            let mut hm = HashMap::new();
            let mut hrptrs = HashSet::new();
            hrptrs.insert(childrptr);
            hm.insert(fk, hrptrs);
            self.0.insert(parent_name, hm);
        }
    }

    pub fn update_edge(&mut self, child_table: &str, 
                       child_rptr: HashedRowPtr, // will have been updated with new values
                       old_parent_name: ObjectIdentifier, 
                       new_parent_name: Option<ObjectIdentifier>, 
                       parent_col_index: usize) 
    {
        warn!("Updating edge from parent {:?} (child {}, col {}) to new val {:?}", 
              old_parent_name, child_table, parent_col_index, new_parent_name);
        // remove old edges from both directions
        if let Some(typ2rows) = self.0.get_mut(&old_parent_name) {
            let fk = ForeignKey {
                child_table:child_table.to_string(),
                col_index: parent_col_index,
                parent_table:old_parent_name.table.clone(),
            };
            if let Some(rows) = typ2rows.get_mut(&fk) {
                // remove old child from this parent
                rows.remove(&child_rptr); 
            } 
        }
        // if not none, insert new edge
        if let Some(np) = new_parent_name {
            self.add_edge(child_rptr, child_table, np, parent_col_index);
        }
    }

    pub fn get_children_of_parent(&self, parent: &ObjectIdentifier) -> Option<&ObjectTypeRows> {
        self.0.get(parent)
    }
}
