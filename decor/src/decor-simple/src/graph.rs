use std::*;
use std::collections::{HashMap, HashSet};
use crate::views::{HashedRowPtrs, HashedRowPtr};
use log::{warn};

/* entity type (parent table+parentcol) => children rptrs */
pub type ObjectTypeRows = HashMap<(String, usize), HashedRowPtrs>;

/* parent oid value to ((table+parentcol)=> rptrs of children) */
pub type ObjectEdges = HashMap<u64 , ObjectTypeRows>;
pub struct ObjectGraph {
    // map from parent type => map of parent id => children rptrs (map of type -> rptrs)
    pub parents_to_children: HashMap<String, ObjectEdges>,
}

impl ObjectGraph {
    pub fn new() -> Self {
        ObjectGraph {
            parents_to_children: HashMap::new(),
        }
    }
    pub fn add_edge(&mut self, 
                    childrptr: HashedRowPtr, 
                    child_table: &str,  
                    parent_table: &str, 
                    parent_oid: u64, 
                    parent_col_index: usize) {
        warn!("Adding edge from {} col {} val {} to {} val {:?}", parent_table, parent_col_index, parent_oid, child_table, childrptr);
        if let Some(edges) = self.parents_to_children.get_mut(parent_table) {
            if let Some(typ2rows) = edges.get_mut(&parent_oid) {
                if let Some(rows) = typ2rows.get_mut(&(child_table.to_string(), parent_col_index)) {
                    rows.insert(childrptr);
                } else {
                    let mut hs = HashSet::new();
                    hs.insert(childrptr);
                    typ2rows.insert((child_table.to_string(), parent_col_index), hs);
                }
            } else {
                let mut hm = HashMap::new();
                let mut hs = HashSet::new();
                hs.insert(childrptr);
                hm.insert((child_table.to_string(), parent_col_index), hs);
                edges.insert(parent_oid, hm);
            }
        } else {
            let mut parenthm = HashMap::new();
            let mut hm = HashMap::new();
            let mut hs = HashSet::new();
            hs.insert(childrptr);
            hm.insert((child_table.to_string(), parent_col_index), hs);
            parenthm.insert(parent_oid, hm);
            self.parents_to_children.insert(parent_table.to_string(), parenthm);
        }
    }

    pub fn update_edge(&mut self, child_table: &str, parent_table: &str,
                       child_rptr: HashedRowPtr, // will have been updated with new values
                       old_parent_oid: u64, 
                       new_parent_oid: Option<u64>, 
                       parent_col_index: usize) 
    {
        warn!("Updating edge from parent {} (child {}, col {} val {}) to new val {:?}", 
              parent_table, child_table, parent_col_index, old_parent_oid, new_parent_oid);
        // remove old edges from both directions
        if let Some(edges) = self.parents_to_children.get_mut(parent_table) {
            if let Some(typ2rows) = edges.get_mut(&old_parent_oid) {
                if let Some(rows) = typ2rows.get_mut(&(child_table.to_string(), parent_col_index)) {
                    // remove old child from this parent
                    rows.remove(&child_rptr); 
                } 
            } 
        }
        // if not none, insert new edge
        if let Some(np_oid) = new_parent_oid {
            self.add_edge(child_rptr, child_table, parent_table, np_oid, parent_col_index);
        }
    }

    pub fn get_children_of_parent(&self, parent_table: &str, parent_oid: u64) -> Option<ObjectTypeRows>
    {
        if let Some(edges) = self.parents_to_children.get(parent_table) {
            match edges.get(&parent_oid) {
                None => return None,
                Some(es) => return Some(es.clone()),
            }
        }
        None
    }
}
