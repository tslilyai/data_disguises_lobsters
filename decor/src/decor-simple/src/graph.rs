use std::*;
use std::collections::{HashMap, HashSet};
use crate::views::{HashedRowPtrs, HashedRowPtr};
use log::{warn};

/* entity type (parent table+parentcol) => children rptrs */
pub type EntityTypeRows = HashMap<(String, usize), HashedRowPtrs>;

/* parent EID value to ((table+parentcol)=> rptrs of children) */
pub type EntityEdges = HashMap<u64 , EntityTypeRows>;
pub struct EntityGraph {
    // map from parent type => map of parent id => children rptrs (map of type -> rptrs)
    pub parents_to_children: HashMap<String, EntityEdges>,
}

impl EntityGraph {
    pub fn new() -> Self {
        EntityGraph {
            parents_to_children: HashMap::new(),
        }
    }
    pub fn add_edge(&mut self, 
                    childrptr: HashedRowPtr, 
                    child_table: &str,  
                    parent_table: &str, 
                    parent_eid: u64, 
                    parent_col_index: usize) {
        warn!("Adding edge from {} col {} val {} to {} val {:?}", parent_table, parent_col_index, parent_eid, child_table, childrptr);
        if let Some(edges) = self.parents_to_children.get_mut(parent_table) {
            if let Some(typ2rows) = edges.get_mut(&parent_eid) {
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
                edges.insert(parent_eid, hm);
            }
        } else {
            let mut parenthm = HashMap::new();
            let mut hm = HashMap::new();
            let mut hs = HashSet::new();
            hs.insert(childrptr);
            hm.insert((child_table.to_string(), parent_col_index), hs);
            parenthm.insert(parent_eid, hm);
            self.parents_to_children.insert(parent_table.to_string(), parenthm);
        }
    }

    pub fn update_edge(&mut self, child_table: &str, parent_table: &str,
                       child_rptr: HashedRowPtr, // will have been updated with new values
                       old_parent_eid: u64, 
                       new_parent_eid: Option<u64>, 
                       parent_col_index: usize) 
    {
        warn!("Updating edge from {} col {} val {} to new val {:?}, child {} {:?}", parent_table, parent_col_index, old_parent_eid, new_parent_eid, child_table, child_rptr);
        // remove old edges from both directions
        if let Some(edges) = self.parents_to_children.get_mut(parent_table) {
            if let Some(typ2rows) = edges.get_mut(&old_parent_eid) {
                if let Some(rows) = typ2rows.get_mut(&(child_table.to_string(), parent_col_index)) {
                    // remove old child from this parent
                    rows.remove(&child_rptr); 
                } 
            } 
        }
        // if not none, insert new edge
        if let Some(np_eid) = new_parent_eid {
            self.add_edge(child_rptr, child_table, parent_table, np_eid, parent_col_index);
        }
    }

    pub fn get_children_of_parent(&self, parent_table: &str, parent_eid: u64) -> Option<EntityTypeRows>
    {
        if let Some(edges) = self.parents_to_children.get(parent_table) {
            match edges.get(&parent_eid) {
                None => return None,
                Some(es) => return Some(es.clone()),
            }
        }
        None
    }
}
