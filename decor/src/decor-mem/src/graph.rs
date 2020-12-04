use std::*;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use crate::views::{HashedRowPtrs, HashedRowPtr};

pub type EntityTypeRows = Rc<RefCell<HashMap<String, HashedRowPtrs>>>;
// parent EID value to (types => rptrs of children)
pub type EntityEdges = HashMap<u64, EntityTypeRows>;
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
                    childrptr: HashedRowPtr, child_table: &str,  
                    parent_table: &str, parent_eid: u64) {
        if let Some(edges) = self.parents_to_children.get_mut(parent_table) {
            if let Some(typ2rows) = edges.get_mut(&parent_eid) {
                if let Some(rows) = typ2rows.borrow_mut().get_mut(child_table) {
                    rows.insert(childrptr);
                } else {
                    let mut hs = HashSet::new();
                    hs.insert(childrptr);
                    typ2rows.borrow_mut().insert(child_table.to_string(), hs);
                }
            } else {
                let mut hm = HashMap::new();
                let mut hs = HashSet::new();
                hs.insert(childrptr);
                hm.insert(child_table.to_string(), hs);
                edges.insert(parent_eid, Rc::new(RefCell::new(hm)));
            }
        } else {
            let mut parenthm = HashMap::new();
            let mut hm = HashMap::new();
            let mut hs = HashSet::new();
            hs.insert(childrptr);
            hm.insert(child_table.to_string(), hs);
            parenthm.insert(parent_eid, Rc::new(RefCell::new(hm)));
            self.parents_to_children.insert(parent_table.to_string(), parenthm);
        }
    }

    pub fn update_edge(&mut self, child_table: &str, parent_table: &str,
                       child_rptr: HashedRowPtr, // will have been updated with new values
                       old_parent_eid: u64, 
                       new_parent_eid: Option<u64>)
    {
        // remove old edges from both directions
        if let Some(edges) = self.parents_to_children.get_mut(parent_table) {
            if let Some(typ2rows) = edges.get_mut(&old_parent_eid) {
                if let Some(rows) = typ2rows.borrow_mut().get_mut(child_table) {
                    // remove old child from this parent
                    rows.remove(&child_rptr); 
                } 
            } 
        }
        // if not none, insert new edge
        if let Some(np_eid) = new_parent_eid {
            self.add_edge(child_rptr, child_table, parent_table, np_eid);
        }
    }

    pub fn get_children_of_parent(&self, parent_table: &str, parent_eid: u64) -> Option<&EntityTypeRows>
    {
        if let Some(edges) = self.parents_to_children.get(parent_table) {
            return edges.get(&parent_eid).clone();
        }
        None
    }
}