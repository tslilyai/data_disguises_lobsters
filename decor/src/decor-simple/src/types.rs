use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::cmp::Ordering;
use std::cell::RefCell;
use std::collections::{HashSet};
use std::rc::Rc;
use std::hash::{Hash, Hasher};
use crate::helpers;

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct ObjectIdentifier {
    pub table: String,
    pub oid: u64,
}

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub child_table: String,
    pub col_index: usize,
    pub parent_table: String,
}

#[derive(Serialize, Deserialize, PartialOrd, Ord, Debug, Clone)]
pub struct ObjectData {
    pub name: ObjectIdentifier,
    pub row_strs: Vec<String>,
}
impl Hash for ObjectData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}
impl PartialEq for ObjectData {
    fn eq(&self, other: &ObjectData) -> bool {
        self.name == other.name
    }
}
impl Eq for ObjectData {} 

/*
 * Object, object data, and parent fk information 
 */
#[derive(Clone, Debug)]
pub struct TraversedObject {
    pub name: ObjectIdentifier,
    pub hrptr: HashedRowPtr,
    pub fk: ForeignKey,
    pub from_pc_edge: bool,
}
impl Hash for TraversedObject {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}
impl PartialEq for TraversedObject {
    fn eq(&self, other: &TraversedObject) -> bool {
        self.name == other.name
    }
}
impl Eq for TraversedObject {} 


/* 
 * Rows and pointers to in-memory rows
 */
pub type Row = Vec<Value>;
pub type RowPtr = Rc<RefCell<Row>>;
pub type RowPtrs = Vec<Rc<RefCell<Row>>>;

#[derive(Debug, Clone, Eq)]
pub struct HashedRowPtr(pub Rc<RefCell<Row>>, pub usize);
impl Hash for HashedRowPtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.borrow()[self.1].hash(state);
    }
}
impl Ord for HashedRowPtr {
    fn cmp(&self, other: &Self) -> Ordering {
        helpers::parser_vals_cmp(&self.0.borrow()[self.1], &other.0.borrow()[other.1])
    }
}
impl PartialOrd for HashedRowPtr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for HashedRowPtr {
    fn eq(&self, other: &Self) -> bool {
        self.0.borrow()[self.1] == other.0.borrow()[other.1]
    }
}
impl HashedRowPtr {
    pub fn row(&self) -> &Rc<RefCell<Row>> {
        &self.0
    }
    pub fn new(row: Rc<RefCell<Row>>, pki: usize) -> Self {
        HashedRowPtr(row.clone(), pki)
    }
    pub fn id(&self) -> u64 {
        helpers::parser_val_to_u64(&self.0.borrow()[self.1])
    }

    pub fn to_strs(&self) -> Vec<String> {
        self.row().borrow().iter().map(|v| v.to_string()).collect()
    }
}

pub type HashedRowPtrs = HashSet<HashedRowPtr>;


