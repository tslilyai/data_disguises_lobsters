use std::collections::{HashMap};
use sql_parser::ast::*;
use crate::types::*;

pub const TARGET: &'static str = "target";

pub type Predicate = Expr;

pub type PairTypePred = Box<dyn Fn(&Row, &Row) -> bool>;

pub struct PairPredicate {
    type1_pred: Option<Predicate>,
    type2_pred: Option<Predicate>,
    pair_pred: Option<PairTypePred>,
}

pub fn ids_satisfy_pair_predicate(id1: &ID, id2: &ID, pred: &PairPredicate, db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
    if let Some(p) = &pred.pair_pred {
        let row1 : Row;
        let row2 : Row;
        if id1.table < id2.table {
            row1 = id1.get_row(db)?;
            row2 = id2.get_row(db)?;
        } else {
            row1 = id2.get_row(db)?;
            row2 = id1.get_row(db)?;
        }
        return Ok(p(&row1, &row2));
    }
    // no pair predicate, return true
    Ok(true)
}

/*
 * Always "acts" on second type of pair
 */
pub enum Action {
    Modify,
    Delete,
    SplitOff,
    SplitOffDelete,
}

pub struct SinglePolicy {
    pub action: Action,
    pub predicate: Predicate, 
    pub modifications: Option<GuiseModifications>,
}

pub struct PairPolicy {
    pub action: Action,
    pub predicate: PairPredicate, 
    pub modifications: Option<GuiseModifications>,
}

pub struct TableInfo {
    pub referencers: Vec<ForeignKeyCol>,
    pub id_col_info: TableCol,
    pub columns: Vec<TableCol>,
}

pub struct SchemaConfig {
    pub user_table: String,
    pub table_info: HashMap<TableName, TableInfo>,
    pub single_policies: HashMap<TableName, Vec<SinglePolicy>>,
    pub pair_policies: HashMap<TableNamePair, Vec<PairPolicy>>,
}
