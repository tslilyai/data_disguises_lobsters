use crate::disguise::*;
use crate::helpers::*;
use crate::stats::QueryStat;
use log::warn;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::sync::{Arc, Mutex};

pub const INSERT_GUISE: u64 = 0;
pub const DELETE_GUISE: u64 = 1;
pub const DECOR_GUISE: u64 = 3;
pub const UPDATE_GUISE: u64 = 4;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VaultEntry {
    pub pred: String,
    pub vault_id: u64,
    pub disguise_id: u64,
    pub priority: u64,
    pub user_id: u64,
    pub update_type: u64,
    // whether this disguise has been reversed at some point
    pub reversed: bool,

    // guise information
    pub guise_name: String,
    pub guise_owner_cols: Vec<String>,
    pub guise_id_cols: Vec<String>,
    pub guise_ids: Vec<String>,

    // for inserted guises
    pub referencer_name: String,

    // for decorrelations
    pub fk_name: String,
    pub fk_col: String,

    // for decorrelations and updates
    pub modified_cols: Vec<String>,

    // blobs
    pub old_value: Vec<RowVal>,
    pub new_value: Vec<RowVal>,
}

impl VaultEntry {
    fn reinsert_guise(
        &self,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<(), mysql::Error> {
        let mut cols = vec![];
        let mut vals = vec![];
        for rv in &self.old_value {
            cols.push(Ident::new(rv.column.clone()));
            // XXX treating everything like a string might backfire
            vals.push(Expr::Value(Value::String(rv.value.clone())));
        }
        query_drop(
            Statement::Insert(InsertStatement {
                table_name: string_to_objname(&self.guise_name),
                columns: cols,
                source: InsertSource::Query(Box::new(values_query(vec![vals]))),
            })
            .to_string(),
            conn,
            stats,
        )
    }

    fn recorrelate_guise(
        &self,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<(), mysql::Error> {
        warn!(
            "Recorrelating guise of table {} to {}, user {}",
            self.guise_name, self.fk_name, self.user_id
        );

        // this may be none if this vault entry is an insert, and not a modification
        let owner_col = &self.modified_cols[0];
        let new_val: String;
        let old_val: String;
        match get_value_of_col(&self.new_value, owner_col) {
            Some(n) => new_val = n,
            None => unimplemented!("Bad col name?"),
        }
        match get_value_of_col(&self.old_value, owner_col) {
            Some(n) => old_val = n,
            None => unimplemented!("Bad col name?"),
        }
        let updates = vec![Assignment {
            id: Ident::new(owner_col),
            value: Expr::Value(Value::Number(old_val)),
        }];
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(owner_col)])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number(new_val.clone()))),
        };
        query_drop(
            Statement::Update(UpdateStatement {
                table_name: string_to_objname(&self.guise_name),
                assignments: updates,
                selection: Some(selection),
            })
            .to_string(),
            conn,
            stats.clone(),
        )?;
        //insert_reversed_vault_entry(&ve, conn, stats.clone());

        /*
         * Delete created guises if objects in this table had been decorrelated
         * TODO can make per-guise-table, rather than assume that only users are guises
         */
        // delete guise
        query_drop(
            Statement::Delete(DeleteStatement {
                table_name: string_to_objname(&self.fk_name),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(self.fk_col.to_string())])),
                    op: BinaryOperator::Eq,
                    // XXX assuming guise is a user... only has one id
                    right: Box::new(Expr::Value(Value::Number(new_val.clone()))),
                }),
            })
            .to_string(),
            conn,
            stats.clone(),
        )?;
        // mark vault entries as reversed
        //insert_reversed_vault_entry(&ve, conn, stats.clone());
        Ok(())
    }

    pub fn restore_ownership(
        &self,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<(), mysql::Error> {
        match self.update_type {
            DECOR_GUISE => self.recorrelate_guise(conn, stats.clone())?,
            DELETE_GUISE => self.reinsert_guise(conn, stats.clone())?,
            _ => unimplemented!("Bad update type"),
        }
        Ok(())
    }

    // if this vault entry modifies or removes something that this disguise predicate
    // depends on, then we have a RAW conflict
    pub fn conflicts_with(&self, disguise: &Disguise) -> bool {
        // a disguise can only conflict with prior disguises of lower priority
        if self.priority >= disguise.priority {
            return false;
        }
        if self.modified_cols.is_empty() {
            return false;
        }
        if self.update_type == DELETE_GUISE || self.update_type == DECOR_GUISE {
            for (_, td) in disguise.table_disguises.clone() {
                let td_locked = td.read().unwrap();
                // if this table disguise isn't of the conflicting table, ignore
                if self.guise_name != td_locked.name {
                    continue;
                }
                for t in &td_locked.transforms {
                    for col in &self.modified_cols {
                        if t.pred.contains(col) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}

pub fn ves_to_bytes(ves: &Vec<VaultEntry>) -> Vec<u8> {
    let s = serde_json::to_string(ves).unwrap();
    s.as_bytes().to_vec()
}

pub fn vec_to_expr<T: Serialize>(vs: &Vec<T>) -> Expr {
    if vs.is_empty() {
        Expr::Value(Value::Null)
    } else {
        let serialized = serde_json::to_string(&vs).unwrap();
        Expr::Value(Value::String(serialized))
    }
}

/*pub fn reverse_decor_ve(
    referencer_table: &str,
    referencer_col: &str,
    fktable: &str,
    fkcol: &str,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    // TODO assuming that all FKs point to users

    /*
     * Undo modifications to objects of this table
     * TODO undo any vault modifications that were dependent on this one, namely "filters" that
     * join with this "filter" (any updates that happened after this?)
     */

}*/
