use crate::helpers::*;
use crate::disguise::*;
use crate::stats::QueryStat;
use crate::vaults::*;
use log::warn;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::sync::{Arc, Mutex};

pub const INSERT_GUISE: u64 = 0;
pub const DELETE_GUISE: u64 = 1;
pub const UPDATE_GUISE: u64 = 2;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VaultEntry {
    pub vault_id: u64,
    pub disguise_id: u64,
    pub priority: u64,
    pub user_id: u64,
    pub guise_name: String,
    pub guise_id_cols: Vec<String>,
    pub guise_ids: Vec<String>,
    pub referencer_name: String,
    pub update_type: u64,
    pub modified_cols: Vec<String>,
    pub old_value: Vec<RowVal>,
    pub new_value: Vec<RowVal>,
    pub reverses: Option<u64>,
}

impl VaultEntry {
    pub fn revert_token(&self, conn: &mut mysql::PooledConn, stats: Arc<Mutex<QueryStat>>) -> Result<(), mysql::Error> {
        /*warn!(
            "ReverseDecor: User {} reversing {} entries of table {}",
            self.user_id,
            self.guise_name,
        );

        // this may be none if this vault entry is an insert, and not a modification
        let mut updates = vec![];
        let mut selects = Expr::Value(Value::Bool(true)); 
        for col in self.modified_cols {
            let new_val: String;
            let old_val: String;
            match get_value_of_col(&ve.new_value, referencer_col) {
                Some(n) => new_val = n,
                None => unimplemented!("Bad col name?"),
            }
            match get_value_of_col(&ve.old_value, referencer_col) {
                Some(n) => old_val = n,
                None => unimplemented!("Bad col name?"),
            }
            updates.push(Assignment {
                id: Ident::new(col),
                value: Expr::Value(Value::Number(old_val)),
            });
            selects = Expr::BinaryOp {
                left: Box::new(selects),
                op: BinaryOperator::And,
                right::Box::new(
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(col)])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(new_val))),
                })
            };
        }
        query_drop(
            Statement::Update(UpdateStatement {
                table_name: string_to_objname(self.guise_name),
                assignments: updates,
                selection: Some(selects),
            }).to_string(),
            conn,
            stats.clone(),
        )?;
        insert_reversed_vault_entry(&ve, conn, stats.clone());

        /*
         * Delete created guises if objects in this table had been decorrelated
         * TODO can make per-guise-table, rather than assume that only users are guises
         */
        let mut guise_ves =
            get_user_entries_with_referencer_in_vault(user_id, referencer_table, conn, stats.clone())?;
        warn!(
            "ReverseDecor: User {} reversing {} entries with referencer {} in vault",
            user_id,
            vault_entries.len(),
            referencer_table
        );
        for ve in &guise_ves {
            // delete guise
            query_drop(
                Statement::Delete(DeleteStatement {
                    table_name: string_to_objname(fktable),
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(fkcol.to_string())])),
                        op: BinaryOperator::Eq,
                        // XXX assuming guise is a user... only has one id
                        right: Box::new(Expr::Value(Value::Number(ve.guise_ids[0].clone()))),
                    }),
                })
                .to_string(),
                conn,
                stats.clone(),
            )?;
            // mark vault entries as reversed
            insert_reversed_vault_entry(&ve, conn, stats.clone());
        }
        vault_entries.append(&mut guise_ves);*/
        Ok(())    
    }

    pub fn apply_token(&self, conn: &mut mysql::PooledConn, stats: Arc<Mutex<QueryStat>>) -> Result<(), mysql::Error> {
        Ok(())
    }

    // if this vault entry modifies or removes something that this disguise predicate
    // depends on, then we have a RAW conflict
    pub fn conflicts_with(&self, disguise: &Disguise) -> bool {
        // a disguise can only conflict with prior disguises of lower priority
        if self.priority >= disguise.priority {
            return false;
        }
        for td in &disguise.table_disguises {
            let td_locked = td.read().unwrap();
            // if this table disguise isn't of the conflicting table, ignore
            if self.guise_name != td_locked.name {
                continue;
            }
            for t in &td_locked.transforms {
                match &t.pred {
                    Some(p) => {
                        for c in &get_expr_idents(&p) {
                            if self.modified_cols.iter().find(|col| &c == col).is_some() {
                                return true;
                            }
                        }
                    },
                    None => (),
                }
            }
        }
        false
    }
}

fn ve_to_bytes(ve: &VaultEntry) -> Vec<u8> {
    let s = serde_json::to_string(ve).unwrap();
    s.as_bytes().to_vec()
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
