use crate::helpers::*;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VaultEntry {
    pub vault_id: u64,
    pub disguise_id: u64,
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

pub fn reverse_decor_ve(
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
    let mut vault_entries =
        get_user_entries_of_table_in_vault(user_id, referencer_table, conn, stats.clone())?;
    warn!(
        "ReverseDecor: User {} reversing {} entries of table {} in vault",
        user_id,
        vault_entries.len(),
        referencer_table
    );

    // we need some way to be able to identify these objects...
    // assume that there is exactly one object for any user?
    for ve in &vault_entries {
        if ve.update_type == DELETE_GUISE {
            continue;
        }

        // this may be none if this vault entry is an insert, and not a modification
        let new_id: String;
        let old_id: String;
        match get_value_of_col(&ve.new_value, referencer_col) {
            Some(n) => new_id = n,
            None => continue,
        }
        match get_value_of_col(&ve.old_value, referencer_col) {
            Some(n) => old_id = n,
            None => continue,
        }

        // XXX just to run tests for now
        if old_id != user_id.to_string() {
            warn!("old id {} != user id {}", old_id, user_id);
            continue;
        }
        //assert!(old_id == user_id.to_string());

        // this vault entry logged a modification to the FK. Restore the original value
        if ve.modified_cols.contains(&referencer_col.to_string()) {
            query_drop(
                Statement::Update(UpdateStatement {
                    table_name: string_to_objname(referencer_table),
                    assignments: vec![Assignment {
                        id: Ident::new(referencer_col.to_string()),
                        value: Expr::Value(Value::Number(user_id.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(
                            referencer_col.to_string(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(new_id))),
                    }),
                })
                .to_string(),
                conn,
                stats.clone(),
            )?;
            insert_reversed_vault_entry(&ve, conn, stats.clone());
        }
    }

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
    vault_entries.append(&mut guise_ves);
    Ok(vault_entries)
}
