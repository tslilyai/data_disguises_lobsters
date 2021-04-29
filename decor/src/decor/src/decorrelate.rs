use crate::helpers::*;
use crate::is_guise;
use crate::stats::QueryStat;
use crate::types::*;
use crate::vault;
use log::warn;
use sql_parser::ast::*;
use std::str::FromStr;

// updates *all* FKs to the contactInfo table,
// instead of just the FKs that point to the user
pub fn decor_obj_txn(
    disguise: &Disguise,
    table_dis: &TableDisguise,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let child_name = &table_dis.name;
    let child_id_cols = table_dis.id_cols.clone();
    let fks = &table_dis.fks_to_decor;

    let mut vault_vals = vec![];
    for fk in fks {
        /* PHASE 1: SELECT REFERENCER OBJECTS */
        let child_objs = get_query_rows_txn(&select_statement(child_name, fk.decor_predicate), txn, stats)?;
        if child_objs.is_empty() {
            continue;
        }

        // get all the IDs of parents (all are of the same type for the same fk)
        let mut fkids = vec![];
        for child in &child_objs {
            fkids.push(
                u64::from_str(&get_value_of_col(child, &fk.referencer_col).unwrap()).unwrap(),
            );
        }
        warn!(
            "decor_obj_txn: Creating guises for fkids {:?} {:?}",
            fk, fkids
        );

        /*
         * PHASE 3: VAULT UPDATES
         * A) insert guises, associate with old parent uid
         * B) record update to child to point to new guise
         * */

        let fk_cols = (*disguise.guise_info.col_generation)();
        for (n, child) in child_objs.iter().enumerate() {
            let old_uid = fkids[n];
            // skip already decorrelated users
            if is_guise(&fk.fk_name, old_uid, txn, stats)? {
                warn!(
                    "decor_obj_txn: skipping decorrelation for {}.{}, already a guise",
                    fk.fk_name, old_uid
                );
                continue;
            }

            /*
             * PHASE 2: OBJECT MODIFICATIONS
             * A) insert guises for parents
             * B) update child to point to new guise
             * */
            
            // Phase 2A: insert new parent
            let new_parent = (*disguise.guise_info.val_generation)();
            get_query_rows_txn(
                &Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&fk.fk_name),
                    columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                    source: InsertSource::Query(Box::new(values_query(vec![new_parent.clone()]))),
                }),
                txn,
                stats,
            )?;
            let guise_id = txn.last_insert_id().unwrap();
            warn!("decor_obj_txn: inserted guise {}.{}", fk.fk_name, guise_id);

            // Phase 2B: update child to point to new parent
            get_query_rows_txn(
                &Statement::Update(UpdateStatement {
                    table_name: string_to_objname(&child_name),
                    assignments: vec![Assignment {
                        id: Ident::new(fk.referencer_col.clone()),
                        value: Expr::Value(Value::Number(guise_id.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![
                            Ident::new(child_name.clone()),
                            Ident::new(fk.referencer_col.clone()),
                        ])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(old_uid.to_string()))),
                    }),
                }),
                txn,
                stats,
            )?;

            let mut i = 0;
            // first turn new_fkobj into Vec<RowVal>
            let new_parent_rowvals: Vec<RowVal> = new_parent
                .iter()
                .map(|v| {
                    let index = i;
                    i += 1;
                    RowVal {
                        column: fk_cols[index].to_string(),
                        value: v.to_string(),
                    }
                })
                .collect();

            // Phase 3A: update the vault with new guise (calculating the uid from the last_insert_id)
            vault_vals.push(vault::VaultEntry {
                vault_id: 0,
                disguise_id: disguise.disguise_id,
                user_id: old_uid,
                guise_name: fk.fk_name.clone(),
                guise_id_cols: vec![disguise.guise_info.id_col.clone()],
                guise_ids: vec![guise_id.to_string()],
                referencer_name: child_name.clone(),
                update_type: vault::INSERT_GUISE,
                modified_cols: vec![],
                old_value: vec![],
                new_value: new_parent_rowvals,
                reverses: None,
            });

            // Phase 3B: update the vault with the modification to children
            let new_child: Vec<RowVal> = child
                .iter()
                .map(|v| {
                    if v.column == fk.referencer_col {
                        RowVal {
                            column: v.column.clone(),
                            value: guise_id.to_string(),
                        }
                    } else {
                        v.clone()
                    }
                })
                .collect();
            let child_ids = get_ids(table_dis, child);
            vault_vals.push(vault::VaultEntry {
                vault_id: 0,
                disguise_id: disguise.disguise_id,
                user_id: old_uid,
                guise_name: child_name.clone(),
                guise_id_cols: child_id_cols.clone(),
                guise_ids: child_ids,
                referencer_name: "".to_string(),
                update_type: vault::UPDATE_GUISE,
                modified_cols: vec![fk.referencer_col.clone()],
                old_value: child.clone(),
                new_value: new_child,
                reverses: None,
            });
        }
    }
    /* PHASE 3: Batch vault updates */
    vault::insert_vault_entries(&vault_vals, txn, stats)?;
    Ok(())
}
