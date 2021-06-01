use crate::helpers::*;
use crate::stats::*;
use crate::types::Transform::*;
use crate::types::*;
use crate::vault;
use crate::*;
use crate::history;
use std::str::FromStr;
use std::collections::{HashMap, HashSet};

pub fn apply(
    user_id: Option<u64>,
    disguise: &Disguise,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = history::DisguiseEntry {
        user_id: user_id.unwrap_or(0),
        disguise_id: disguise.disguise_id,
        reverse: false,
    };

    let mut vault_vals = vec![];
    for table in &disguise.table_disguises {
        let mut items : HashMap<Vec<RowVal>, Vec<&Transform>> = HashMap::new();
        let mut removed_items : HashSet<Vec<RowVal>> = HashSet::new();

        /*
         * PHASE 1: OBJECT SELECTION
         * Assign each object its assigned transformations
         */
        for (pred, transform) in &table.transforms {
            let pred_items = get_query_rows_txn(&select_statement(&table.name, &pred), txn, stats)?;

            // just remove item if it's supposed to be removed
            match transform {
                Remove => {
                    // reverse all possible decorrelations for this table
                    let start = time::Instant::now();
                    /*for (ref_table, ref_col) in &disguise.guise_info.referencers {
                        if ref_table == &table.name {
                            vault::reverse_vault_decor_referencer_entries(
                                disguise.user_id,
                                &ref_table,
                                &ref_col, 
                                // assume just one id col for user
                                &table.name,
                                &table.id_cols[0], 
                                txn,
                                stats,
                            )?;
                         }
                    }*/
                    stats.undo_dur += start.elapsed();

                    let start = time::Instant::now();

                    /* PHASE 2: OBJECT MODIFICATION */
                    helpers::query_drop_txn(
                        &Statement::Delete(DeleteStatement {
                            table_name: string_to_objname(&table.name),
                            selection: pred.clone(),
                        }).to_string(),
                        txn,
                        stats,
                    )?;
                    stats.remove_dur += start.elapsed();

                    /* PHASE 3: VAULT UPDATES */
                    // XXX removal entries get stored in *all* vaults????
                    let mut vault_vals = vec![];
                    for i in &pred_items {
                        let ids = get_ids(&table.id_cols, i);
                        for owner_col in &table.owner_cols {
                            let uid = get_value_of_col(&i, &owner_col).unwrap();
                            if (*disguise.is_owner)(&uid) {
                                vault_vals.push(vault::VaultEntry {
                                    vault_id: 0,
                                    disguise_id: disguise.disguise_id,
                                    user_id: u64::from_str(&uid).unwrap(),
                                    guise_name: table.name.clone(),
                                    guise_id_cols: table.id_cols.clone(),
                                    guise_ids: ids.clone(),
                                    referencer_name: "".to_string(),
                                    update_type: vault::DELETE_GUISE,
                                    modified_cols: vec![],
                                    old_value: i.clone(),
                                    new_value: vec![],
                                    reverses: None,
                                });
                            }
                        }
                    
                        // some extra stuff: save in remvoed so we don't 
                        // transform this item further
                        items.remove(i);
                        removed_items.insert(i.to_vec());
                    }
                }
                _ => {
                    for i in pred_items {
                        if removed_items.contains(&i) {
                            continue;
                        }
                        if let Some(ts) = items.get_mut(&i) {
                            ts.push(transform);
                        } else {
                            items.insert(i, vec![transform]);
                        }
                    }
                }
            }
        }
        for (i, ts) in items {
            for t in ts {
                match t {
                    Remove => {
                        /*
                         * PHASE 0: What vault operations must come "after" removal?
                         * ==> Those that have made the object to remove inaccessible, namely those that would have
                         * satisfied the predicate, but no longer do.
                         *
                         * TODO also undo any operations that happened in that disguise after these decorrelation
                         * modifications? This only is correct if all decorrelated FKs are to the contactInfo table
                         * Note: we don't need to redo these because deletion is final!
                         * TODO need a way to figure out how to get fks to recorrelate...
                         */
                                    }
                    Decor {
                        referencer_col,
                        fk_name,
                        ..
                    } => {
                        /*
                         * PHASE 0: REVERSE ANY PRIOR DECORRELATED ENTRIES
                         * FOR TESTING ONLY: WE KNOW THESE ARE GOING TO BE DECORRELATED AGAIN
                        for (ref_table, ref_col) in &disguise.guise_info.referencers {
                            if ref_table == &table.name {
                                vault::reverse_vault_decor_referencer_entries(
                                    disguise.user_id,
                                    &ref_table,
                                    &ref_col, 
                                    // assume just one id col for user
                                    &table.name,
                                    &table.id_cols[0], 
                                    txn,
                                    stats,
                                )?;
                             }
                        }
                        */
                        /* PHASE 1: SELECT REFERENCER OBJECTS */
                        let start = time::Instant::now();

                        // get all the IDs of parents (all are of the same type for the same fk)
                        let old_uid = u64::from_str(&get_value_of_col(&i, &referencer_col).unwrap()).unwrap();
                        warn!(
                            "decor_obj_txn {}: Creating guises for fkids {:?} {:?}",
                            table.name, fk_name, old_uid,
                        );

                        let fk_cols = (*disguise.guise_info.col_generation)();
                        stats.decor_dur_pred += start.elapsed();

                        // skip already decorrelated users
                        let start = time::Instant::now();
                        if is_guise(&fk_name, &disguise.guise_info.id_col, old_uid, txn, stats)? {
                            warn!(
                                "decor_obj_txn: skipping decorrelation for {}.{}, already a guise",
                                fk_name, old_uid
                            );
                            continue;
                        }
                        stats.decor_dur_guise += start.elapsed();

                        /*
                         * PHASE 2: OBJECT MODIFICATIONS
                         * A) insert guises for parents
                         * B) update child to point to new guise
                         * */

                        // Phase 2A: insert new parent
                        let start = time::Instant::now();
                        let new_parent = (*disguise.guise_info.val_generation)();
                        stats.decor_dur_2A_1 += start.elapsed();
                        let stmt = Statement::Insert(InsertStatement {
                                table_name: string_to_objname(&fk_name),
                                columns: fk_cols
                                    .iter()
                                    .map(|c| Ident::new(c.to_string()))
                                    .collect(),
                                source: InsertSource::Query(Box::new(values_query(vec![
                                    new_parent.clone(),
                                ])))}).to_string();
                        let start = time::Instant::now();
                        query_drop_txn(&stmt,
                            txn,
                            stats,
                        )?;
                        stats.decor_dur_2A_2 += start.elapsed();
                        let start = time::Instant::now();
                        let guise_id = txn.last_insert_id().unwrap();
                        stats.decor_queries_2A += 1;
                        stats.decor_dur_2A_3 += start.elapsed();
                        warn!("decor_obj_txn: inserted guise {}.{}", fk_name, guise_id);

                        // Phase 2B: update child to point to new parent
                        let start = time::Instant::now();
                        query_drop_txn(
                            &Statement::Update(UpdateStatement {
                                table_name: string_to_objname(&table.name),
                                assignments: vec![Assignment {
                                    id: Ident::new(referencer_col.clone()),
                                    value: Expr::Value(Value::Number(guise_id.to_string())),
                                }],
                                selection: Some(Expr::BinaryOp {
                                    left: Box::new(Expr::Identifier(vec![
                                        Ident::new(table.name.clone()),
                                        Ident::new(referencer_col.clone()),
                                    ])),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(Expr::Value(Value::Number(
                                        old_uid.to_string(),
                                    ))),
                                }),
                            }).to_string(),
                            txn,
                            stats,
                        )?;
                        stats.decor_queries_2B += 1;
                        stats.decor_dur_2B += start.elapsed();

                        /*
                         * PHASE 3: VAULT UPDATES
                         * A) insert guises, associate with old parent uid
                         * B) record update to child to point to new guise
                         * */
                        // Phase 3A: update the vault with new guise (calculating the uid from the last_insert_id)
                        let start = time::Instant::now();
                        let mut ix = 0;
                        let new_parent_rowvals: Vec<RowVal> = new_parent
                            .iter()
                            .map(|v| {
                                let index = ix;
                                ix += 1;
                                RowVal {
                                    column: fk_cols[index].to_string(),
                                    value: v.to_string(),
                                }
                            })
                            .collect();
                        vault_vals.push(vault::VaultEntry {
                            vault_id: 0,
                            disguise_id: disguise.disguise_id,
                            user_id: old_uid,
                            guise_name: fk_name.clone(),
                            guise_id_cols: vec![disguise.guise_info.id_col.clone()],
                            guise_ids: vec![guise_id.to_string()],
                            referencer_name: table.name.clone(),
                            update_type: vault::INSERT_GUISE,
                            modified_cols: vec![],
                            old_value: vec![],
                            new_value: new_parent_rowvals,
                            reverses: None,
                        });

                        // Phase 3B: update the vault with the modification to children
                        let new_child: Vec<RowVal> = i 
                            .iter()
                            .map(|v| {
                                if &v.column == referencer_col {
                                    RowVal {
                                        column: v.column.clone(),
                                        value: guise_id.to_string(),
                                    }
                                } else {
                                    v.clone()
                                }
                            })
                            .collect();
                        let child_ids = get_ids(&table.id_cols, &i);
                        vault_vals.push(vault::VaultEntry {
                            vault_id: 0,
                            disguise_id: disguise.disguise_id,
                            user_id: old_uid,
                            guise_name: table.name.clone(),
                            guise_id_cols: table.id_cols.clone(),
                            guise_ids: child_ids,
                            referencer_name: "".to_string(),
                            update_type: vault::UPDATE_GUISE,
                            modified_cols: vec![referencer_col.clone()],
                            old_value: i.clone(),
                            new_value: new_child,
                            reverses: None,
                        });
                        stats.decor_dur_3 += start.elapsed();
                        stats.decor_dur += start.elapsed();
                    }
                    
                    Modify {
                        col,
                        generate_modified_value,
                        ..
                    } => {
                        /* PHASE 1: SELECT REFERENCER OBJECTS */
                        let old_val = get_value_of_col(&i, &col).unwrap();
                        let new_val = (*(generate_modified_value))(&old_val);

                        let selection = get_select_of_row(&table.id_cols, &i);

                        /*
                         * PHASE 2: OBJECT MODIFICATIONS
                         * */
                        query_drop_txn(
                            &Statement::Update(UpdateStatement {
                                table_name: string_to_objname(&table.name),
                                assignments: vec![Assignment {
                                    id: Ident::new(col.clone()),
                                    value: Expr::Value(Value::String(new_val.clone())),
                                }],
                                selection: Some(selection),
                            }).to_string(),
                            txn,
                            stats,
                        )?;

                        /*
                         * PHASE 3: VAULT UPDATES
                         * */
                        let new_obj: Vec<RowVal> = i 
                            .iter()
                            .map(|v| {
                                if &v.column == col {
                                    RowVal {
                                        column: v.column.clone(),
                                        value: new_val.clone(),
                                    }
                                } else {
                                    v.clone()
                                }
                            })
                            .collect();

                        // XXX insert a vault entry for every owning user (every fk)
                        // should just update for the calling user, if there is one?
                        let ids = get_ids(&table.id_cols, &i);
                        for owner_col in &table.owner_cols {
                            let uid = get_value_of_col(&i, &owner_col).unwrap();
                            if (*disguise.is_owner)(&uid) {
                                vault_vals.push(vault::VaultEntry {
                                    vault_id: 0,
                                    disguise_id: disguise.disguise_id,
                                    user_id: u64::from_str(&uid).unwrap(),
                                    guise_name: table.name.clone(),
                                    guise_id_cols: table.id_cols.clone(),
                                    guise_ids: ids.clone(),
                                    referencer_name: "".to_string(),
                                    update_type: vault::UPDATE_GUISE,
                                    modified_cols: vec![col.clone()],
                                    old_value: i.clone(),
                                    new_value: new_obj.clone(),
                                    reverses: None,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    let start = time::Instant::now();
    vault::insert_vault_entries(&vault_vals, txn, stats)?;
    record_disguise(&de, txn, stats)?;
    stats.record_dur += start.elapsed();
    Ok(())
}

pub fn undo(
    user_id: Option<u64>,
    disguise_id: u64,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = history::DisguiseEntry {
        disguise_id: disguise_id,
        user_id: match user_id {
            Some(u) => u,
            None => 0,
        },
        reverse: true,
    };

    // only reverse if disguise has been applied
    if !history::is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        record_disguise(&de, txn, stats)?;
    }
    Ok(())
}
