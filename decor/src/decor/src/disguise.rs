use crate::helpers::*;
use crate::history;
use crate::stats::*;
use crate::types::Transform::*;
use crate::types::*;
use crate::vault;
use crate::*;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub fn apply(
    user_id: Option<u64>,
    disguise: &Disguise,
    pool: &mysql::Pool,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<(), mysql::Error> {
    let de = history::DisguiseEntry {
        user_id: user_id.unwrap_or(0),
        disguise_id: disguise.disguise_id,
        reverse: false,
    };

    let mut threads = vec![];

    let mut vault_vals = vec![];
    for table in &disguise.table_disguises {
        let mut items: HashMap<Vec<RowVal>, Vec<&Transform>> = HashMap::new();
        let mut removed_items: HashSet<Vec<RowVal>> = HashSet::new();

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

        /*
         * PHASE 1: OBJECT SELECTION
         * Assign each object its assigned transformations
         */
        for (pred, transform) in &table.transforms {
            let pred_items = get_query_rows(&select_statement(&table.name, &pred), &pool, stats.clone())?;

            // just remove item if it's supposed to be removed
            match transform {
                Remove => {
                    // reverse all possible decorrelations for this table
                    let start = time::Instant::now();

                    /* PHASE 2: OBJECT MODIFICATION */
                    helpers::query_drop(
                        Statement::Delete(DeleteStatement {
                            table_name: string_to_objname(&table.name),
                            selection: pred.clone(),
                        })
                        .to_string(),
                        pool,
                        &mut threads,
                        stats.clone(),
                    );
                    stats.lock().unwrap().remove_dur += start.elapsed();

                    /* PHASE 3: VAULT UPDATES */
                    // XXX removal entries get stored in *all* vaults????
                    for i in &pred_items {
                        if disguise.is_reversible {
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
                        }

                        // EXTRA: save in removed so we don't
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
                    Decor {
                        referencer_col,
                        fk_name,
                        ..
                    } => {
                        let start = time::Instant::now();

                        /*
                         * PHASE 2: OBJECT MODIFICATIONS
                         * A) insert guises for parents
                         * B) update child to point to new guise
                         * */

                        // get ID of parent
                        let old_uid =
                            u64::from_str(&get_value_of_col(&i, &referencer_col).unwrap()).unwrap();
                        warn!(
                            "decor_obj {}: Creating guises for fkids {:?} {:?}",
                            table.name, fk_name, old_uid,
                        );
                        let fk_cols = (*disguise.guise_info.col_generation)();

                        // skip already decorrelated users
                        if is_guise(&fk_name, &disguise.guise_info.id_col, old_uid, pool, stats.clone())? {
                            warn!(
                                "decor_obj: skipping decorrelation for {}.{}, already a guise",
                                fk_name, old_uid
                            );
                            continue;
                        }
                        // Phase 2A: insert new parent
                        let new_parent = (*disguise.guise_info.val_generation)();
                        let stmt = Statement::Insert(InsertStatement {
                            table_name: string_to_objname(&fk_name),
                            columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                            source: InsertSource::Query(Box::new(values_query(vec![
                                new_parent.clone()
                            ]))),
                        })
                        .to_string();
                        query_drop(stmt, pool, &mut threads, stats.clone());

                        // TODO 
                        let guise_id = 0;//pool.last_insert_id().unwrap();

                        warn!("decor_obj: inserted guise {}.{}", fk_name, guise_id);

                        // Phase 2B: update child to point to new parent
                        query_drop(
                            Statement::Update(UpdateStatement {
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
                            })
                            .to_string(),
                            pool,
                            &mut threads,
                            stats.clone(),
                        );

                        /*
                         * PHASE 3: VAULT UPDATES
                         * A) insert guises, associate with old parent uid
                         * B) record update to child to point to new guise
                         * */
                        // Phase 3A: update the vault with new guise (calculating the uid from the last_insert_id)
                        if disguise.is_reversible {
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
                        }
                        stats.lock().unwrap().decor_dur += start.elapsed();
                    }

                    Modify {
                        col,
                        generate_modified_value,
                        ..
                    } => {
                        let start = time::Instant::now();

                        let old_val = get_value_of_col(&i, &col).unwrap();
                        let new_val = (*(generate_modified_value))(&old_val);
                        let selection = get_select_of_row(&table.id_cols, &i);

                        /*
                         * PHASE 2: OBJECT MODIFICATIONS
                         * */
                        query_drop(
                            Statement::Update(UpdateStatement {
                                table_name: string_to_objname(&table.name),
                                assignments: vec![Assignment {
                                    id: Ident::new(col.clone()),
                                    value: Expr::Value(Value::String(new_val.clone())),
                                }],
                                selection: Some(selection),
                            })
                            .to_string(),
                            pool,
                            &mut threads,
                            stats.clone(),
                        );

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
                        if disguise.is_reversible {
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
                        stats.lock().unwrap().mod_dur += start.elapsed();
                    }
                    _ => unimplemented!("Removes should already have been performed!"),
                }
            }
        }
    }
    let start = time::Instant::now();
    vault::insert_vault_entries(&vault_vals, pool, &mut threads, stats.clone());
    record_disguise(&de, pool, &mut threads, stats.clone())?;
    
    // wait until all mysql queries are done
    for jh in threads.into_iter() {
        assert!(jh.join().is_ok());
    }
    stats.lock().unwrap().record_dur += start.elapsed();
    Ok(())
}

pub fn undo(
    user_id: Option<u64>,
    disguise_id: u64,
    pool: &mysql::Pool,
    stats: Arc<Mutex<QueryStat>>,
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
    if !history::is_disguise_reversed(&de, pool, stats.clone())? {
        // TODO undo disguise

        record_disguise(&de, pool, &mut vec![], stats.clone())?;
    }
    Ok(())
}
