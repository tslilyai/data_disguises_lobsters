use crate::datagen::*;
use crate::*;
use decor::vault;
use decor::types::*;
use decor::stats::QueryStat;
use decor::helpers::*;
use sql_parser::ast::*;
use std::str::FromStr;

pub fn decor_obj_txn(
    disguise_id: u64,
    tablefk: &TableFKs,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let child_name = &tablefk.name;
    let child_id_cols = tablefk.id_cols.clone();
    let fks = &tablefk.fks;

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let child_objs = get_query_rows_txn(&select_statement(child_name, None), txn, stats)?;
    // no selected objects, return
    if child_objs.is_empty() {
        return Ok(());
    }

    for fk in fks {
        // get all the IDs of parents (all are of the same type for the same fk)
        let mut fkids = vec![];
        for child in &child_objs {
            fkids.push(
                u64::from_str(&get_value_of_col(child, &fk.referencer_col).unwrap()).unwrap(),
            );
        }
        warn!("decor_obj_txn: Creating guises for fkids {:?} {:?}", fk, fkids);

        /*
         * PHASE 2: OBJECT MODIFICATIONS
         * A) insert guises for parents
         * B) update child to point to new guise
         * */

        /*
         * PHASE 3: VAULT UPDATES
         * A) insert guises, associate with old parent uid
         * B) record update to child to point to new guise
         * */

        // Phase 2
        let fk_cols = get_guise_contact_info_cols();
        let mut vault_vals = vec![];
        for (n, child) in child_objs.iter().enumerate() {
            let old_uid = fkids[n];

            if is_guise(&fk.fk_name, old_uid, txn, stats)? {
                warn!("decor_obj_txn: skipping decorrelation for {}.{}, already a guise", fk.fk_name, old_uid);
                continue;
            }

            // if parent of this fk is not a guise, then insert a new parent and update child
            let new_parent = get_guise_contact_info_vals();
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

            // Phase 3B: update child to point to new parent
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
                disguise_id: disguise_id,
                user_id: old_uid,
                guise_name: fk.fk_name.clone(),
                // XXX assume is ContactInfo table guise
                guise_id_cols: vec![SCHEMA_UID_COL.to_string()],
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
            let child_ids = child_id_cols.iter().map(|id_col| get_value_of_col(child, &id_col).unwrap()).collect();
            vault_vals.push(vault::VaultEntry {
                vault_id: 0,
                disguise_id: disguise_id,
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

        /* PHASE 3: Batch vault updates */
        vault::insert_vault_entries(&vault_vals, txn, stats)?;
    }
    Ok(())
}


pub fn decor_obj_txn_for_user(
    user_id: u64,
    disguise_id: u64,
    tablefk: &TableFKs,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let child_name = tablefk.name.clone();
    let child_id_cols = tablefk.id_cols.clone();
    let fks = &tablefk.fks;

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let mut selection = Expr::Value(Value::Boolean(false));
    for fk in fks {
        selection = Expr::BinaryOp {
            left: Box::new(selection),
            op: BinaryOperator::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new(
                    fk.referencer_col.to_string(),
                )])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
            }),
        };
    }
    let child_objs =
        get_query_rows_txn(&select_statement(&child_name, Some(selection)), txn, stats)?;
    if child_objs.is_empty() {
        return Ok(());
    }

    for fk in fks {
        /*
         * PHASE 2: OBJECT MODIFICATIONS
         * A) insert guises for parents
         * B) update child to point to new guise
         * */

        /*
         * PHASE 3: VAULT UPDATES
         * A) insert guises, associate with old parent uid
         * B) record update to child to point to new guise
         * */

        // Phase 2A: batch insertion of parents
        // We can batch here because we know all selected children have not been decorrelated from
        // the parent (which is the user user_id) yet
        let mut new_parents_vals = vec![];
        let fk_cols = get_guise_contact_info_cols();
        for _ in &child_objs {
            new_parents_vals.push(get_guise_contact_info_vals());
        }
        assert!(!new_parents_vals.is_empty());
        get_query_rows_txn(
            &Statement::Insert(InsertStatement {
                table_name: string_to_objname(&fk.fk_name),
                columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                source: InsertSource::Query(Box::new(values_query(new_parents_vals.clone()))),
            }),
            txn,
            stats,
        )?;

        let last_uid = txn.last_insert_id().unwrap();
        let mut cur_uid = last_uid - child_objs.len() as u64;

        let mut vault_vals = vec![];
        for (n, child) in child_objs.iter().enumerate() {
            cur_uid += 1;

            // Phase 2B: update child to point to new parent
            get_query_rows_txn(
                &Statement::Update(UpdateStatement {
                    table_name: string_to_objname(&child_name),
                    assignments: vec![Assignment {
                        id: Ident::new(fk.referencer_col.clone()),
                        value: Expr::Value(Value::Number(cur_uid.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(
                            fk.referencer_col.clone(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                    }),
                }),
                txn,
                stats,
            )?;

            let mut i = 0;
            // first turn new_fkobj into Vec<RowVal>
            let new_parent_rowvals: Vec<RowVal> = new_parents_vals[n]
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

            // Phase 3A: update the vault with new guises (calculating the uid from the last_insert_id)
            vault_vals.push(vault::VaultEntry {
                vault_id: 0,
                disguise_id: disguise_id,
                user_id: user_id,
                guise_name: fk.fk_name.clone(),
                // XXX assume this is a user guise
                guise_id_cols: vec![SCHEMA_UID_COL.to_string()],
                guise_ids: vec![cur_uid.to_string()],
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
                            value: cur_uid.to_string(),
                        }
                    } else {
                        v.clone()
                    }
                })
                .collect();
            let child_ids = child_id_cols.iter().map(|id_col| get_value_of_col(child, &id_col).unwrap()).collect();
            vault_vals.push(vault::VaultEntry {
                vault_id: 0,
                disguise_id: disguise_id,
                user_id: user_id,
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

        /* PHASE 3: Batch vault updates */
        vault::insert_vault_entries(&vault_vals, txn, stats)?;
    }
    Ok(())
}
