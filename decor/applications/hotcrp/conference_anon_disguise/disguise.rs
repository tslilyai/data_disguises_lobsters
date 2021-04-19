use crate::conference_anon_disguise::constants::*;
use crate::datagen::*;
use crate::*;
use decor::disguises::*;
use decor::helpers::*;
use log::warn;
use mysql::TxOpts;
use sql_parser::ast::*;
use std::str::FromStr;

/*
 * CONFERENCE ANONYMIZATION DISGUISE
 */

pub fn undo(
    user_id: Option<u64>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = DisguiseEntry {
        disguise_id: CONF_ANON_DISGUISE_ID,
        user_id: 0,
        reverse: true,
    };

    // only reverse if disguise has been applied
    if !is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        insert_disguise_history_entry(&de, txn, stats)?;
    }
    Ok(())
}

pub fn undo_for_user(
    user_id: u64,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // Only decorrelated tables are "PaperReviewPreference" and "PaperWatch"

    /*
     * Get decorrelated children, and restore ownership
     */
    let mut children = get_user_entries_of_table_in_vault(
        user_id,
        "PaperReviewPreference",
        false, /* is_reversed */
        txn,
        stats,
    )?;
    children.append(&mut get_user_entries_of_table_in_vault(
        user_id,
        "PaperWatch",
        false, /* is_reversed */
        txn,
        stats,
    )?);
    // we need some way to be able to identify these objects...
    // assume that there is exactly one PaperWatch and one PaperReviewPreference for any user
    for c in children {
        let new_contact_id = get_value_of_col(&c.new_value, SCHEMA_UID_COL).unwrap();
        let old_contact_id = get_value_of_col(&c.old_value, SCHEMA_UID_COL).unwrap();
        assert!(old_contact_id == user_id.to_string());
        get_query_rows_txn(
            &Statement::Update(UpdateStatement {
                table_name: string_to_objname(&"PaperReviewPreference"),
                assignments: vec![Assignment {
                    id: Ident::new(SCHEMA_UID_COL.to_string()),
                    value: Expr::Value(Value::Number(user_id.to_string())),
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        SCHEMA_UID_COL.to_string(),
                    )])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(new_contact_id))),
                }),
            }),
            txn,
            stats,
        )?;
        mark_vault_entry_reversed(&c, txn, stats)?;
    }

    /*
     * Delete created guises from old disguise
     */
    let mut guise_ves = get_user_entries_with_referencer_in_vault(
        user_id,
        "PaperWatch",
        false, /* is_reversed */
        txn,
        stats,
    )?;
    guise_ves.append(&mut get_user_entries_with_referencer_in_vault(
        user_id,
        "PaperReviewPreference",
        false, /* is_reversed */
        txn,
        stats,
    )?);
    for guise in guise_ves {
        // TODO delete guise
        get_query_rows_txn(
            &Statement::Delete(DeleteStatement {
                table_name: string_to_objname(SCHEMA_UID_TABLE),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        SCHEMA_UID_COL.to_string(),
                    )])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(guise.guise_id.to_string()))),
                }),
            }),
            txn,
            stats,
        )?;
        // mark vault entries as reversed
        mark_vault_entry_reversed(&guise, txn, stats)?;
    }

    Ok(())
}

fn decor_obj_txn(
    tablefk: &TableFKs,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let child_name = &tablefk.name;
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
            vault_vals.push(VaultEntry {
                vault_id: 0,
                disguise_id: CONF_ANON_DISGUISE_ID,
                user_id: old_uid,
                guise_name: fk.fk_name.clone(),
                guise_id: guise_id,
                referencer_name: child_name.clone(),
                update_type: INSERT_GUISE,
                modified_cols: vec![],
                old_value: vec![],
                new_value: new_parent_rowvals,
                reversed: false,
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
            vault_vals.push(VaultEntry {
                vault_id: 0,
                disguise_id: CONF_ANON_DISGUISE_ID,
                user_id: old_uid,
                guise_name: child_name.clone(),
                guise_id: 0, // XXX nothing here for now
                referencer_name: "".to_string(),
                update_type: UPDATE_GUISE,
                modified_cols: vec![fk.referencer_col.clone()],
                old_value: child.clone(),
                new_value: new_child,
                reversed: false,
            });
        }

        /* PHASE 3: Batch vault updates */
        insert_vault_entries(&vault_vals, txn, stats)?;
    }
    Ok(())
}

pub fn apply(
    _: Option<u64>,
    db: &mut mysql::Conn,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let mut txn = db.start_transaction(TxOpts::default())?;

    // we should be able to reapply the conference anonymization disguise, in case more data has
    // been added in the meantime
    let de = DisguiseEntry {
        disguise_id: CONF_ANON_DISGUISE_ID,
        user_id: 0,
        reverse: false,
    };

    // DECORRELATION
    for tablefk in get_decor_names() {
        decor_obj_txn(&tablefk, &mut txn, stats)?;
    }
    
    insert_disguise_history_entry(&de, &mut txn, stats)?;

    txn.commit()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn apply_none() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::Warn)
            //.filter_level(log::LevelFilter::Error)
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();

        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url: String;
        let mut db: mysql::Conn;

        let test_dbname = "test_gdpr_none";
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        warn!("***************** POPULATING ****************");
        datagen::populate_database(&mut db).unwrap();
        warn!("***************** APPLYING CONF_ANON DISGUISE ****************");
        let mut stats = QueryStat::new();
        apply(None, &mut db, &mut stats).unwrap()
    }
}
