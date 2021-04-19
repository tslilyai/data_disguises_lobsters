use crate::conference_anon_disguise;
use crate::datagen::*;
use crate::gdpr_disguise::constants::*;
use crate::*;
use decor::disguises::*;
use decor::helpers::stats::QueryStat;
use decor::helpers::*;
use mysql::TxOpts;
use sql_parser::ast::*;

/*
 * GDPR REMOVAL DISGUISE
 */

pub fn undo(
    user_id: Option<u64>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // user must be provided as input
    let user_id = user_id.unwrap();

    let de = DisguiseEntry {
        disguise_id: GDPR_DISGUISE_ID,
        user_id: user_id,
        reverse: true,
    };

    // only apply if disguise has been applied
    if !is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        insert_disguise_history_entry(&de, txn, stats)?;
    }
    Ok(())
}

fn remove_obj_txn(
    user_id: u64,
    name: &str,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let selection = Some(Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![
            Ident::new(SCHEMA_UID_COL.to_string()), // assumes fkcol is uid_col
        ])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
    });

    /*
     * PHASE 1: OBJECT SELECTION
     */
    let predicated_objs =
        get_query_rows_txn(&select_statement(name, selection.clone()), txn, stats)?;

    /* PHASE 2: OBJECT MODIFICATION */
    get_query_rows_txn(
        &Statement::Delete(DeleteStatement {
            table_name: string_to_objname(SCHEMA_UID_TABLE),
            selection: selection,
        }),
        txn,
        stats,
    )?;

    /* PHASE 3: VAULT UPDATES */
    let mut vault_vals = vec![];
    for objrow in &predicated_objs {
        vault_vals.push(VaultEntry {
            vault_id: 0,
            disguise_id: GDPR_DISGUISE_ID,
            user_id: user_id,
            guise_name: name.to_string(),
            guise_id: 0,
            referencer_name: "".to_string(),
            update_type: DELETE_GUISE,
            modified_cols: vec![],
            old_value: objrow.clone(),
            new_value: vec![],
            reversed: false,
        });
    }
    insert_vault_entries(&vault_vals, txn, stats)?;
    Ok(())
}

fn decor_obj_txn(
    user_id: u64,
    tablefk: &TableFKs,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let child_name = tablefk.name.clone();
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
            vault_vals.push(VaultEntry {
                vault_id: 0,
                disguise_id: GDPR_DISGUISE_ID,
                user_id: user_id,
                guise_name: fk.fk_name.clone(),
                guise_id: cur_uid,
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
                            value: cur_uid.to_string(),
                        }
                    } else {
                        v.clone()
                    }
                })
                .collect();
            vault_vals.push(VaultEntry {
                vault_id: 0,
                disguise_id: GDPR_DISGUISE_ID,
                user_id: user_id,
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
    user_id: Option<u64>,
    db: &mut mysql::Conn,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // user must be provided as input
    let user_id = user_id.unwrap();

    let mut txn = db.start_transaction(TxOpts::default())?;

    let de = DisguiseEntry {
        disguise_id: GDPR_DISGUISE_ID,
        user_id: user_id,
        reverse: false,
    };

    // only apply if disguise is reversed (or hasn't been applied)
    if is_disguise_reversed(&de, &mut txn, stats)? {
        // UNDO PRIOR DISGUISES
        // TODO check if previous disguises have been applied; if so, undo them for this user
        conference_anon_disguise::undo(Some(user_id), &mut txn, stats)?;

        // DECORRELATION TXNS
        for tablefk in get_decor_names() {
            decor_obj_txn(user_id, &tablefk, &mut txn, stats)?;
        }

        // REMOVAL TXNS
        for name in get_remove_names() {
            remove_obj_txn(user_id, name, &mut txn, stats)?;
        }

        insert_disguise_history_entry(&de, &mut txn, stats)?;
    }

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

        let test_dbname = "test_conf_none";
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        datagen::populate_database(&mut db).unwrap();
        let mut stats = QueryStat::new();
        apply(Some(1), &mut db, &mut stats).unwrap()
    }
}
