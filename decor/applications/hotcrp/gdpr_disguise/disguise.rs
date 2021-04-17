use crate::gdpr_disguise::constants::*;
use crate::*;
use decor::disguises::*;
use decor::helpers::*;
use mysql::TxOpts;
use sql_parser::ast::*;

/*
 * GDPR REMOVAL DISGUISE
 */

fn undo_previous_disguises(user_id: u64, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    // Only decorrelated tables are "PaperReviewPreference" and "PaperWatch"
    // select modified entries from vault 
    let mut txn = db.start_transaction(TxOpts::default())?;
    let vault_entries = get_user_entries_in_vault(user_id, &mut txn)?;

    // select introduced guises from vault

    // remove all guise entries

    // mark as reversed entries from vault
    
    Ok(())
}

fn remove_obj_txn(user_id: u64, name: &str, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let mut txn = db.start_transaction(TxOpts::default())?;
    /* 
     * PHASE 1: REFERENCER SELECTION 
     * */
    let predicated_objs = get_query_rows_txn(
        &select_statement(
            name,
            Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![
                    Ident::new(name.clone()),
                    Ident::new(SCHEMA_UID_COL.to_string()), // assumes fkcol is uid_col
                ])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
            }),
        ),
        &mut txn,
    )?;

    /* PHASE 2: REFERENCED SELECTION */
    // noop because we're dealing only with a single table, and not with any fks

    /* PHASE 3: OBJECT MODIFICATION */
    get_query_rows_txn(
        &Statement::Delete(DeleteStatement {
            table_name: string_to_objname(name),
            selection: None,
        }),
        &mut txn,
    )?;

    /* PHASE 4: VAULT UPDATES */
    let mut vault_vals = vec![];
    for objrow in &predicated_objs {
        vault_vals.push(VaultEntry {
                user_id: user_id,
                guise_name: name.to_string(),
                guise_id: 0,
                referencer_name: "".to_string(),
                update_type: DELETE_GUISE,
                modified_cols: vec![],
                old_value: objrow.clone(),
                new_value: vec![], 
            });
    }
    insert_vault_entries(&vault_vals, &mut txn)?;
    txn.commit()
}

fn decor_obj_txn(
    user_id: u64,
    tablefk: &TableFKs,
    db: &mut mysql::Conn,
) -> Result<(), mysql::Error> {
    let child_name = tablefk.name.clone();
    let fks = &tablefk.fks;
    let mut txn = db.start_transaction(TxOpts::default())?;

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let mut selection = Expr::Value(Value::Boolean(false));
    for fk in fks {
        selection = Expr::BinaryOp {
            left: Box::new(selection),
            op: BinaryOperator::Or,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![
                    Ident::new(child_name.clone()),
                    Ident::new(fk.referencer_col.to_string()),
                ])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
            }),
        };
    }
    let child_objs = get_query_rows_txn(&select_statement(&child_name, Some(selection)), &mut txn)?;
    if child_objs.is_empty() {
        return Ok(());
    }

    /* PHASE 2: SELECT REFERENCED OBJECTS */
    // noop---we don't need the value of these objects of perform guise inserts

    for fk in fks {

        /*
         * PHASE 3: OBJECT MODIFICATIONS
         * A) insert guises for parents
         * B) update child to point to new guise
         * */

        /*
         * PHASE 4: VAULT UPDATES
         * A) insert guises, associate with old parent uid
         * B) record update to child to point to new guise
         * */

        // Phase 3A: batch insertion of parents
        let mut new_parents_vals = vec![];
        let fk_cols = get_contact_info_cols();
            
        for _ in &child_objs {
            new_parents_vals.push(get_contact_info_vals());
        }
        assert!(!new_parents_vals.is_empty());
        get_query_rows_txn(
            &Statement::Insert(InsertStatement {
                table_name: string_to_objname(&fk.fk_name),
                columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                source: InsertSource::Query(Box::new(values_query(new_parents_vals.clone()))),
            }),
            &mut txn,
        )?;
    
        let last_uid = txn.last_insert_id().unwrap();
        let mut cur_uid = last_uid - child_objs.len() as u64;

        let mut vault_vals = vec![];
        for (n, child) in child_objs.iter().enumerate() {
            cur_uid += 1;

            // Phase 3B: update child to point to new parent
            get_query_rows_txn(
                &Statement::Update(UpdateStatement {
                    table_name: string_to_objname(&child_name),
                    assignments: vec![Assignment {
                        id: Ident::new(fk.referencer_col.clone()),
                        value: Expr::Value(Value::Number(cur_uid.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![
                            Ident::new(child_name.clone()),
                            Ident::new(fk.referencer_col.clone()),
                        ])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                    }),
                }),
                &mut txn,
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
            
            // Phase 4A: update the vault with new guises (calculating the uid from the last_insert_id)
            vault_vals.push(VaultEntry {
                user_id: user_id,
                guise_name: fk.fk_name.clone(),
                guise_id: cur_uid,
                referencer_name: child_name.clone(),
                update_type: INSERT_GUISE,
                modified_cols: vec![],
                old_value: vec![],
                new_value: new_parent_rowvals, 
            });

            // Phase 4B: update the vault with the modification to children
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
                user_id: user_id,
                guise_name: child_name.clone(),
                guise_id: 0, // XXX nothing here for now
                referencer_name: "".to_string(),
                update_type: UPDATE_GUISE,
                modified_cols: vec![fk.referencer_col.clone()],
                old_value: child.clone(),
                new_value: new_child, 
            });
        }

        /* PHASE 4B: Batch vault updates */
        insert_vault_entries(&vault_vals, &mut txn)?;
    }
    txn.commit()
}

pub fn apply(user_id: Option<u64>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    // user must be provided as input
    let user_id = user_id.unwrap();

    undo_previous_disguises(user_id, db)?;

    // DECORRELATION TXNS
    for tablefk in get_decor_names() {
        decor_obj_txn(user_id, &tablefk, db)?;
    }
    // REMOVAL TXNS
    for name in get_remove_names() {
        remove_obj_txn(user_id, name, db)?;
    }

    Ok(())
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
        let url : String;
        let mut db : mysql::Conn;
          
        let test_dbname = "test_conf_none";
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname)).unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname)).unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        datagen::populate_database(&mut db).unwrap();
        apply(Some(1), &mut db).unwrap()
    }
}
