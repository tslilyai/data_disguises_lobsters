use crate::conference_anon_disguise::constants::*;
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

fn decor_obj_txn(tablefk: &TableFKs, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let child_name = &tablefk.name;
    let fks = &tablefk.fks;
    let mut txn = db.start_transaction(TxOpts::default())?;

    /*
     * PHASE 0: PREAMBLE
     * Noop: Should not undo any deletions; all decorrelations already occurred
     */

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let child_objs = get_query_rows_txn(&select_statement(child_name, None), &mut txn)?;
    // no selected objects, return
    if child_objs.is_empty() {
        return Ok(());
    }

    for fk in fks {
        // get all the IDs of parents (all are of the same type for the same fk)
        let mut fkids = vec![];
        for child in &child_objs {
            for rc in child {
                warn!(
                    "Checking {} = {} ? {}",
                    rc.column,
                    fk.referencer_col,
                    rc.column == fk.referencer_col
                );
                if rc.column == fk.referencer_col {
                    warn!("Adding {} to fkids", rc.value);
                    fkids.push(rc.value.parse::<u64>().unwrap());
                }
            }
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
        // last_insert_id returns the ID of the first inserted value
        let mut cur_uid = txn.last_insert_id().unwrap();
        warn!(
            "last inserted id was {}, number children was {}",
            cur_uid,
            child_objs.len()
        );

        // Update children one-by-one
        // Collect inputs for batch inserts to vault
        let mut vault_vals = vec![];
        for (n, child) in child_objs.iter().enumerate() {
            cur_uid += 1;
            let old_uid = fkids[n];

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
                        right: Box::new(Expr::Value(Value::Number(old_uid.to_string()))),
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

            // Phase 3A: update the vault with new guise (calculating the uid from the last_insert_id)
            vault_vals.push(VaultEntry {
                vault_id: 0,
                user_id: old_uid,
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
        insert_vault_entries(&vault_vals, &mut txn)?;
    }
    txn.commit()
}

pub fn apply(_: Option<u64>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    // DECORRELATION TXNS
    for tablefk in get_decor_names() {
        decor_obj_txn(&tablefk, db)?;
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
        warn!("***************** APPLYING CONFANON DISGUISE ****************");
        apply(None, &mut db).unwrap()
    }
}
