use crate::conference_anon_disguise::constants::*;
use crate::*;
use decor::disguises::*;
use decor::helpers::*;
use mysql::TxOpts;
use sql_parser::ast::*;

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

    /*
     * PHASE 2: SELECT REFERENCED OBJECTS
     * Noop: we don't need the value of these objects of perform guise inserts
     */

    for fk in fks {
        // get all the IDs of parents (all are of the same type for the same fk)
        let mut fkids = vec![];
        for child in &child_objs {
            let fkid: Vec<&RowVal> = child.iter().filter(|rc| rc.column == fk.fk_col).collect();
            fkids.push(Expr::Value(Value::Number(fkid[0].value.to_string())));
        }

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
        if !new_parents_vals.is_empty() {
            get_query_rows_txn(
                &Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&fk.fk_name),
                    columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                    source: InsertSource::Query(Box::new(values_query(new_parents_vals.clone()))),
                }),
                &mut txn,
            )?;
        }

        let last_uid = txn.last_insert_id().unwrap();
        let mut cur_uid = last_uid - child_objs.len() as u64;

        // Update children one-by-one
        // Collect inputs for batch inserts to vault
        let mut vault_vals = vec![];
        for (n, child) in child_objs.iter().enumerate() {
            cur_uid += 1;
            let old_uid = &fkids[n];

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
                        right: Box::new(old_uid.clone()),
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

            // Phase 4A: update the vault with new guise (calculating the uid from the last_insert_id)
            let mut guise_vault_vals = vec![];
            // uid
            guise_vault_vals.push(old_uid.clone());
            // modifiedObjectName
            guise_vault_vals.push(Expr::Value(Value::String(fk.fk_name.clone())));
            // modified all columns
            guise_vault_vals.push(Expr::Value(Value::Null));
            // old value
            guise_vault_vals.push(Expr::Value(Value::Null));
            // new value
            let serialized = serde_json::to_string(&new_parent_rowvals).unwrap();
            guise_vault_vals.push(Expr::Value(Value::String(serialized)));
            vault_vals.push(guise_vault_vals);

            // Phase 4B: update the vault with the modification to children
            let mut child_vault_vals = vec![];
            // uid
            child_vault_vals.push(old_uid.clone());
            // modifiedObjectName
            child_vault_vals.push(Expr::Value(Value::String(fk.fk_name.clone())));
            // modified fk column
            child_vault_vals.push(Expr::Value(Value::String(
                serde_json::to_string(&vec![fk.referencer_col.clone()]).unwrap(),
            )));
            // old value
            child_vault_vals.push(Expr::Value(Value::String(
                serde_json::to_string(&child).unwrap(),
            )));
            // new value
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
            child_vault_vals.push(Expr::Value(Value::String(
                serde_json::to_string(&new_child).unwrap(),
            )));
            vault_vals.push(child_vault_vals);
        }

        /* PHASE 4B: Batch vault updates */
        if !vault_vals.is_empty() {
            get_query_rows_txn(
                &Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&table_to_vault(&fk.fk_name)),
                    columns: get_insert_vault_colnames(),
                    source: InsertSource::Query(Box::new(values_query(vault_vals))),
                }),
                &mut txn,
            )?;
        }
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
        create_schema(&mut db).unwrap();

        datagen::insert_contact_info(2, &mut db).unwrap();

        apply(None, &mut db).unwrap()
    }
}
