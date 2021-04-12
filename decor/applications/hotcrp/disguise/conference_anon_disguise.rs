use crate::disguise::*;
use decor::disguises::*;
use decor::helpers::*;
use mysql::TxOpts;
use sql_parser::ast::*;

fn remove_obj_txn(name: &str, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let mut txn = db.start_transaction(TxOpts::default())?;

    /* PHASE 0: PREAMBLE */
    // TODO undo any dependent disguises (XXX touches all vaults??)

    /* PHASE 1: REFERENCER SELECTION */
    let predicated_objs = get_query_rows_txn(&select_statement(name, None), &mut txn)?;
    
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
        let mut uid = String::new();
        for v in objrow {
            if &v.column == SCHEMA_UID_COL {
                uid = v.value.clone();
                break;
            }
        }
        let mut evals = vec![];
        // uid
        evals.push(Expr::Value(Value::Number(uid.to_string())));
        // name
        evals.push(Expr::Value(Value::String(name.to_string())));
        // modified columns
        evals.push(Expr::Value(Value::Null));
        // old value
        let serialized = serde_json::to_string(&objrow).unwrap();
        evals.push(Expr::Value(Value::String(serialized)));
        // new value
        evals.push(Expr::Value(Value::Null));
    }
    get_query_rows_txn(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname(&table_to_vault(name)),
            columns: get_insert_vault_colnames(),
            source: InsertSource::Query(Box::new(values_query(vault_vals))),
        }),
        &mut txn,
    )?;
    txn.commit()
}

fn decor_obj_txn(tablefk: &TableFKs, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let child_name = &tablefk.name;
    let fks = &tablefk.fks;
    let mut txn = db.start_transaction(TxOpts::default())?;

    /* PHASE 0: PREAMBLE */
    // TODO undo any dependent disguises (XXX touches all vaults??)

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let child_objs = get_query_rows_txn(&select_statement(child_name, None), &mut txn)?;

    /* PHASE 2: SELECT REFERENCED OBJECTS */
    // noop---we don't need the value of these objects of perform guise inserts
    
    /* PHASE 3: OBJECT MODIFICATIONS */
    /* PHASE 4: VAULT UPDATES */
    for fk in fks {
        // get all the IDs of parents (all are of the same type for the same fk)
        let mut fkids = vec![];
        for child in &child_objs {
            let fkid: Vec<&RowVal> = child.iter().filter(|rc| rc.column == fk.fk_col).collect();
            fkids.push(Expr::Value(Value::Number(fkid[0].value.to_string())));
        }

        // Phase 3 insert guises for parents
        let mut new_parents_vals = vec![];
        let fk_cols = get_contact_info_cols();
        for _ in &child_objs {
            new_parents_vals.push(get_contact_info_vals());
        }
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
            let old_uid = &fkids[n];

            // Phase 3 update child to point to new parent
            get_query_rows_txn(
                &Statement::Update(UpdateStatement {
                    table_name: string_to_objname(&child_name),
                    assignments: vec![Assignment {
                        id: Ident::new(fk.referencer_col.clone()),
                        value: Expr::Value(Value::Number(cur_uid.to_string())),
                    }],
                    // TODO should use indexed child ID column
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

            // Phase 4: update the vault with new guises (calculating the uid from the last_insert_id)
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

            // Phase 4: update the vault with the modification to children
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
            let new_child : Vec<RowVal> = child
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
        // Phase 4: batched updates for all children together
        get_query_rows_txn(
            &Statement::Insert(InsertStatement {
                table_name: string_to_objname(&table_to_vault(&fk.fk_name)),
                columns: get_insert_vault_colnames(),
                source: InsertSource::Query(Box::new(values_query(vault_vals))),
            }),
            &mut txn,
        )?;
    }
    txn.commit()
}

pub fn apply_conference_anon_disguise(_: Option<u64>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    // DECORRELATION TXNS
    for tablefk in get_decor_names() {
        decor_obj_txn(&tablefk, db)?;
    }
    
    // REMOVAL TXNS
    for name in get_remove_names() {
        remove_obj_txn(name, db)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn apply_none() {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut jh = None;
        let url : String;
        let mut db : mysql::Conn;
          
        let test_dbname = "test_none";
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname)).unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname)).unwrap();
        assert_eq!(db.ping(), true);
        create_schema(&mut db).unwrap();
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);

        assert_eq(apply_conference_anon_disguise(None, &db), Ok(()));
    }
}
