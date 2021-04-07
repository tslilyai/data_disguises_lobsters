use crate::disguise::SCHEMA_UID_COL;
use decor::disguises::*;
use decor::helpers::*;
use mysql::TxOpts;
use rand::{distributions::Alphanumeric, Rng};
use sql_parser::ast::*; // 0.8

const ANON_PW: &'static str = "password123";
fn get_contact_info_cols() -> Vec<&'static str> {
    vec![
        "firstName",
        "lastName",
        "unaccentedName",
        "email",
        "preferredEmail",
        "affiliation",
        "orcid",
        "phone",
        "country",
        "password",
        "passwordTime",
        "passwordUseTime",
        "collaborators",
        "updateTime",
        "lastLogin",
        "defaultWatch",
        "roles",
        "disabled",
        "contactTags",
        "data",
        "primaryContactId",
    ]
}
fn get_remove_names() -> Vec<&'static str> {
    vec![
        "ContactInfo", // we remove users after their data has been anonymized
        "PaperReviewPreference",
        "PaperWatch",
        "Capability",
        "PaperConflict",
        "TopicInterest",
        "PaperTag",
        "PaperTagAnno",
    ]
}

fn get_contact_info_vals() -> Vec<Expr> {
    vec![
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(get_random_email())),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(ANON_PW.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(2.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::Number(0.to_string())),
    ]
}

fn select_statement(name: &str, selection: Option<Expr>) -> Statement {
    Statement::Select(SelectStatement {
        query: Box::new(Query::select(Select {
            distinct: true,
            projection: vec![SelectItem::Wildcard],
            from: str_to_tablewithjoins(&name),
            selection: selection.clone(),
            group_by: vec![],
            having: None,
        })),
        as_of: None,
    })
}

fn get_random_email() -> String {
    let randstr: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();
    format!("anonymous{}@secret.mail", randstr)
}

pub fn apply_conference_anon_disguise(db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    // REMOVAL TXNS
    for name in get_remove_names() {
        let mut txn = db.start_transaction(TxOpts::default())?;
        let selection = None;

        let predicated_objs = get_query_rows_txn(&select_statement(name, None), &mut txn)?;

        get_query_rows_txn(
            &Statement::Delete(DeleteStatement {
                table_name: string_to_objname(name),
                selection: selection,
            }),
            &mut txn,
        )?;

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
            // modifiedObjectName
            evals.push(Expr::Value(Value::String(name.to_string())));
            // old value
            let serialized = serde_json::to_string(&objrow).unwrap();
            evals.push(Expr::Value(Value::String(serialized)));
            // new value is deleted
            evals.push(Expr::Value(Value::Null));
            vault_vals.push(evals);
        }
        get_query_rows_txn(
            &Statement::Insert(InsertStatement {
                table_name: string_to_objname(&table_to_vault(name)),
                columns: get_insert_vault_colnames(),
                source: InsertSource::Query(Box::new(values_query(vault_vals))),
            }),
            &mut txn,
        )?;
        txn.commit()?;
    }

    // DECORRELATION TXNS
    for tablefk in get_decor_names() {
        let mut txn = db.start_transaction(TxOpts::default())?;
        let predicated_objs = get_query_rows_txn(&select_statement(&tablefk.name, None), &mut txn)?;

        // insert new users for all foreign keys for all objects
        let fk_cols = get_contact_info_cols();
        for fk in tablefk.fks {
            let mut old_fk_objs = vec![];
            let mut new_fk_objs = vec![];

            for objrow in &predicated_objs {
                let mut uid = String::from("Null");
                // get current uid value
                for v in objrow {
                    if v.column == fk.fk_col {
                        uid = v.value.clone();
                        break;
                    }
                }

                // get current fk object pointed to
                let mut fkobj = get_query_rows_txn(
                &select_statement(&fk.fk_name, 
                            Some(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(vec![
                                    Ident::new(fk.fk_name.clone()),
                                    Ident::new(fk.fk_col.clone()),
                                ])),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
                            })),&mut txn,
                )?;
                old_fk_objs.append(&mut fkobj);

                // generate new values for fk object for specified columns
                new_fk_objs.push(get_contact_info_vals());
            }

            // actually insert the guises into the DB
            get_query_rows_txn(
                &Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&fk.fk_name),
                    columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                    source: InsertSource::Query(Box::new(values_query(new_fk_objs))),
                }),
                &mut txn,
            )?;

            // update the vault with new guises (calculating the uid from the last_insert_id)
            let last_uid = txn.last_insert_id().unwrap();
            let mut cur_uid = last_uid - old_fk_objs.len() as u64 + 1;

            // insert one vault record for each new obj, going from Null to obj values
            let mut vault_vals = vec![];
            for (n, old_fkobj) in old_fk_objs.iter().enumerate() {
                // TODO turn new_fkobj into Vec<RowVal>
                //let new_fkobj = new_fk_objs[n];
                //new_fkobj.push_front(Value::Number(cur_uid.to_string()));
                cur_uid += 1;

                // insert on behalf of the user that is being "removed"
                let mut old_uid = String::new();
                let mut found = false;
                for colval in old_fkobj {
                    if colval.column == SCHEMA_UID_COL {
                        old_uid = colval.value.clone();
                        found = true;
                        break;
                    }
                }
                assert!(found);

                let mut evals = vec![];
                // uid
                evals.push(Expr::Value(Value::Number(old_uid)));
                // modifiedObjectName
                evals.push(Expr::Value(Value::String(fk.fk_name.clone())));
                // old value
                evals.push(Expr::Value(Value::Null));
                // new value TODO
                let serialized = serde_json::to_string("").unwrap();
                evals.push(Expr::Value(Value::String(serialized)));
                vault_vals.push(evals);
            }
            get_query_rows_txn(
                &Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&table_to_vault(&fk.fk_name)),
                    columns: get_insert_vault_colnames(),
                    source: InsertSource::Query(Box::new(values_query(vault_vals))),
                }),
                &mut txn,
            )?;
        }
        txn.commit()?;
    }
    Ok(())
}

fn get_decor_names() -> Vec<TableFKs> {
    vec![
        TableFKs {
            name: "PaperReviewRefused".to_string(),
            fks: vec![
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "refusedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableFKs {
            name: "ActionLog".to_string(),
            fks: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "destContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "trueContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableFKs {
            name: "ReviewRating".to_string(),
            fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableFKs {
            name: "PaperComment".to_string(),
            fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableFKs {
            name: "PaperReview".to_string(),
            fks: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
    ]
}

/*
fn remove_vault_updates_closure(
    objName: &str,
    old_row: Vec<RowVal>,
    new_row: Vec<RowVal>,
) -> Vec<Statement> {
    let mut uid = -1;

    match objName {
        "ContactInfo"
        | "PaperConflict"
        | "TopicInterest"
        | "Capability"
        | "PaperWatch"
        | "PaperReviewPreference" => {
            // these all have a fk column to "contactID"
            for v in old_row {
                if v.column == SCHEMA_UID_COL {
                    uid = v.value;
                    break;
                }
            }

            let mut evals = vec![];

            // uid
            evals.push(Expr::Value(Value::Number(uid)));
            // modifiedObjectName
            evals.push(Expr::Value(Value::String(objname)));
            // old value
            let serialized_old = serde_json::to_string(&old_row).unwrap();
            evals.push(Expr::Value(Value::String(serialized_old)));
            // new value
            evals.push(Expr::Value(Value::Null));

            vec![Statement::Insert(InsertStatement {
                table_name: helpers::str_to_objname(&format!(VAULT_FMT_STR, name)),
                columns: VAULT_COL_NAMES,
                source: InsertSource::Query(Box::new(values_query(evals))),
            })]
        }
        // not sure what to do here---remove tags with user's name?
        // would need to query the ContactInfo table and use result
        "PaperTag" | "PaperTagAnno" => vec![],
        _ => vec![],
    }
}*/
