use crate::helpers::*;
use crate::stats::QueryStat;
use crate::types::*;
use log::{debug, warn, error};
use mysql::prelude::*;
use mysql::TxOpts;
use serde::Serialize;
use sql_parser::ast::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;

pub const VAULT_TABLE: &'static str = "VaultTable";
pub const INSERT_GUISE: u64 = 0;
pub const DELETE_GUISE: u64 = 1;
pub const UPDATE_GUISE: u64 = 2;

#[derive(Default, Clone, Debug)]
pub struct VaultEntry {
    pub vault_id: u64,
    pub disguise_id: u64,
    pub user_id: u64,
    pub guise_name: String,
    pub guise_id_cols: Vec<String>,
    pub guise_ids: Vec<String>,
    pub referencer_name: String,
    pub update_type: u64,
    pub modified_cols: Vec<String>,
    pub old_value: Vec<RowVal>,
    pub new_value: Vec<RowVal>,
    pub reverses: Option<u64>,
}

fn vec_to_expr<T: Serialize>(vs: &Vec<T>) -> Expr {
    if vs.is_empty() {
        Expr::Value(Value::Null)
    } else {
        let serialized = serde_json::to_string(&vs).unwrap();
        Expr::Value(Value::String(serialized))
    }
}

fn get_vault_entries_with_constraint(
    constraint: Expr,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let rows = get_query_rows_txn(
        &select_ordered_statement(VAULT_TABLE, Some(constraint), "vaultId"),
        txn,
        stats,
    )?;
    let mut ves = vec![];
    for row in rows {
        let mut ve: VaultEntry = Default::default();
        debug!("GetVaultEntries: got entry {:?}", ve);
        for rv in row {
            match rv.column.as_str() {
                "vaultId" => ve.vault_id = u64::from_str(&rv.value).unwrap(),
                "disguiseId" => ve.disguise_id = u64::from_str(&rv.value).unwrap(),
                "userId" => ve.user_id = u64::from_str(&rv.value).unwrap(),
                "guiseName" => ve.guise_name = rv.value.clone(),
                "guiseIdCols" => {
                    ve.guise_id_cols = if &rv.value != NULLSTR {
                        serde_json::from_str(&rv.value).unwrap()
                    } else {
                        vec![]
                    }
                }
                "guiseIds" => {
                    ve.guise_ids = if &rv.value != NULLSTR {
                        serde_json::from_str(&rv.value).unwrap()
                    } else {
                        vec![]
                    }
                }
                "referencerName" => ve.referencer_name = rv.value.clone(),
                "updateType" => ve.update_type = u64::from_str(&rv.value).unwrap(),
                "modifiedCols" => {
                    ve.modified_cols = if &rv.value != NULLSTR {
                        serde_json::from_str(&rv.value).unwrap()
                    } else {
                        vec![]
                    }
                }
                "oldValue" => {
                    ve.old_value = if &rv.value != NULLSTR {
                        serde_json::from_str(&rv.value).unwrap()
                    } else {
                        vec![]
                    }
                }
                "newValue" => {
                    ve.new_value = if &rv.value != NULLSTR {
                        serde_json::from_str(&rv.value).unwrap()
                    } else {
                        vec![]
                    }
                }
                "reverses" => {
                    ve.reverses = if &rv.value != NULLSTR {
                        Some(u64::from_str(&rv.value).unwrap())
                    } else {
                        None
                    }
                }
                _ => unimplemented!("Incorrect column name! {:?}", rv),
            };
        }
        ves.push(ve);
    }
    Ok(ves)
}

fn insert_reversed_vault_entry(
    ve: &VaultEntry,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    warn!("Reversing {:?}", ve);
    let mut evals = vec![];
    // store vault entry metadata
    evals.push(Expr::Value(Value::Number(ve.disguise_id.to_string())));
    evals.push(Expr::Value(Value::Number(ve.user_id.to_string())));
    evals.push(Expr::Value(Value::String(ve.guise_name.clone())));
    evals.push(vec_to_expr(&ve.guise_id_cols));
    evals.push(vec_to_expr(&ve.guise_ids));
    evals.push(Expr::Value(Value::String(ve.referencer_name.clone())));
    evals.push(Expr::Value(Value::Number(ve.update_type.to_string())));
    // but don't actually store the updates
    evals.push(Expr::Value(Value::Null));
    evals.push(Expr::Value(Value::Null));
    evals.push(Expr::Value(Value::Null));
    evals.push(Expr::Value(Value::Number(ve.vault_id.to_string())));
    let vault_vals: Vec<Vec<Expr>> = vec![evals];
    get_query_rows_txn(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname(VAULT_TABLE),
            columns: get_insert_vault_colnames(),
            source: InsertSource::Query(Box::new(values_query(vault_vals))),
        }),
        txn,
        stats,
    )?;
    Ok(())
}

/*
 * Returns unreversed vault entries belonging to the user that modified this table and had the specified referencer
 */
fn get_user_entries_with_referencer_in_vault(
    uid: u64,
    referencer_table: &str,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let equal_uid_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("userId")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
    };
    let ref_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("referencerName")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::String(referencer_table.to_string()))),
    };
    let final_constraint = Expr::BinaryOp {
        left: Box::new(equal_uid_constraint),
        op: BinaryOperator::And,
        right: Box::new(ref_constraint),
    };
    let mut ves = get_vault_entries_with_constraint(final_constraint, txn, stats)?;
    let mut reversed = HashSet::new();
    let mut applied = vec![];
    while let Some(ve) = ves.pop() {
        if let Some(vid) = ve.reverses {
            reversed.insert(vid);
        } else if !reversed.contains(&ve.vault_id) {
            applied.push(ve);
        }
    }
    Ok(applied)
}

/*
 * Returns unreversed vault entries belonging to the user that modified this table
 */
pub fn get_user_entries_of_table_in_vault(
    uid: u64,
    guise_table: &str,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let equal_uid_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("userId")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
    };
    let g_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("guiseName")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::String(guise_table.to_string()))),
    };
    let final_constraint = Expr::BinaryOp {
        left: Box::new(equal_uid_constraint),
        op: BinaryOperator::And,
        right: Box::new(g_constraint),
    };
    let mut ves = get_vault_entries_with_constraint(final_constraint, txn, stats)?;
    let mut reversed = HashSet::new();
    let mut applied = vec![];
    while let Some(ve) = ves.pop() {
        if let Some(vid) = ve.reverses {
            reversed.insert(vid);
        } else if !reversed.contains(&ve.vault_id) {
            applied.push(ve);
        }
    }
    Ok(applied)
}

/*pub fn reapply_vault_decor_referencer_entries(
    ves: &Vec<VaultEntry>,
    table_name: &str,
    fkcol: &str,
    fktable: &str,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    /*
     * Redo modifications to objects of this table
     */
    for ve in ves {
    }

    Ok(())
}*/

pub fn reverse_vault_decor_referencer_entries(
    user_id: u64,
    table_name: &str,
    fkcol: &str,
    fktable: &str,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    /*
     * Undo modifications to objects of this table
     * TODO undo any vault modifications that were dependent on this one, namely "filters" that
     * join with this "filter" (any updates that happened after this?)
     */
    let mut vault_entries = get_user_entries_of_table_in_vault(user_id, table_name, txn, stats)?;
    warn!(
        "ReverseDecor: User {} entries of table {} in vault: {:?}",
        user_id, table_name, vault_entries
    );

    // we need some way to be able to identify these objects...
    // assume that there is exactly one object for any user?
    for ve in &vault_entries {

        if ve.update_type == DELETE_GUISE {
            continue;
        }

        //error!("ve is {:?} with fkcol {}", ve, fkcol);

        let new_id = get_value_of_col(&ve.new_value, fkcol).unwrap();
        let old_id = get_value_of_col(&ve.old_value, fkcol).unwrap();
        assert!(old_id == user_id.to_string());

        // this vault entry logged a modification to the FK. Restore the original value
        // TODO assuming that all FKs point to users
        if ve.modified_cols.contains(&fkcol.to_string()) {
            get_query_rows_txn(
                &Statement::Update(UpdateStatement {
                    table_name: string_to_objname(table_name),
                    assignments: vec![Assignment {
                        id: Ident::new(fkcol.to_string()),
                        value: Expr::Value(Value::Number(user_id.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(fkcol.to_string())])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(new_id))),
                    }),
                }),
                txn,
                stats,
            )?;
            insert_reversed_vault_entry(&ve, txn, stats)?;
        }
    }

    /*
     * Delete created guises if objects in this table had been decorrelated
     */
    let mut guise_ves = get_user_entries_with_referencer_in_vault(user_id, table_name, txn, stats)?;
    warn!(
        "User {} entries with referencer {} in vault: {:?}",
        user_id, table_name, vault_entries
    );
    for ve in &guise_ves {
        // delete guise
        get_query_rows_txn(
            &Statement::Delete(DeleteStatement {
                table_name: string_to_objname(fktable),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(fkcol.to_string())])),
                    op: BinaryOperator::Eq,
                    // XXX assuming guise is a user... only has one id
                    right: Box::new(Expr::Value(Value::Number(ve.guise_ids[0].clone()))),
                }),
            }),
            txn,
            stats,
        )?;
        // mark vault entries as reversed
        insert_reversed_vault_entry(&ve, txn, stats)?;
    }
    vault_entries.append(&mut guise_ves);
    Ok(vault_entries)
}

pub fn insert_vault_entries(
    entries: &Vec<VaultEntry>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let vault_vals: Vec<Vec<Expr>> = entries
        .iter()
        .map(|ve| {
            let mut evals = vec![];
            evals.push(Expr::Value(Value::Number(ve.disguise_id.to_string())));
            evals.push(Expr::Value(Value::Number(ve.user_id.to_string())));
            evals.push(Expr::Value(Value::String(ve.guise_name.clone())));
            evals.push(vec_to_expr(&ve.guise_id_cols));
            evals.push(vec_to_expr(&ve.guise_ids));
            evals.push(Expr::Value(Value::String(ve.referencer_name.clone())));
            evals.push(Expr::Value(Value::Number(ve.update_type.to_string())));
            evals.push(vec_to_expr(&ve.modified_cols));
            evals.push(vec_to_expr(&ve.old_value));
            evals.push(vec_to_expr(&ve.new_value));
            match ve.reverses {
                None => evals.push(Expr::Value(Value::Null)),
                Some(v) => evals.push(Expr::Value(Value::Number(v.to_string()))),
            }
            warn!(
                "InsertVEs: User {} inserting into table {} in vault: {:?}",
                ve.user_id, ve.guise_name, ve
            );
            evals
        })
        .collect();
    if !vault_vals.is_empty() {
        get_query_rows_txn(
            &Statement::Insert(InsertStatement {
                table_name: string_to_objname(VAULT_TABLE),
                columns: get_insert_vault_colnames(),
                source: InsertSource::Query(Box::new(values_query(vault_vals))),
            }),
            txn,
            stats,
        )?;
    }
    Ok(())
}

pub fn print_as_filters(db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let mut file = File::create("vault_filters.out".to_string())?;
    let mut stats: QueryStat = QueryStat::new();
    let mut txn = db.start_transaction(TxOpts::default()).unwrap();
    let ves =
        get_vault_entries_with_constraint(Expr::Value(Value::Boolean(true)), &mut txn, &mut stats)?;
    txn.commit()?;

    for ve in ves {
        let filter = match ve.reverses {
            Some(vid) => {
                file.write(format!("Rev D{}:{}\t", ve.disguise_id, vid).as_bytes())?;
                match ve.update_type {
                    INSERT_GUISE => format!(
                        "DELETE FROM {} WHERE {:?} = {:?}\n",
                        ve.guise_name, ve.guise_id_cols, ve.guise_ids
                    ),
                    DELETE_GUISE => format!(
                        "INSERT INTO {} VALUES ({:?})\n",
                        ve.guise_name, ve.new_value
                    ),
                    UPDATE_GUISE => format!(
                        "UPDATE {} SET {:?} = {:?}\n",
                        ve.guise_name, ve.modified_cols, ve.old_value
                    ),
                    _ => unimplemented!("Bad vault update type! {}\n", ve.update_type),
                }
            }
            None => {
                file.write(format!("D{}:{}\t", ve.disguise_id, ve.vault_id).as_bytes())?;
                match ve.update_type {
                    INSERT_GUISE => {
                        format!("INSERT INTO {} VALUES({:?})\n", ve.guise_name, ve.new_value)
                    }
                    DELETE_GUISE => format!(
                        "DELETE FROM {} WHERE {:?} = {:?}\n",
                        ve.guise_name, ve.guise_id_cols, ve.guise_ids
                    ),
                    UPDATE_GUISE => format!(
                        "UPDATE {} SET {:?} = {:?}\n",
                        ve.guise_name, ve.modified_cols, ve.new_value
                    ),
                    _ => unimplemented!("Bad vault update type! {}\n", ve.update_type),
                }
            }
        };
        file.write(filter.as_bytes())?;
    }
    file.flush()?;
    Ok(())
}

pub fn create_vault(in_memory: bool, txn: &mut mysql::Transaction) -> Result<(), mysql::Error> {
    let engine = Some(if in_memory {
        Engine::Memory
    } else {
        Engine::InnoDB
    });
    let indexes = vec![
        IndexDef {
            name: Ident::new("uidIndex"),
            index_type: None,
            key_parts: vec![Ident::new("userId")],
        },
        IndexDef {
            name: Ident::new("disguiseIndex"),
            index_type: None,
            key_parts: vec![Ident::new("disguiseId")],
        },
    ];

    txn.query_drop(
        &Statement::CreateTable(CreateTableStatement {
            name: string_to_objname(VAULT_TABLE),
            columns: get_vault_cols(),
            constraints: vec![],
            indexes: indexes.clone(),
            with_options: vec![],
            if_not_exists: true,
            engine: engine.clone(),
        })
        .to_string(),
    )
}

/***********************************************
 ********* VAULT DEFINITIONS *******************
***********************************************/

fn get_insert_vault_colnames() -> Vec<Ident> {
    vec![
        Ident::new("disguiseId"),
        Ident::new("userId"),
        Ident::new("guiseName"),
        Ident::new("guiseIdCols"),
        Ident::new("guiseIds"),
        Ident::new("referencerName"),
        Ident::new("updateType"),   // remove, add, or modify
        Ident::new("modifiedCols"), // null if all modified
        Ident::new("oldValue"),
        Ident::new("newValue"),
        Ident::new("reverses"),
    ]
}

fn get_vault_cols() -> Vec<ColumnDef> {
    vec![
        // for ordering
        ColumnDef {
            name: Ident::new("vaultId"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![
                ColumnOptionDef {
                    name: None,
                    option: ColumnOption::NotNull,
                },
                ColumnOptionDef {
                    name: None,
                    option: ColumnOption::AutoIncrement,
                },
                ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Unique { is_primary: true },
                },
            ],
        },
        // FK to disguise history
        ColumnDef {
            name: Ident::new("disguiseId"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        },
        // user Id
        ColumnDef {
            name: Ident::new("userId"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![],
        },
        // table and column name
        ColumnDef {
            name: Ident::new("guiseName"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // guise Id colss
        ColumnDef {
            name: Ident::new("guiseIdCols"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // guise Ids
        ColumnDef {
            name: Ident::new("guiseIds"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // optional name of referencer
        ColumnDef {
            name: Ident::new("referencerName"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // type of vault entry
        ColumnDef {
            name: Ident::new("updateType"),
            data_type: DataType::Int,
            collation: None,
            options: vec![],
        },
        // modified columns, null if all modified
        ColumnDef {
            name: Ident::new("modifiedCols"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // value that object was changed from
        ColumnDef {
            name: Ident::new("oldValue"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // value that object was changed from
        ColumnDef {
            name: Ident::new("newValue"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // whether this update reverses a prior vault entry
        ColumnDef {
            name: Ident::new("reverses"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![],
        },
    ]
}
