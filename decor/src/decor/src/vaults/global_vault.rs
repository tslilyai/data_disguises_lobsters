use crate::helpers::*;
use crate::stats::QueryStat;
use crate::types::*;
use log::{debug, warn};
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

pub const VAULT_TABLE: &'static str = "VaultTable";
pub const INSERT_GUISE: u64 = 0;
pub const DELETE_GUISE: u64 = 1;
pub const UPDATE_GUISE: u64 = 2;

static VAULT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
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
pub fn ve_to_bytes(ve: &VaultEntry) -> Vec<u8> {
    let s = serde_json::to_string(ve).unwrap();
    s.as_bytes().to_vec()
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
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let rows = get_query_rows(
        &select_ordered_statement(VAULT_TABLE, Some(constraint), "vaultId"),
        conn,
        stats.clone(),
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
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) {
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
    evals.push(vec_to_expr(&ve.modified_cols));
    evals.push(vec_to_expr(&ve.old_value));
    evals.push(vec_to_expr(&ve.new_value));
    evals.push(Expr::Value(Value::Number(ve.vault_id.to_string())));
    let vault_vals: Vec<Vec<Expr>> = vec![evals];
    query_drop(
        Statement::Insert(InsertStatement {
            table_name: string_to_objname(VAULT_TABLE),
            columns: get_insert_vault_colnames(),
            source: InsertSource::Query(Box::new(values_query(vault_vals))),
        })
        .to_string(),
        conn,
        stats.clone(),
    ).unwrap();
}

/*
 * Returns unreversed vault entries belonging to the user that modified this table and had the specified referencer
 */
fn get_user_entries_with_referencer_in_vault(
    uid: u64,
    referencer_table: &str,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
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
    let mut ves = get_vault_entries_with_constraint(final_constraint, conn, stats)?;
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
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
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
    let mut ves = get_vault_entries_with_constraint(final_constraint, conn, stats)?;
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
    referencer_table: &str,
    referencer_col: &str,
    fktable: &str,
    fkcol: &str,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    // TODO assuming that all FKs point to users

    /*
     * Undo modifications to objects of this table
     * TODO undo any vault modifications that were dependent on this one, namely "filters" that
     * join with this "filter" (any updates that happened after this?)
     */
    let mut vault_entries =
        get_user_entries_of_table_in_vault(user_id, referencer_table, conn, stats.clone())?;
    warn!(
        "ReverseDecor: User {} reversing {} entries of table {} in vault",
        user_id,
        vault_entries.len(),
        referencer_table
    );

    // we need some way to be able to identify these objects...
    // assume that there is exactly one object for any user?
    for ve in &vault_entries {
        if ve.update_type == DELETE_GUISE {
            continue;
        }

        // this may be none if this vault entry is an insert, and not a modification
        let new_id: String;
        let old_id: String;
        match get_value_of_col(&ve.new_value, referencer_col) {
            Some(n) => new_id = n,
            None => continue,
        }
        match get_value_of_col(&ve.old_value, referencer_col) {
            Some(n) => old_id = n,
            None => continue,
        }

        // XXX just to run tests for now
        if old_id != user_id.to_string() {
            warn!("old id {} != user id {}", old_id, user_id);
            continue;
        }
        //assert!(old_id == user_id.to_string());

        // this vault entry logged a modification to the FK. Restore the original value
        if ve.modified_cols.contains(&referencer_col.to_string()) {
            query_drop(
                Statement::Update(UpdateStatement {
                    table_name: string_to_objname(referencer_table),
                    assignments: vec![Assignment {
                        id: Ident::new(referencer_col.to_string()),
                        value: Expr::Value(Value::Number(user_id.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(
                            referencer_col.to_string(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(new_id))),
                    }),
                })
                .to_string(),
                conn,
                stats.clone(),
            )?;
            insert_reversed_vault_entry(&ve, conn, stats.clone());
        }
    }

    /*
     * Delete created guises if objects in this table had been decorrelated
     * TODO can make per-guise-table, rather than assume that only users are guises
     */
    let mut guise_ves =
        get_user_entries_with_referencer_in_vault(user_id, referencer_table, conn, stats.clone())?;
    warn!(
        "ReverseDecor: User {} reversing {} entries with referencer {} in vault",
        user_id,
        vault_entries.len(),
        referencer_table
    );
    for ve in &guise_ves {
        // delete guise
        query_drop(
            Statement::Delete(DeleteStatement {
                table_name: string_to_objname(fktable),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(fkcol.to_string())])),
                    op: BinaryOperator::Eq,
                    // XXX assuming guise is a user... only has one id
                    right: Box::new(Expr::Value(Value::Number(ve.guise_ids[0].clone()))),
                }),
            })
            .to_string(),
            conn,
            stats.clone(),
        )?;
        // mark vault entries as reversed
        insert_reversed_vault_entry(&ve, conn, stats.clone());
    }
    vault_entries.append(&mut guise_ves);
    Ok(vault_entries)
}

pub fn insert_vault_entries(
    entries: &Vec<VaultEntry>,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) {
    let vault_vals: Vec<Vec<Expr>> = entries
        .iter()
        .map(|ve| {
            let mut evals = vec![];
            evals.push(Expr::Value(Value::Number(VAULT_ID.fetch_add(1, Ordering::SeqCst).to_string())));
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
                "InsertVEs: User {} inserting ve for table {}",
                ve.user_id, ve.guise_name
            );
            evals
        })
        .collect();
    if !vault_vals.is_empty() {
        query_drop(
            Statement::Insert(InsertStatement {
                table_name: string_to_objname(VAULT_TABLE),
                columns: get_insert_vault_colnames(),
                source: InsertSource::Query(Box::new(values_query(vault_vals))),
            })
            .to_string(),
            conn,
            stats.clone(),
        ).unwrap();
    }
}

pub fn print_as_filters(conn: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    let mut file = File::create("vault_filters.out".to_string())?;
    let stats = Arc::new(Mutex::new(QueryStat::new()));
    let ves = get_vault_entries_with_constraint(Expr::Value(Value::Boolean(true)), conn, stats)?;

    for ve in ves {
        let filter = match ve.reverses {
            Some(_) => {
                file.write(format!("\t-D{}: ", ve.disguise_id).as_bytes())?;
                match ve.update_type {
                    INSERT_GUISE => format!(
                        "DELETE FROM {} WHERE {:?} = {:?}\n",
                        ve.guise_name, ve.guise_id_cols, ve.guise_ids
                    ),
                    DELETE_GUISE => {
                        let mut cols = vec![];
                        let mut vals = vec![];
                        for rc in ve.old_value {
                            cols.push(rc.column);
                            vals.push(rc.value);
                        }
                        format!(
                            "INSERT INTO {} ({:?}) VALUES({:?})\n",
                            ve.guise_name, cols, vals
                        )
                    }
                    UPDATE_GUISE => {
                        let mut setstr = String::new();
                        for rc in ve.old_value {
                            if ve.modified_cols.contains(&rc.column) {
                                setstr.push_str(&format!("{} = {}, ", rc.column, rc.value));
                            }
                        }
                        format!(
                            "UPDATE {} SET {} WHERE {:?} = {:?}\n",
                            ve.guise_name, setstr, ve.guise_id_cols, ve.guise_ids
                        )
                    }
                    _ => unimplemented!("Bad vault update type! {}\n", ve.update_type),
                }
            }
            None => {
                file.write(format!("+D{}: ", ve.disguise_id).as_bytes())?;
                match ve.update_type {
                    INSERT_GUISE => {
                        let mut cols = vec![];
                        let mut vals = vec![];
                        for rc in ve.new_value {
                            cols.push(rc.column);
                            vals.push(rc.value);
                        }
                        format!(
                            "INSERT INTO {} ({:?}) VALUES({:?})\n",
                            ve.guise_name, cols, vals
                        )
                    }
                    DELETE_GUISE => format!(
                        "DELETE FROM {} WHERE {:?} = {:?}\n",
                        ve.guise_name, ve.guise_id_cols, ve.guise_ids
                    ),
                    UPDATE_GUISE => {
                        let mut setstr = String::new();
                        for rc in ve.new_value {
                            if ve.modified_cols.contains(&rc.column) {
                                setstr.push_str(&format!("{} = {}, ", rc.column, rc.value));
                            }
                        }
                        format!(
                            "UPDATE {} SET {} WHERE {:?} = {:?}\n",
                            ve.guise_name, setstr, ve.guise_id_cols, ve.guise_ids
                        )
                    }
                    _ => unimplemented!("Bad vault update type! {}\n", ve.update_type),
                }
            }
        };
        file.write(filter.as_bytes())?;
    }
    file.flush()?;
    Ok(())
}

pub fn create_vault(in_memory: bool, conn: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
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

    conn.query_drop(
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
        Ident::new("vaultId"),
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
