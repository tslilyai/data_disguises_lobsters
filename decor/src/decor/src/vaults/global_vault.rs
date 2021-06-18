use crate::helpers::*;
use crate::stats::QueryStat;
use crate::vaults::*;
use log::{debug, warn};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const VAULT_TABLE: &'static str = "GlobalVault";

pub static VAULT_ID: AtomicU64 = AtomicU64::new(1);

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
    Ok(rows_to_ves(&rows))
}

fn rows_to_ves(rows: &Vec<Vec<RowVal>>) -> Vec<VaultEntry> {
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
                "reversed" => ve.reversed = &rv.value != NULLSTR,
                _ => unimplemented!("Incorrect column name! {:?}", rv),
            };
        }
        ves.push(ve);
    }
    ves
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
    )
    .unwrap();
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
    let mut applied = vec![];
    while let Some(ve) = ves.pop() {
        if !ve.reversed {
            applied.push(ve);
        }
    }
    Ok(applied)
}

/*
 * Returns unreversed vault entries belonging to the user
 */
pub fn get_global_vault_ves(
    uid: Option<u64>,
    table: Option<String>,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let final_constraint: Expr;
    match (uid, table) {
        (Some(uid), Some(tab)) => {
            let equal_uid_constraint = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new("userId")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
            };
            let g_constraint = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new("guiseName")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::String(tab.to_string()))),
            };
            final_constraint = Expr::BinaryOp {
                left: Box::new(equal_uid_constraint),
                op: BinaryOperator::And,
                right: Box::new(g_constraint),
            };
        }
        (Some(uid), None) => {
            final_constraint = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new("userId")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
            };
        }
        (None, Some(tab)) => {
            final_constraint = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new("guiseName")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(tab.to_string()))),
            };
        }
        _ => final_constraint = Expr::Value(Value::Boolean(true)),
    }
    let mut ves = get_vault_entries_with_constraint(final_constraint, conn, stats)?;
    let mut applied = vec![];
    while let Some(ve) = ves.pop() {
        if !ve.reversed {
            applied.push(ve);
        }
    }
    Ok(applied)
}

pub fn insert_global_ves(
    entries: &HashMap<u64, Vec<VaultEntry>>,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) {
    let mut vault_vals: Vec<Vec<Expr>> = vec![];
    for (_, user_ves) in entries.iter() {
        vault_vals.append(
            &mut user_ves
                .iter()
                .map(|ve| {
                    let mut evals = vec![];
                    evals.push(Expr::Value(Value::Number(
                        VAULT_ID.fetch_add(1, Ordering::SeqCst).to_string(),
                    )));
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
                    evals.push(Expr::Value(Value::Boolean(ve.reversed)));
                    warn!(
                        "InsertVEs: User {} inserting ve for table {}",
                        ve.user_id, ve.guise_name
                    );
                    evals
                })
                .collect(),
        );
    }

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
        )
        .unwrap();
    }
}

pub fn print_as_filters(conn: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    let mut file = File::create("vault_filters.out".to_string())?;
    let stats = Arc::new(Mutex::new(QueryStat::new()));
    let ves = get_vault_entries_with_constraint(Expr::Value(Value::Boolean(true)), conn, stats)?;

    for ve in ves {
        let filter = if ve.reversed {
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
        } else {
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
        };
        file.write(filter.as_bytes())?;
    }
    file.flush()?;
    Ok(())
}

pub fn create_vault(in_memory: bool, conn: &mut mysql::Conn) -> Result<(), mysql::Error> {
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
        Ident::new("reversed"),
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
        // whether this update reversed a prior vault entry
        ColumnDef {
            name: Ident::new("reversed"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![],
        },
    ]
}
