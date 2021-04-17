use crate::disguises::*;
use crate::helpers::*;
use mysql::prelude::*;
use serde::Serialize;
use sql_parser::ast::*;
use std::str::FromStr;

pub const VAULT_TABLE: &'static str = "VaultTable";
pub const INSERT_GUISE: u64 = 0;
pub const DELETE_GUISE: u64 = 1;
pub const UPDATE_GUISE: u64 = 2;

#[derive(Default)]
pub struct VaultEntry {
    pub vault_id: u64,
    pub user_id: u64,
    pub guise_name: String,
    pub guise_id: u64,
    pub referencer_name: String,
    pub update_type: u64,
    pub modified_cols: Vec<String>,
    pub old_value: Vec<RowVal>,
    pub new_value: Vec<RowVal>,
    pub reversed: bool,
}

fn get_vault_entries_with_constraint(
    constraint: Expr,
    txn: &mut mysql::Transaction,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let rows = get_query_rows_txn(&select_statement(VAULT_TABLE, Some(constraint)), txn)?;
    let mut ves = vec![];
    for row in rows {
        let mut ve: VaultEntry = Default::default();
        for rv in row {
            match rv.column.as_str() {
                "vaultID" => ve.vault_id = u64::from_str(&rv.value).unwrap(),
                "userID" => ve.user_id = u64::from_str(&rv.value).unwrap(),
                "guiseName" => ve.guise_name = rv.value.clone(),
                "guiseID" => ve.guise_id = u64::from_str(&rv.value).unwrap(),
                "referencerName" => ve.referencer_name = rv.value.clone(),
                "updateType" => ve.update_type = u64::from_str(&rv.value).unwrap(),
                "modifiedCols" => ve.modified_cols = serde_json::from_str(&rv.value).unwrap(),
                "oldValue" => ve.old_value = serde_json::from_str(&rv.value).unwrap(),
                "newValue" => ve.new_value = serde_json::from_str(&rv.value).unwrap(),
                _ => unimplemented!("Incorrect column name! {:?}", rv),
            };
        }
        ves.push(ve);
    }
    Ok(ves)
}

fn vec_to_expr<T: Serialize>(vs: &Vec<T>) -> Expr {
    if vs.is_empty() {
        Expr::Value(Value::Null)
    } else {
        let serialized = serde_json::to_string(&vs).unwrap();
        Expr::Value(Value::String(serialized))
    }
}

pub fn mark_vault_entry_reversed(
    ve: &VaultEntry,
    txn: &mut mysql::Transaction,
) -> Result<(), mysql::Error> {
    get_query_rows_txn(
        &Statement::Update(UpdateStatement {
            table_name: string_to_objname(&VAULT_TABLE),
            assignments: vec![Assignment {
                id: Ident::new("reversed"),
                value: Expr::Value(Value::Boolean(true)),
            }],
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new("vaultID")])),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Number(ve.vault_id.to_string()))),
            }),
        }),
        txn,
    )?;
    Ok(())
}

pub fn get_user_entries_with_referencer_in_vault(
    uid: u64,
    referencer_table: &str,
    is_reversed: bool,
    txn: &mut mysql::Transaction,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let equal_uid_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("userID")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
    };
    let reversed_constraint = match is_reversed {
        true => Expr::Identifier(vec![Ident::new("reversed")]),
        false => Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Identifier(vec![Ident::new("reversed")])),
        },
    };
    let ref_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("referencerName")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::String(referencer_table.to_string()))),
    };
    let intermed_constraint = Expr::BinaryOp {
        left: Box::new(equal_uid_constraint),
        op: BinaryOperator::And,
        right: Box::new(reversed_constraint),
    };
    let final_constraint = Expr::BinaryOp {
        left: Box::new(intermed_constraint),
        op: BinaryOperator::And,
        right: Box::new(ref_constraint),
    };
    get_vault_entries_with_constraint(final_constraint, txn)
}

pub fn get_user_entries_of_table_in_vault(
    uid: u64,
    guise_table: &str,
    is_reversed: bool,
    txn: &mut mysql::Transaction,
) -> Result<Vec<VaultEntry>, mysql::Error> {
    let equal_uid_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("userID")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
    };
    let reversed_constraint = match is_reversed {
        true => Expr::Identifier(vec![Ident::new("reversed")]),
        false => Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Identifier(vec![Ident::new("reversed")])),
        },
    };
    let g_constraint = Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new("guiseName")])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::String(guise_table.to_string()))),
    };
    let intermed_constraint = Expr::BinaryOp {
        left: Box::new(equal_uid_constraint),
        op: BinaryOperator::And,
        right: Box::new(reversed_constraint),
    };
    let final_constraint = Expr::BinaryOp {
        left: Box::new(intermed_constraint),
        op: BinaryOperator::And,
        right: Box::new(g_constraint),
    };
    get_vault_entries_with_constraint(final_constraint, txn)
}

pub fn insert_vault_entries(
    entries: &Vec<VaultEntry>,
    txn: &mut mysql::Transaction,
) -> Result<(), mysql::Error> {
    let vault_vals: Vec<Vec<Expr>> = entries
        .iter()
        .map(|ve| {
            let mut evals = vec![];
            evals.push(Expr::Value(Value::Number(ve.user_id.to_string())));
            evals.push(Expr::Value(Value::String(ve.guise_name.clone())));
            evals.push(Expr::Value(Value::Number(ve.guise_id.to_string())));
            evals.push(Expr::Value(Value::String(ve.referencer_name.clone())));
            evals.push(Expr::Value(Value::Number(ve.update_type.to_string())));
            evals.push(vec_to_expr(&ve.modified_cols));
            evals.push(vec_to_expr(&ve.old_value));
            evals.push(vec_to_expr(&ve.new_value));
            evals.push(Expr::Value(Value::Boolean(ve.reversed)));
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
        )?;
    }
    Ok(())
}

pub fn get_insert_vault_colnames() -> Vec<Ident> {
    vec![
        Ident::new("userID"),
        Ident::new("guiseName"),
        Ident::new("guiseID"),
        Ident::new("referencerName"),
        Ident::new("updateType"),   // remove, add, or modify
        Ident::new("modifiedCols"), // null if all modified
        Ident::new("oldValue"),
        Ident::new("newValue"),
        Ident::new("reversed"),
    ]
}

pub fn get_vault_cols() -> Vec<ColumnDef> {
    vec![
        // for ordering
        ColumnDef {
            name: Ident::new("vaultID"),
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
        // user ID
        ColumnDef {
            name: Ident::new("userID"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        },
        // table and column name
        ColumnDef {
            name: Ident::new("guiseName"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // guise ID
        ColumnDef {
            name: Ident::new("guiseID"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
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
        // whether this update has been reversed
        ColumnDef {
            name: Ident::new("reversed"),
            data_type: DataType::Boolean,
            collation: None,
            options: vec![],
        },
    ]
}

pub fn create_vault(in_memory: bool, txn: &mut mysql::Transaction) -> Result<(), mysql::Error> {
    let engine = Some(if in_memory {
        Engine::Memory
    } else {
        Engine::InnoDB
    });
    let indexes = vec![IndexDef {
        name: Ident::new("uid_index"),
        index_type: None,
        key_parts: vec![Ident::new("userID")],
    }];

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
