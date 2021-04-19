use crate::disguises::*;
use crate::helpers::stats::QueryStat;
use crate::helpers::*;
use mysql::prelude::*;
use serde::Serialize;
use sql_parser::ast::*;
use std::str::FromStr;

pub const HISTORY_TABLE: &'static str = "DisguiseHistory";

#[derive(Default)]
pub struct DisguiseEntry {
    pub disguise_id: u64,
    pub user_id: u64,
    pub reverse: bool,
}

pub fn insert_disguise_entry(
    de: &DisguiseEntry,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let mut evals = vec![];
    evals.push(Expr::Value(Value::Number(de.disguise_id.to_string())));
    evals.push(Expr::Value(Value::Number(de.user_id.to_string())));
    evals.push(Expr::Value(Value::Boolean(de.reverse)));
    let vals: Vec<Vec<Expr>> = vec![evals];
    if !vals.is_empty() {
        get_query_rows_txn(
            &Statement::Insert(InsertStatement {
                table_name: string_to_objname(HISTORY_TABLE),
                columns: get_insert_disguise_colnames(),
                source: InsertSource::Query(Box::new(values_query(vals))),
            }),
            txn,
            stats,
        )?;
    }
    Ok(())
}

pub fn get_insert_disguise_colnames() -> Vec<Ident> {
    vec![
        Ident::new("disguiseID"),
        Ident::new("userID"),
        Ident::new("reverse"),
    ]
}

pub fn get_history_cols() -> Vec<ColumnDef> {
    vec![
        // for ordering
        ColumnDef {
            name: Ident::new("historyID"),
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
        // for ordering
        ColumnDef {
            name: Ident::new("disguiseID"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        },
        // user ID
        ColumnDef {
            name: Ident::new("userID"),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![],
        },
        // whether this disguise entry is a reversal
        ColumnDef {
            name: Ident::new("reverse"),
            data_type: DataType::Boolean,
            collation: None,
            options: vec![],
        },
    ]
}

pub fn create_history(in_memory: bool, txn: &mut mysql::Transaction) -> Result<(), mysql::Error> {
    let engine = Some(if in_memory {
        Engine::Memory
    } else {
        Engine::InnoDB
    });
    let indexes = vec![IndexDef {
        name: Ident::new("userDisguiseIndex"),
        index_type: None,
        key_parts: vec![Ident::new("disguiseID"), Ident::new("userID")],
    }];

    txn.query_drop(
        &Statement::CreateTable(CreateTableStatement {
            name: string_to_objname(HISTORY_TABLE),
            columns: get_history_cols(),
            constraints: vec![],
            indexes: indexes.clone(),
            with_options: vec![],
            if_not_exists: true,
            engine: engine.clone(),
        })
        .to_string(),
    )
}
