use crate::stats::QueryStat;
use crate::helpers::*;
use mysql::prelude::*;
use sql_parser::ast::*;
use std::sync::{Arc, Mutex};

pub const HISTORY_TABLE: &'static str = "DisguiseHistory";

#[derive(Default)]
pub struct DisguiseEntry {
    pub disguise_id: u64,
    pub user_id: u64,
    pub reverse: bool,
}

/* 
 * Assumes that a disguise cannot be reversed or applied twice in sequence
 */
pub fn is_disguise_reversed (
    de: &DisguiseEntry,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<bool, mysql::Error> {
    let equal_uid_constraint = Expr::BinaryOp {
         left: Box::new(Expr::Identifier(vec![Ident::new("userId")])),
         op: BinaryOperator::Eq,
         right: Box::new(Expr::Value(Value::Number(de.user_id.to_string()))),
     };
     let disguise_constraint = Expr::BinaryOp {
         left: Box::new(Expr::Identifier(vec![Ident::new("disguiseId")])),
         op: BinaryOperator::Eq,
         right: Box::new(Expr::Value(Value::Number(de.disguise_id.to_string()))),
     };
     let constraint = Expr::BinaryOp {
         left: Box::new(equal_uid_constraint),
         op: BinaryOperator::And,
         right: Box::new(disguise_constraint),
     };

    let rows = get_query_rows(&select_ordered_statement(HISTORY_TABLE, Some(constraint), "historyId"), conn, stats)?;
    let mut is_reversed = true;
    for r in rows {
        if &get_value_of_col(&r, "reverse").unwrap() == "0" {
            is_reversed = false;
        } else {
            is_reversed = true;
        }
    }
    Ok(is_reversed)
}

pub fn insert_disguise_history_entry(
    de: &DisguiseEntry,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) {
    let mut evals = vec![];
    evals.push(Expr::Value(Value::Number(de.disguise_id.to_string())));
    evals.push(Expr::Value(Value::Number(de.user_id.to_string())));
    evals.push(Expr::Value(Value::Boolean(de.reverse)));
    query_drop(
        Statement::Insert(InsertStatement {
            table_name: string_to_objname(HISTORY_TABLE),
            columns: get_insert_disguise_colnames(),
            source: InsertSource::Query(Box::new(values_query(vec![evals]))),
        }).to_string(),
        conn,
        stats,
    ).unwrap();
}

fn get_insert_disguise_colnames() -> Vec<Ident> {
    vec![
        Ident::new("disguiseId"),
        Ident::new("userId"),
        Ident::new("reverse"),
    ]
}

pub fn get_history_cols() -> Vec<ColumnDef> {
    vec![
        // for ordering
        ColumnDef {
            name: Ident::new("historyId"),
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
        // whether this disguise entry is a reversal
        ColumnDef {
            name: Ident::new("reverse"),
            data_type: DataType::Boolean,
            collation: None,
            options: vec![],
        },
    ]
}

pub fn create_history(in_memory: bool, conn: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
    let engine = Some(if in_memory {
        Engine::Memory
    } else {
        Engine::InnoDB
    });
    let indexes = vec![IndexDef {
        name: Ident::new("userDisguiseIndex"),
        index_type: None,
        key_parts: vec![Ident::new("disguiseId"), Ident::new("userId")],
    }];

    conn.query_drop(
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
