use crate::helpers::stats::QueryStat;
use crate::helpers::*;
use mysql::prelude::*;
use sql_parser::ast::*;

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
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<bool, mysql::Error> {
    let equal_uid_constraint = Expr::BinaryOp {
         left: Box::new(Expr::Identifier(vec![Ident::new("userID")])),
         op: BinaryOperator::Eq,
         right: Box::new(Expr::Value(Value::Number(de.user_id.to_string()))),
     };
     let disguise_constraint = Expr::BinaryOp {
         left: Box::new(Expr::Identifier(vec![Ident::new("disguiseID")])),
         op: BinaryOperator::Eq,
         right: Box::new(Expr::Value(Value::Number(de.disguise_id.to_string()))),
     };
     let constraint = Expr::BinaryOp {
         left: Box::new(equal_uid_constraint),
         op: BinaryOperator::And,
         right: Box::new(disguise_constraint),
     };

    let rows = get_query_rows_txn(&select_ordered_statement(HISTORY_TABLE, Some(constraint), "historyID"), txn, stats)?;
    let mut is_reversed = false;
    for r in rows {
        if get_value_of_col(&r, "reverse").is_some() {
            is_reversed = true;
        } else {
            is_reversed = false;
        }
    }
    Ok(is_reversed)
}

pub fn insert_disguise_history_entry(
    de: &DisguiseEntry,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let mut evals = vec![];
    evals.push(Expr::Value(Value::Number(de.disguise_id.to_string())));
    evals.push(Expr::Value(Value::Number(de.user_id.to_string())));
    evals.push(Expr::Value(Value::Boolean(de.reverse)));
    get_query_rows_txn(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname(HISTORY_TABLE),
            columns: get_insert_disguise_colnames(),
            source: InsertSource::Query(Box::new(values_query(vec![evals]))),
        }),
        txn,
        stats,
    )?;
    Ok(())
}

fn get_insert_disguise_colnames() -> Vec<Ident> {
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
