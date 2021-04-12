use crate::helpers;
use sql_parser::ast::*;

pub const VAULT_UID: &'static str = "uid";

pub fn table_to_vault(table: &str) -> String {
    format!("{}Vault", table)
}

pub fn get_insert_vault_colnames() -> Vec<Ident> {
    vec![
        Ident::new(VAULT_UID),
        // leaving out timestamp to be automatically generated
        //Ident::new("timestamp"),
        Ident::new("name"),
        Ident::new("modifiedCols"), // default NULL implies all
        Ident::new("oldValue"),
        Ident::new("newValue"),
    ]
}
pub fn get_vault_cols() -> Vec<ColumnDef> {
    vec![
        // user ID
        ColumnDef {
            name: Ident::new(VAULT_UID),
            data_type: DataType::BigInt,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        },
        // for ordering
        ColumnDef {
            name: Ident::new("timestamp"),
            data_type: DataType::Timestamp,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        },
        // table and column name
        ColumnDef {
            name: Ident::new("name"),
            data_type: DataType::Varbinary(4096),
            collation: None,
            options: vec![],
        },
        // table and column name
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
    ]
}

pub fn get_create_vault_statements(table_names: Vec<&str>, in_memory: bool) -> Vec<Statement> {
    let engine = Some(if in_memory {
        Engine::Memory
    } else {
        Engine::InnoDB
    });
    let indexes = vec![IndexDef {
        name: Ident::new("uid_index"),
        index_type: None,
        key_parts: vec![Ident::new(VAULT_UID)],
    }];

    let mut stmts = vec![];

    for name in table_names {
        stmts.push(Statement::CreateTable(CreateTableStatement {
            name: helpers::string_to_objname(&table_to_vault(&name)),
            columns: get_vault_cols(),
            constraints: vec![],
            indexes: indexes.clone(),
            with_options: vec![],
            if_not_exists: true,
            engine: engine.clone(),
        }));
    }
    stmts
}
