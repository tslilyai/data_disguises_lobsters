use decor::disguise::*;
use sql_parser::ast::{DataType, Expr, Ident, ObjectName, Statement, UnaryOperator, Value};

pub struct fk {
    pub referencer_col: String,
    pub fk_name: String,
    pub fk_col: String,
}

pub struct table_fks {
    referencer_name: String,
    fks: Vec<fk>,
}

const VAULT_FMT_STR: &'static str = "{}Vault";
const VAULT_UID: &'static str = "uid";
const SCHEMA_UID_COL: &'static str = "contactID";
const SCHEMA_UID_TABLE: &'static str = "ContactInfo";
const VAULT_COL_NAMES :Vec<Ident> = vec![
    Ident::new(VAULT_UID), 
    Ident::new("timestamp"), 
    Ident::new("modifiedObjectName"), 
    Ident::new("oldValue"), 
    Ident::new("newValue"), 
];
const VAULT_COLS: Vec<ColumnDef> = vec![
    // user ID
    ColumnDef {
        name: Ident::new(VAULT_UID),
        data_type: Datatype::BigInt,
        collation: None,
        options: vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::NotNull,
        }],
    },
    // for ordering
    ColumnDef {
        name: Ident::new("timestamp"),
        data_type: Datatype::Timestamp,
        collation: None,
        options: vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::NotNull,
        }],
    },
    // table and column name
    ColumnDef {
        name: Ident::new("modifiedObjectName"),
        data_type: Datatype::Varbinary(4096),
        collation: None,
        options: vec![],
    },
    // value that object col was changed from
    ColumnDef {
        name: Ident::new("oldValue"),
        data_type: Datatype::Varbinary(4096),
        collation: None,
        options: vec![],
    },
    // value that object col was changed to (NULL if deleted)
    ColumnDef {
        name: Ident::new("newValue"),
        data_type: Datatype::Varbinary(4096),
        collation: None,
        options: vec![],
    },
];
const TABLE_NAMES: Vec<&'static str> = vec![
    // modified
    "PaperReviewRefused",
    "ActionLog",
    "ReviewRating",
    "PaperReview",
    "PaperComment",
    // deleted
    "PaperWatch",
    "PaperReviewPreference",
    "Capability",
    "PaperConflict",
    "TopicInterest",
    "PaperTag",
    "PaperTagAnno",
    "ContactInfo",
];

fn str_to_tablewithjoins(name: &str) -> Vec<TableWithJoins> {
    vec![TableWithJoins {
        relation: TableFactor::Table {
            name: helpers::str_to_objname(name),
            alias: None,
        },
        joins: vec![],
    }]
}

fn get_schema_statements(schema: &str, in_memory: bool) -> Vec<Statement> {
    /* issue schema statements but only if we're not priming and not decor */
    let mut stmts = vec![];
    let mut stmt = String::new();
    for line in SCHEMA.lines() {
        if line.starts_with("--") || line.is_empty() {
            continue;
        }
        if !stmt.is_empty() {
            stmt.push_str(" ");
        }
        stmt.push_str(line);
        if stmt.ends_with(';') {
            // only save create table statements for now
            if stmt.contains("CREATE") {
                stmt = helpers::process_schema_stmt(&stmt, in_memory);
                stmts.push(helpers::get_single_parsed_stmt(&stmt).unwrap());
            }
            stmt = String::new();
        }
    }
}

fn get_vault_statements(in_memory: bool) -> Vec<Statement> {
    let engine = Some(if in_memory { Engine::Memory } else { InnoDB });
    let indexes = vec![IndexDef {
        name: Ident::new("uid_index"),
        index_type: None,
        key_parts: Ident::new(VAULT_UID),
    }];

    let mut stmts = vec![];

    for name in TABLE_NAMES {
        stmts.push(Statement::CreateTableStatement {
            name: helpers::str_to_objname(&format!(VAULT_FMT_STR, name)),
            columns: VAULT_COLS,
            constraints: vec![],
            indexes: indexes,
            with_options: None,
            if_not_exists: true,
            engine: engine,
        });
    }
    stmts
}

pub fn get_hotcrp_application(schema: &str, in_memory: bool) -> Application {
    let mut disguises = vec![get_conference_anon_disguise(), get_gdpr_removal_disguise()];

    Application {
        disguises: disguises,
        schema: get_schema_statements(schema, in_memory),
        vaults: get_vault_statements(in_memory),
    }
}
