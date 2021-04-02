use decor::disguise::*;
use sql_parser::ast::{DataType, Expr, Ident, ObjectName, Statement, UnaryOperator, Value};

const VAULT_FMT_STR: &'static str = "{}Vault";
const VAULT_UID: &'static str = "uid";
const SCHEMA_UID_COL: &'static str = "contactID";
const SCHEMA_UID_TABLE: &'static str = "ContactInfo";

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
        name: Ident::new("modifiedObject"),
        data_type: Datatype::Varbinary(4096),
        collation: None,
        options: vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::NotNull,
        }],
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
            name: helpers::str_to_objname(name),
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

fn str_to_tablewithjoins(name: &str) -> Vec<TableWithJoins> {
    vec![TableWithJoins {
        relation: TableFactor::Table {
            name: helpers::str_to_objname(name),
            alias: None,
        },
        joins: vec![],
    }]
}

fn get_conference_anon_disguise() -> Disguise {
    let mut txns = vec![];

    // remove disguises
    let remove_names = vec![
        "PaperReviewPreference",
        "Capability",
        "PaperConflict",
        "TopicInterest",
        "PaperTag",
        "PaperTagAnno",
    ];
    let decor_names_and_parents = vec![
        (
            "PaperReviewRefused",
            vec![("requestedBy", "ContactInfo"), ("refusedBy", "ContactInfo")],
        ),
        (
            "ActionLog",
            vec![
                ("contactId", "ContactInfo"),
                ("destContactId", "ContactInfo"),
                ("trueContactId", "ContactInfo"),
            ],
        ),
        ("ReviewRating", vec![("contactId", "ContactInfo")]),
        (
            "PaperReview",
            vec![("contactId", "ContactInfo"), ("requestedBy", "ContactInfo")],
        ),
        ("PaperComment", vec![("contactId", "ContactInfo")]),
    ];

    // vault: insert found value
    // obj: delete found values
    for name in remove_names {
        txns.push(DisguiseTxn {
            predicate: Select {
                distinct: true,
                projection: SelectItem::Wildcard,
                from: str_to_tablewithjoins(name),
                selection: None,
                group_by: vec![],
                having: None,
            },
            vault_updates: vec![],
            obj_updates: vec![Statement::DeleteStatement {
                table_name: helpers::str_to_objname(name),
                selection: None,
            }],
        });
    }

    // vault:
    //  - save referenced object and referencer
    //  - save what anon object has been inserted into fk table
    // obj:
    //  - insert new anon row for fk table
    //  - update referencer fk column
    //  - remove referenced object in fk table
    for (name, fk_cols) in decor_names_and_parents {
        txns.push(DisguiseTxn {
            predicate: Select {
                distinct: true,
                projection: SelectItem::Wildcard,
                from: str_to_tablewithjoins(name),
                selection: None,
                group_by: vec![],
                having: None,
            },
            vault_updates: vec![],
            obj_updates: vec![Statement::DeleteStatement {
                table_name: helpers::str_to_objname(name),
                selection: None,
            }],
        });
    }
    Disguise(txns)
}

fn get_gdpr_removal_disguise() -> Disguise {
    let mut txns = vec![];
    Disguise(txns)
}

pub fn get_hotcrp_application(schema: &str, in_memory: bool) -> Application {
    let mut disguises = vec![get_conference_anon_disguise(), get_gdpr_removal_disguise()];

    Application {
        disguises: disguises,
        schema: get_schema_statements(schema, in_memory),
        vaults: get_vault_statements(in_memory),
    }
}
