use decor::disguise::*;
use sql_parser::ast::{DataType, Expr, Ident, ObjectName, Statement, UnaryOperator, Value};

fn empty_query() -> Query {
    Query {
            ctes: vec![],
            // TODO how to populate with uid, old/new value contents??
            // pass in as a closure?
            body: SetExpr::Values(Values(vec![])), 
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }))
    }
}

pub fn get_conference_anon_disguise() -> Disguise {
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
    
    // vault: insert found value
    // obj: delete found values
    for name in remove_names {
        txns.push(DisguiseTxn {
            // used to select objects to modify
            predicate: Select {
                distinct: true,
                projection: SelectItem::Wildcard,
                from: str_to_tablewithjoins(name),
                selection: None,
                group_by: vec![],
                having: None,
            },
            
            // called on every selected object
            vault_updates: vec![Statement::Insert(InsertStatement {
                table_name: helpers::str_to_objname(&format!(VAULT_FMT_STR, name)),
                columns: VAULT_COL_NAMES,
                source: InsertSource::Query(Box::new(empty_query())],
            })],

            // called on every selected object
            obj_updates: vec![Statement::Delete(DeleteStatement {
                table_name: helpers::str_to_objname(name),
                // will update with Select predicate
                selection: None, 
            })],
        });
    }

    // decorrelate disguises
    let decor_names = vec![
        table_fks {
            referencer_name: "PaperReviewRefused",
            fks: vec![ 
                fk{ 
                    referencer_col: "requestedBy",
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },
                fk {
                    referencer_col: "refusedBy", 
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                }
            ],
        },
        table_fks {
            referencer_name: "ActionLog",
            fks: vec![ 
                fk{ 
                    referencer_col: "contactId",
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },
                fk {
                    referencer_col: "destContactId", 
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },
                fk {
                    referencer_col: "trueContactId", 
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                }
            ],
        },
        table_fks {
            referencer_name: "ReviewRating",
            fks: vec![ 
                fk{ 
                    referencer_col: "contactId",
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },
            ],
        },
        table_fks {
            referencer_name: "PaperComment",
            fks: vec![ 
                fk{ 
                    referencer_col: "contactId",
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },
            ],
        },
        table_fks {
            referencer_name: "PaperReview",
            fks: vec![ 
                fk{ 
                    referencer_col: "contactId",
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },
                fk{ 
                    referencer_col: "requestedBy",
                    fk_name: "ContactInfo",
                    fk_col: "contactId",
                },

            ],
        },
    ];

    // vault:
    //  - save referenced object and referencer
    //  - save what anon object has been inserted into fk table
    // obj:
    //  - insert new anon row for fk table
    //  - update referencer fk column TODO how to get from insertion?
    //  - remove referenced object in fk table
    for table_fks in decor_names{
        txns.push(DisguiseTxn {
            predicate: Select {
                distinct: true,
                projection: SelectItem::Wildcard,
                from: str_to_tablewithjoins(table_fks.referencer_name),
                selection: None,
                group_by: vec![],
                having: None,
            },
            vault_updates: vec![],
            obj_updates: vec![],
        });
    }
    Disguise(txns)
}
