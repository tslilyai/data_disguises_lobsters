use decor::disguise::*;
use sql_parser::ast::{DataType, Expr, Ident, ObjectName, Statement, UnaryOperator, Value};

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
                    fk_col: "contactId",A
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
            vault_updates: vec![Statement::Insert(InsertStatement {

            })],
            obj_updates: vec![Statement::Delete(DeleteStatement {
                table_name: helpers::str_to_objname(name),
                selection: None,
            })],
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
            obj_updates: vec![],
        });
    }
    Disguise(txns)
}
