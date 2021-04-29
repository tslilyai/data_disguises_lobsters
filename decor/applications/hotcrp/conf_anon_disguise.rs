use crate::datagen::*;
use crate::*;
use decor::types::*;
use sql_parser::ast::*;

const ROLE_PC: u64 = 1;

pub fn get_disguise() -> Disguise {
    Disguise {
        user_id: None,
        disguise_id: CONF_ANON_DISGUISE_ID,
        update_names: get_update_names(),
        remove_names: get_remove_names(),
        guise_info: GuiseInfo {
            name: SCHEMA_UID_TABLE.to_string(),
            id_col: SCHEMA_UID_COL.to_string(),
            col_generation: Box::new(get_insert_guise_contact_info_cols),
            val_generation: Box::new(get_insert_guise_contact_info_vals),
        },
    }
}

fn get_update_names() -> Vec<TableDisguise> {
    vec![
        TableDisguise {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            cols_to_update: vec![ColumnModification {
                col: "email".to_string(),
                // only modify if a PC member
                should_modify: Some(Expr::BinaryOp {
                    left: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new("role")])),
                        op: BinaryOperator::BitwiseAnd,
                        right: Box::new(Expr::Value(Value::Number(ROLE_PC.to_string()))),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(1.to_string()))),
                }),
                generate_modified_value: Box::new(users::get_random_email),
                satisfies_modification: Box::new(|v| {
                    v.contains("anonymous") && v.contains("secret")
                }),
            }],
            fks_to_decor: vec![],
        },
        TableDisguise {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableDisguise {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableDisguise {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "refusedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableDisguise {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "destContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "trueContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableDisguise {
            name: "ReviewRating".to_string(),
            id_cols: vec![
                "paperId".to_string(),
                "reviewId".to_string(),
                "contactId".to_string(),
            ],
            cols_to_update: vec![],
            fks_to_decor: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableDisguise {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableDisguise {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableDisguise {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            cols_to_update: vec![],
            fks_to_decor: vec![
                FK {
                    referencer_col: "leadContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "managerContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "shepherdContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
    ]
}

fn get_remove_names() -> Vec<TableDisguise> {
    vec![]
}
