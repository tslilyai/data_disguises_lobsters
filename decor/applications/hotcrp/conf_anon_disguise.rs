use crate::datagen::*;
use crate::*;
use decor::types::*;
use sql_parser::ast::*;
use std::sync::{Arc, RwLock};

const ROLE_PC: u64 = 1;

pub fn get_disguise() -> Disguise {
    Disguise {
        disguise_id: CONF_ANON_DISGUISE_ID,
        table_disguises: get_table_disguises(),
        is_owner: Arc::new(RwLock::new(Box::new(|_| true))),
        guise_info: Arc::new(RwLock::new(GuiseInfo {
            name: SCHEMA_UID_TABLE.to_string(),
            id_col: SCHEMA_UID_COL.to_string(),
            col_generation: Box::new(get_insert_guise_contact_info_cols),
            val_generation: Box::new(get_insert_guise_contact_info_vals),
            referencers: vec![],
        })),
        is_reversible: true,
    }
}

fn get_table_disguises() -> Vec<Arc<RwLock<TableDisguise>>> {
    vec![
        Arc::new(RwLock::new(TableDisguise {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![
                // only modify if a PC member
                (
                    Some(Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(vec![Ident::new("roles")])),
                            op: BinaryOperator::BitwiseAnd,
                            right: Box::new(Expr::Value(Value::Number(ROLE_PC.to_string()))),
                        }),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(1.to_string()))),
                    }),
                    Arc::new(RwLock::new(Transform::Modify {
                        col: "email".to_string(),
                        generate_modified_value: Box::new(|_| users::get_random_email()),
                        satisfies_modification: Box::new(|v| {
                            v.contains("anonymous") && v.contains("secret")
                        }),
                    })),
                ),
            ],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperWatchId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                None,
                Arc::new(RwLock::new(Transform::Decor {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperRevPrefId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                None,
                Arc::new(RwLock::new(Transform::Decor {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
            transforms: vec![
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "requestedBy".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "refusedBy".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
            ],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            owner_cols: vec![
                "contactId".to_string(),
                "destContactId".to_string(),
                "trueContactId".to_string(),
            ],
            transforms: vec![
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "contactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "destContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "trueContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
            ],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "ReviewRating".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                None,
                Arc::new(RwLock::new(Transform::Decor {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                None,
                Arc::new(RwLock::new(Transform::Decor {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            owner_cols: vec!["contactId".to_string(), "requestedBy".to_string()],
            transforms: vec![
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "contactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "requestedBy".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
            ],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            owner_cols: vec![
                "leadContactId".to_string(),
                "managerContactId".to_string(),
                "shepherdContactId".to_string(),
            ],
            transforms: vec![
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "leadContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "managerContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
                (
                    None,
                    Arc::new(RwLock::new(Transform::Decor {
                        referencer_col: "shepherdContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    })),
                ),
            ],
        })),
    ]
}
