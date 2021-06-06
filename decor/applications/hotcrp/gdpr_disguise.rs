use crate::datagen::*;
use crate::*;
use decor::types::*;
use sql_parser::ast::*;
use std::sync::{Arc, RwLock};

fn get_eq_expr(col: &str, val: Value) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new(col)])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(val)),
    }
}

pub fn get_disguise(user_id: u64) -> Disguise {
    Disguise {
        disguise_id: GDPR_DISGUISE_ID,
        table_disguises: get_table_disguises(user_id),
        is_owner: Arc::new(RwLock::new(Box::new(move |uid| uid == user_id.to_string()))),
        guise_info: Arc::new(RwLock::new(GuiseInfo {
            name: SCHEMA_UID_TABLE.to_string(),
            id_col: SCHEMA_UID_COL.to_string(),
            col_generation: Box::new(get_insert_guise_contact_info_cols),
            val_generation: Box::new(get_insert_guise_contact_info_vals),
            referencers: vec![
                // TODO list all referencers
                // right now we're assuming that we're "smart" enough to only restore referencers
                // if we don't decorrelate them later
                ("PaperReviewPreference".to_string(), "contactId".to_string()),
                ("Capability".to_string(), "contactId".to_string()),
                ("PaperWatch".to_string(), "contactId".to_string()),
                ("PaperConflict".to_string(), "contactId".to_string()),
                ("TopicInterest".to_string(), "contactId".to_string()),
                ("PaperReview".to_string(), "contactId".to_string()),
                ("PaperReview".to_string(), "requestedBy".to_string()),
                ("PaperComment".to_string(), "contactId".to_string()),
                ("ReviewRating".to_string(), "contactId".to_string()),
                ("PaperReviewRefused".to_string(), "requestedBy".to_string()),
                ("PaperReviewRefused".to_string(), "refusedBy".to_string()),
                ("Paper".to_string(), "leadContactId".to_string()),
                ("Paper".to_string(), "managerContactId".to_string()),
                ("Paper".to_string(), "shepherdContactId".to_string()),
            ],
        })),
        is_reversible: true,
    }
}

fn get_table_disguises(user_id: u64) -> Vec<Arc<RwLock<TableDisguise>>> {
    use Transform::*;
    vec![
        // REMOVED
        Arc::new(RwLock::new(TableDisguise {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Remove,
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperRevPrefId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Remove,
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperWatchId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Remove,
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "Capability".to_string(),
            id_cols: vec!["salt".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Remove,
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperConflict".to_string(),
            id_cols: vec!["paperConflictId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Remove,
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "TopicInterest".to_string(),
            id_cols: vec!["topicInterestId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Remove,
            )],
        })),
        // DECORRELATED
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
            transforms: vec![
                (
                    Some(get_eq_expr(
                        "requestedBy",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "requestedBy".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
                (
                    Some(get_eq_expr("refusedBy", Value::Number(user_id.to_string()))),
                    Transform::Decor {
                        referencer_col: "refusedBy".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
            ],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![
                (
                    Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                    Transform::Decor {
                        referencer_col: "contactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
                (
                    Some(get_eq_expr(
                        "destContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "destContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
                (
                    Some(get_eq_expr(
                        "trueContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "trueContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
            ],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "ReviewRating".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Transform::Decor {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![(
                Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                Transform::Decor {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            )],
        })),
        Arc::new(RwLock::new(TableDisguise {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            owner_cols: vec!["contactId".to_string(), "requestedBy".to_string()],
            transforms: vec![
                (
                    Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                    Transform::Decor {
                        referencer_col: "contactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
                (
                    Some(get_eq_expr(
                        "requestedBy",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "requestedBy".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
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
                    Some(get_eq_expr(
                        "leadContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "leadContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
                (
                    Some(get_eq_expr(
                        "managerContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "managerContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
                (
                    Some(get_eq_expr(
                        "shepherdContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    Transform::Decor {
                        referencer_col: "shepherdContactId".to_string(),
                        fk_name: "ContactInfo".to_string(),
                        fk_col: "contactId".to_string(),
                    },
                ),
            ],
        })),
    ]
}
