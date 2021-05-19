use crate::datagen::*;
use crate::*;
use decor::types::*;
use sql_parser::ast::*;

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
        is_owner: Box::new(move |uid| uid == user_id.to_string()),
        guise_info: GuiseInfo {
            name: SCHEMA_UID_TABLE.to_string(),
            id_col: SCHEMA_UID_COL.to_string(),
            col_generation: Box::new(get_insert_guise_contact_info_cols),
            val_generation: Box::new(get_insert_guise_contact_info_vals),
        },
    }
}

fn get_table_disguises(user_id: u64) -> Vec<TableDisguise> {
    use Transform::*;
    vec![
        // REMOVED
        TableDisguise {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperRevPrefId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperWatchId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "PaperConflict".to_string(),
            id_cols: vec!["paperConflictId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "TopicInterest".to_string(),
            id_cols: vec!["topicInterestId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "ReviewRating".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            owner_cols: vec!["contactId".to_string(), "requestedBy".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
    ]
}
