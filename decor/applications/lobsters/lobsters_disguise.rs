use crate::datagen::*;
use crate::*;
use decor::types::*;
use sql_parser::ast::*;

pub fn get_insert_guise_contact_info_cols() -> Vec<&'static str> {
    vec![id, username, karma, is_guise]
}

pub fn get_insert_guise_contact_info_vals() -> Vec<Expr> {
    vec![
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Boolean(true)),
    ]
}

fn get_table_disguises(user_id: u64) -> Vec<TableDisguise> {
    use Transform::*;
    vec![
        TableDisguise {
            name: "users".to_string(),
            id_cols: vec!["contactId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "stories".to_string(),
            id_cols: vec!["paperRevPrefId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "hats".to_string(),
            id_cols: vec!["paperWatchId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "comments".to_string(),
            id_cols: vec!["salt".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "moderations".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
            transforms: vec![
                Transform::Remove {
                    pred: Some(get_eq_expr(
                        "requestedBy",
                        Value::Number(user_id.to_string()),
                    )),
                },
                Transform::Remove {
                    pred: Some(get_eq_expr("refusedBy", Value::Number(user_id.to_string()))),
                },
            ],
        },
        TableDisguise {
            name: "invitations".to_string(),
            id_cols: vec!["logId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "votes".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Remove {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
            }],
        },
        TableDisguise {
            name: "messages".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![
                Remove {
                    pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                },
                Remove {
                    pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                },
            ],
        },
    ];
}
