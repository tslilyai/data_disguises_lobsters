

Disguise {
    disguise_name: "UserScrub",
    user_to_disguise: user_id,
    tables {
        "User": {
            generate_guise_info: [
                ("name", Random),
                ("email", Default(None)),
                ..
            ],
            transformations: [Remove(pred: "id" == user_id)],
        },
        "Review" : {
            guise_info: [], 
            transformations: [
                Decorrelate(
                    pred: "id" == user_id,
                    fk_column: "id",
                    fk_table: "User"
                )
            ],
        }
        "ReviewPreference" : {
            guise_info: [], 
            transformations: [Remove(pred: "id" == user_id)],
        }
    }
    
}


fn get_eq_expr(col: &str, val: Value) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![Ident::new(col)])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(val)),
    }
}

pub fn get_disguise(user_id: u64) -> Disguise {
    Disguise {
        table_disguises: get_table_disguises(user_id),
        guise_info: GuiseInfo {
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
            ]
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
            name: "Capability".to_string(),
            id_cols: vec!["salt".to_string()],
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
        // DECORRELATED
        TableDisguise {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
            transforms: vec![
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "requestedBy",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                Transform::Decor {
                    pred: Some(get_eq_expr("refusedBy", Value::Number(user_id.to_string()))),
                    referencer_col: "refusedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableDisguise {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![
                Transform::Decor {
                    pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "destContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "destContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "trueContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "trueContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableDisguise {
            name: "ReviewRating".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Transform::Decor {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableDisguise {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
            transforms: vec![Transform::Decor {
                pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableDisguise {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            owner_cols: vec!["contactId".to_string(), "requestedBy".to_string()],
            transforms: vec![
                Transform::Decor {
                    pred: Some(get_eq_expr("contactId", Value::Number(user_id.to_string()))),
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "requestedBy",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableDisguise {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            owner_cols: vec![
                "leadContactId".to_string(),
                "managerContactId".to_string(),
                "shepherdContactId".to_string(),
            ],
            transforms: vec![
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "leadContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "leadContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "managerContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "managerContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                Transform::Decor {
                    pred: Some(get_eq_expr(
                        "shepherdContactId",
                        Value::Number(user_id.to_string()),
                    )),
                    referencer_col: "shepherdContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
    ]
}