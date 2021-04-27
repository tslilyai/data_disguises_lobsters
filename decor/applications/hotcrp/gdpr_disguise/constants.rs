use decor::types::*;
pub fn get_remove_names() -> Vec<TableInfo> {
    vec![
        TableInfo {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "Capability".to_string(),
            id_cols: vec!["salt".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "PaperConflict".to_string(),
            id_cols: vec!["contactId".to_string(), "paperId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "TopicInterest".to_string(),
            id_cols: vec!["contactId".to_string(), "topicId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
    ]
}

pub fn get_update_names() -> Vec<TableInfo> {
    vec![
        TableInfo {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            used_cols: vec![],
            used_fks: vec![
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
                FK {
                    referencer_col: "refusedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
            ],
        },
        TableInfo {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            used_cols: vec![],
            used_fks: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
                FK {
                    referencer_col: "destContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
                FK {
                    referencer_col: "trueContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
            ],
        },
        TableInfo {
            name: "ReviewRating".to_string(),
            id_cols: vec![
                "paperId".to_string(),
                "reviewId".to_string(),
                "contactId".to_string(),
            ],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
                is_owner: true,
            }],
        },
        TableInfo {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            used_cols: vec![],
            used_fks: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
            ],
        },
        TableInfo {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            used_cols: vec![],
            used_fks: vec![
                FK {
                    referencer_col: "leadContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
                FK {
                    referencer_col: "managerContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
                FK {
                    referencer_col: "shepherdContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                    is_owner: true,
                },
            ],
        },
    ]
}
