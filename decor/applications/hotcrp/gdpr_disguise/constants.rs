use decor::types::*;

pub fn get_remove_names() -> Vec<TableFKs> {
    vec![
        TableFKs {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            fks: vec![],
        },
        TableFKs {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            fks: vec![],
        },
        TableFKs {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            fks: vec![],
        },
        TableFKs {
            name: "Capability".to_string(),
            id_cols: vec!["salt".to_string()],
            fks: vec![],
        },
        TableFKs {
            name: "PaperConflict".to_string(),
            id_cols: vec!["contactId".to_string(), "paperId".to_string()],
            fks: vec![],
        },
        TableFKs {
            name: "TopicInterest".to_string(),
            id_cols: vec!["contactId".to_string(), "topicId".to_string()],
            fks: vec![],
        },
    ]
}

pub fn get_decor_names() -> Vec<TableFKs> {
    vec![
        TableFKs {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            fks: vec![
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
        TableFKs {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            fks: vec![
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
        TableFKs {
            name: "ReviewRating".to_string(),
            id_cols: vec![
                "paperId".to_string(),
                "reviewId".to_string(),
                "contactId".to_string(),
            ],
            fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableFKs {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableFKs {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            fks: vec![
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
        TableFKs {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            fks: vec![
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
