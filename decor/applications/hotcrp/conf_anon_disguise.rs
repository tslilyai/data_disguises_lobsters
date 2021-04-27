use crate::datagen::*;
use crate::*;
use decor::types::*;

pub fn get_disguise() -> Disguise {
    Disguise {
        user_id: None,
        disguise_id: CONF_ANON_DISGUISE_ID,
        update_names: get_update_names(),
        remove_names: get_remove_names(),
        guise_info: GuiseInfo {
            name: SCHEMA_UID_TABLE.to_string(),
            ids: vec![SCHEMA_UID_COL.to_string()],
            col_generation: Box::new(get_insert_guise_contact_info_cols),
            val_generation: Box::new(get_insert_guise_contact_info_vals),
        },
    }
}

fn get_update_names() -> Vec<TableInfo> {
    vec![
        TableInfo {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            used_cols: vec![ColumnModification {
                col: "email".to_string(),
                satisfies_modification: Box::new(|v| {
                    v.contains("anonymous") && v.contains("secret")
                }),
                generate_modified_value: Box::new(users::get_random_email),
            }],
            used_fks: vec![],
        },
        TableInfo {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperId".to_string(), "contactId".to_string()],
            used_cols: vec![],
            used_fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
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
            }],
        },
        TableInfo {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            used_cols: vec![],
            used_fks: vec![
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
        TableInfo {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            used_cols: vec![],
            used_fks: vec![
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
                },
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
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

fn get_remove_names() -> Vec<TableInfo> {
    vec![]
}
