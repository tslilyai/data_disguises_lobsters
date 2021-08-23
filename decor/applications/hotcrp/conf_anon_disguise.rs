use crate::datagen::*;
use crate::*;
use decor::disguise::*;
use decor::predicate::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const ROLE_PC: u64 = 1;

pub fn get_disguise(user: User) -> Disguise {
    Disguise {
        did: CONF_ANON_DISGUISE_ID,
        user: Some(user),
        table_disguises: get_table_disguises(),
        table_info: get_table_info(),
        guise_gen: get_guise_gen(),
    }
}

fn get_guise_gen() -> Arc<RwLock<HashMap<String, GuiseGen>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "ContactInfo".to_string(),
        GuiseGen {
            col_generation: Box::new(get_insert_guise_contact_info_cols),
            val_generation: Box::new(get_insert_guise_contact_info_vals),
        },
    );
    Arc::new(RwLock::new(hm))
}

fn get_table_info() -> Arc<RwLock<HashMap<String, TableInfo>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "ContactInfo".to_string(),
        TableInfo {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperWatch".to_string(),
        TableInfo {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperWatchId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperReviewPreference".to_string(),
        TableInfo {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperRevPrefId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperReviewRefused".to_string(),
        TableInfo {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
        },
    );
    hm.insert(
        "ActionLog".to_string(),
        TableInfo {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            owner_cols: vec![
                "contactId".to_string(),
                "destContactId".to_string(),
                "trueContactId".to_string(),
            ],
        },
    );
    hm.insert(
        "ReviewRating".to_string(),
        TableInfo {
            name: "ReviewRating".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperComment".to_string(),
        TableInfo {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );

    hm.insert(
        "PaperReview".to_string(),
        TableInfo {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            owner_cols: vec!["contactId".to_string(), "requestedBy".to_string()],
        },
    );

    hm.insert(
        "Paper".to_string(),
        TableInfo {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            owner_cols: vec![
                "leadContactId".to_string(),
                "managerContactId".to_string(),
                "shepherdContactId".to_string(),
            ],
        },
    );
    Arc::new(RwLock::new(hm))
}

fn get_table_disguises() -> HashMap<String, Arc<RwLock<Vec<Transform>>>> {
    let mut hm = HashMap::new();

    hm.insert(
        "ContactInfo".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            Transform {
                pred: vec![vec![PredClause::ColValCmp {
                    col: "roles".to_string(),
                    val: ROLE_PC.to_string(),
                    op: BinaryOperator::BitwiseAnd,
                }]],
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "email".to_string(),
                    generate_modified_value: Box::new(|_| users::get_random_email()),
                    satisfies_modification: Box::new(|v| {
                        v.contains("anonymous") && v.contains("secret")
                    }),
                })),
                global: false,
            },
        ])),
    );
    /*Arc::new(RwLock::new(TableDisguise {
        name: "PaperWatch".to_string(),
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
    })),*/
    hm
}
