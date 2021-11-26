use crate::datagen::*;
use crate::*;
use edna::predicate::*;
use edna::*;
use edna::spec::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

const ROLE_PC: u64 = 1;

pub fn apply(
    edna: Arc<Mutex<EdnaClient>>,
) -> Result<
    (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ),
    mysql::Error,
> {
    let anon_disguise = get_disguise();
    edna.lock()
        .unwrap()
        .apply_disguise(Arc::new(anon_disguise), vec![], vec![])
}

// no revealing

fn get_disguise_id() -> u64 {
    1
}

pub fn get_disguise() -> Disguise {
    Disguise {
        did: get_disguise_id(),
        user: 0.to_string(),
        table_disguises: get_table_disguises(),
        table_info: get_table_info(),
    }
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

fn get_table_disguises() -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    hm.insert(
        "ContactInfo".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
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
                global: true,
            },
        ])),
    );
    hm.insert(
        "PaperWatch".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: vec![],
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            })),
            global: true,
        }])),
    );
    hm.insert(
        "PaperReviewPreference".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: vec![],
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            })),
            global: true,
        }])),
    );
    hm.insert(
        "PaperReviewRefused".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "requestedBy".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "refusedBy".to_string(),
                })),
                global: true,
            },
        ])),
    );
    hm.insert(
        "ActionLog".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "destContactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "trueContactId".to_string(),
                })),
                global: true,
            },
        ])),
    );
    hm.insert(
        "ReviewRating".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: vec![],
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            })),
            global: true,
        }])),
    );
    hm.insert(
        "PaperComment".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: vec![],
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            })),
            global: true,
        }])),
    );
    hm.insert(
        "PaperReview".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "requestedBy".to_string(),
                })),
                global: true,
            },
        ])),
    );
    hm.insert(
        "Paper".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "leadContactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "managerContactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "shepherdContactId".to_string(),
                })),
                global: true,
            },
        ])),
    );
    hm
}
