use crate::*;
use edna::predicate::*;
use edna::spec::*;
use edna::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub fn apply(
    edna: Arc<Mutex<EdnaClient>>,
    uid: u64,
    decryption_cap: tokens::DecryptCap,
    loc_caps: Vec<tokens::LocCap>,
) -> Result<
    (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ),
    mysql::Error,
> {
    let gdpr_disguise = get_disguise(uid);
    edna.lock()
        .unwrap()
        .apply_disguise(Arc::new(gdpr_disguise), decryption_cap, loc_caps)
}

pub fn reveal(
    edna: Arc<Mutex<EdnaClient>>,
    decryption_cap: tokens::DecryptCap,
    diff_loc_caps: Vec<tokens::LocCap>,
    own_loc_caps: Vec<tokens::LocCap>,
) -> Result<(), mysql::Error> {
    edna.lock().unwrap().reverse_disguise(
        get_disguise_id(),
        decryption_cap,
        diff_loc_caps,
        own_loc_caps,
    )
}

fn get_eq_pred(col: &str, val: String) -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::ColValCmp {
        col: col.to_string(),
        val: val,
        op: BinaryOperator::Eq,
    }]]
}

fn get_disguise_id() -> u64 {
    0
}

pub fn get_disguise(user_id: u64) -> Disguise {
    Disguise {
        did: get_disguise_id(),
        user: user_id.to_string(),
        table_disguises: get_table_disguises(user_id),
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
        "Capability".to_string(),
        TableInfo {
            name: "Capability".to_string(),
            id_cols: vec!["salt".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperConflict".to_string(),
        TableInfo {
            name: "PaperConflict".to_string(),
            id_cols: vec!["paperConflictId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "TopicInterest".to_string(),
        TableInfo {
            name: "TopicInterest".to_string(),
            id_cols: vec!["topicInterestId".to_string()],
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

fn get_table_disguises(user_id: u64) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // REMOVED
    hm.insert(
        "ContactInfo".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    hm.insert(
        "PaperReviewPreference".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    hm.insert(
        "PaperWatch".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    hm.insert(
        "Capability".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    hm.insert(
        "PaperConflict".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    hm.insert(
        "TopicInterest".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );

    // DECORRELATED
    hm.insert(
        "PaperReviewRefused".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("requestedBy", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "requestedBy".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("refusedBy", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "refusedBy".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "ActionLog".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("destContactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "destContactId".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("trueContactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "trueContactId".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "ReviewRating".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "PaperComment".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "PaperReview".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("contactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("requestedBy", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "requestedBy".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "PaperReview".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("leadContactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "leadContactId".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("managerContactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "managerContactId".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("shepherdContactId", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "shepherdContactId".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm
}
