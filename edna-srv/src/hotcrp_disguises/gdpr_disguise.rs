use crate::hotcrp_disguises::*;
use edna::predicate::*;
use edna::spec::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

fn get_eq_pred(col: &str, val: String) -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::ColValCmp {
        col: col.to_string(),
        val: val,
        op: BinaryOperator::Eq,
    }]]
}

pub fn get_disguise_id() -> u64 {
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

fn get_table_disguises(user_id: u64) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // REMOVED
    hm.insert(
        "ContactInfo".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "PaperReviewPreference".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "PaperWatch".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "Capability".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "PaperConflict".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "TopicInterest".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    // DECORRELATED
    hm.insert(
        "PaperReviewRefused".to_string(),
        Arc::new(RwLock::new(vec![
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
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            })),
            global: false,
        }])),
    );
    hm.insert(
        "PaperComment".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("contactId", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            })),
            global: false,
        }])),
    );
    hm.insert(
        "PaperReview".to_string(),
        Arc::new(RwLock::new(vec![
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
        "Paper".to_string(),
        Arc::new(RwLock::new(vec![
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
