use crate::datagen::*;
use crate::*;
use edna::predicate::*;
use edna::spec::*;
use edna::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const ROLE_PC: u64 = 1;

pub fn apply(edna: &mut EdnaClient) -> Result<HashMap<(UID, DID), tokens::LocCap>, mysql::Error> {
    let anon_disguise = get_disguise();
    edna.apply_disguise(Arc::new(anon_disguise), vec![], vec![])
}

// no revealing

pub fn get_disguise_id() -> u64 {
    1
}

pub fn get_disguise() -> Disguise {
    Disguise {
        did: get_disguise_id(),
        user: 0.to_string(),
        table_disguises: get_table_disguises(),
        table_info: disguises::get_table_info(),
        use_txn: true,
    }
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
            pred: vec![vec![PredClause::Bool(true)]],
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
            pred: vec![vec![PredClause::Bool(true)]],
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
                pred: vec![vec![PredClause::Bool(true)]],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "requestedBy".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![vec![PredClause::Bool(true)]],
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
                pred: vec![vec![PredClause::Bool(true)]],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![vec![PredClause::Bool(true)]],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "destContactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![vec![PredClause::Bool(true)]],
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
            pred: vec![vec![PredClause::Bool(true)]],
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
            pred: vec![vec![PredClause::Bool(true)]],
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
                pred: vec![vec![PredClause::Bool(true)]],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![vec![PredClause::Bool(true)]],
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
                pred: vec![vec![PredClause::Bool(true)]],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "leadContactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![vec![PredClause::Bool(true)]],
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "managerContactId".to_string(),
                })),
                global: true,
            },
            ObjectTransformation {
                pred: vec![vec![PredClause::Bool(true)]],
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
