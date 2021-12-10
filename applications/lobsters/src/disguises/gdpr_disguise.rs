use crate::*;
use edna::predicate::*;
use edna::spec::*;
use edna::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn apply(
    edna: &mut EdnaClient,
    uid: u64,
    decryption_cap: tokens::DecryptCap,
    loc_caps: Vec<tokens::LocCap>,
) -> Result<HashMap<(UID, DID), tokens::LocCap>, mysql::Error> {
    let gdpr_disguise = get_disguise(uid);
    edna.apply_disguise(Arc::new(gdpr_disguise), decryption_cap, loc_caps)
}

pub fn reveal(
    edna: &mut EdnaClient,
    decryption_cap: tokens::DecryptCap,
    loc_caps: Vec<tokens::LocCap>,
) -> Result<(), mysql::Error> {
    edna.reverse_disguise(get_disguise_id(), decryption_cap, loc_caps)
}

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
        table_info: disguises::get_table_info(),
        use_txn: false,
    }
}

// generate modifications
fn gen_anon_comment(_: &str) -> String {
    "deleted_comment".to_string()
}
fn check_anon_comment(s: &str) -> bool {
    s == "deleted_comment"
}
fn gen_anon_title(_: &str) -> String {
    "deleted_story".to_string()
}
fn check_anon_title(s: &str) -> bool {
    s == "deleted_story"
}
fn gen_anon_url(_: &str) -> String {
    "deleted_url".to_string()
}
fn check_anon_url(s: &str) -> bool {
    s == "deleted_url"
}
fn gen_true_str(_: &str) -> String {
    true.to_string()
}
fn check_true(_: &str) -> bool {
    true
}

pub fn get_table_disguises(
    user_id: u64,
) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // REMOVE
    hm.insert(
        "hat_requests".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "hats".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "hidden_stories".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "invitations".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    hm.insert(
        "read_ribbons".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "saved_stories".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    hm.insert(
        "suggested_taggings".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "suggested_titles".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    hm.insert(
        "tag_filters".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm.insert(
        "users".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    // DECOR AND MODIFY
    hm.insert(
        "comments".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "comment".to_string(),
                    generate_modified_value: Box::new(gen_anon_comment),
                    satisfies_modification: Box::new(check_anon_comment),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "markeddown_comment".to_string(),
                    generate_modified_value: Box::new(gen_anon_comment),
                    satisfies_modification: Box::new(check_anon_comment),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "user_id".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "stories".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "url".to_string(),
                    generate_modified_value: Box::new(gen_anon_url),
                    satisfies_modification: Box::new(check_anon_url),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "title".to_string(),
                    generate_modified_value: Box::new(gen_anon_title),
                    satisfies_modification: Box::new(check_anon_title),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "user_id".to_string(),
                })),
                global: false,
            },
        ])),
    );

    // DECOR ONLY
    hm.insert(
        "messages".to_string(),
        Arc::new(RwLock::new(vec![
            // remove if both parties deleted
            ObjectTransformation {
                pred: vec![
                    vec![
                        PredClause::ColValCmp {
                            col: "author_user_id".to_string(),
                            val: user_id.to_string(),
                            op: BinaryOperator::Eq,
                        },
                        PredClause::ColValCmp {
                            col: "deleted_by_recipient".to_string(),
                            val: 1.to_string(),
                            op: BinaryOperator::Eq,
                        },
                    ],
                    vec![
                        PredClause::ColValCmp {
                            col: "recipient_user_id".to_string(),
                            val: user_id.to_string(),
                            op: BinaryOperator::Eq,
                        },
                        PredClause::ColValCmp {
                            col: "deleted_by_author".to_string(),
                            val: 1.to_string(),
                            op: BinaryOperator::Eq,
                        },
                    ],
                ],
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
            // otherwise modify and decor
            ObjectTransformation {
                pred: get_eq_pred("author_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "deleted_by_author".to_string(),
                    generate_modified_value: Box::new(gen_true_str),
                    satisfies_modification: Box::new(check_true),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("recipient_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "deleted_by_recipient".to_string(),
                    generate_modified_value: Box::new(gen_true_str),
                    satisfies_modification: Box::new(check_true),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("author_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "author_user_id".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("recipient_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "recipient_user_id".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "mod_notes".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "user_id".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "user_id".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "moderations".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_eq_pred("moderator_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "moderator_user_id".to_string(),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "user_id".to_string(),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "votes".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Decor {
                fk_name: "users".to_string(),
                fk_col: "user_id".to_string(),
            })),
            global: false,
        }])),
    );
    hm
}
