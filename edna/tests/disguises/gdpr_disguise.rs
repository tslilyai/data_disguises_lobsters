use crate::disguises::*;
use edna::spec::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn get_disguise(user_id: u64) -> Disguise {
    Disguise {
        did: 0,
        user: user_id.to_string(),
        table_disguises: get_table_disguises(user_id),
        table_info: get_table_info(),
        use_txn: true,
    }
}

fn get_table_disguises(user_id: u64) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // REMOVE USER
    hm.insert(
        "users".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_eq_pred("id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    // REMOVE STORIES
    hm.insert(
        "stories".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    // DECOR MOD
    hm.insert(
        "moderations".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
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
    hm
}
