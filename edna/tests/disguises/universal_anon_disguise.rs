use crate::disguises::*;
use decor::disguise::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn get_disguise() -> Disguise {
    Disguise {
        did: 1,
        user: None,
        table_disguises: get_table_disguises(),
        table_info: get_table_info(),
        guise_gen: get_guise_gen(),
    }
}

fn get_table_disguises() -> HashMap<String, Arc<RwLock<Vec<Transform>>>> {
    let mut hm = HashMap::new();

    // DECOR MOD AND STORIES

    hm.insert(
        "moderations".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            Transform {
                pred: get_true_pred(),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "moderator_user_id".to_string(),
                })),
                global: false,
            },
            Transform {
                pred: get_true_pred(),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "user_id".to_string(),
                })),
                global: false,
            },
        ])),
    );
    // only users can restore their stories
    hm.insert(
        "stories".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            Transform {
                pred: get_true_pred(),
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
