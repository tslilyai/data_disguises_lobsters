use crate::disguises::*;
use edna::disguise::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn get_disguise_id() -> u64 {
    return 1;
}

pub fn get_disguise() -> Disguise {
    Disguise {
        did: 1,
        user: String::new(),
        table_disguises: get_table_disguises(),
        table_info: get_table_info(),
        guise_gen: get_guise_gen(),
    }
}

fn get_table_disguises() -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // DECOR ANSWERS 
    hm.insert(
        "answers".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_true_pred(),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    group_by_cols: vec!["lec".to_string()],
                    fk_name: "users".to_string(),
                    fk_col: "user".to_string(),
                })),
                global: false,
            },
        ])),
    );

    hm
}
