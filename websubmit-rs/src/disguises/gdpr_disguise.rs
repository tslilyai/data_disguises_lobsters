use crate::disguises::*;
use edna::disguise::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn get_disguise_id() -> u64 {
    return 0;
}

pub fn get_disguise(user_email: String) -> Disguise {
    Disguise {
        did: 0,
        user: user_email.clone(),
        table_disguises: get_table_disguises(user_email),
        table_info: get_table_info(),
        guise_gen: get_guise_gen(),
    }
}

fn get_table_disguises(user_email: String) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // REMOVE USER
    hm.insert(
        "users".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            ObjectTransformation {
                pred: get_eq_pred("email", user_email.clone()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    // REMOVE ANSWERS 
    hm.insert(
        "answers".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user", user_email),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm
}
