use crate::disguises::*;
use edna::disguise::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn get_disguise(user_apikey: String) -> Disguise {
    Disguise {
        did: 0,
        user: user_apikey.clone(),
        table_disguises: get_table_disguises(user_apikey),
        table_info: get_table_info(),
        guise_gen: get_guise_gen(),
    }
}

fn get_table_disguises(user_apikey: String) -> HashMap<String, Arc<RwLock<Vec<Transform>>>> {
    let mut hm = HashMap::new();

    // REMOVE USER
    hm.insert(
        "users".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            Transform {
                pred: get_eq_pred("apikey", user_apikey.clone()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    // REMOVE ANSWERS 
    hm.insert(
        "answers".to_string(),
        Arc::new(RwLock::new(vec![Transform {
            pred: get_eq_pred("user", user_apikey),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );
    hm
}
