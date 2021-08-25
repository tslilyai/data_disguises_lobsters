use decor::disguise::*;
use decor::predicate::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

static GUISE_ID: AtomicU64 = AtomicU64::new(1 << 10);

pub fn get_disguise(user_id: u64) -> Disguise {
    Disguise {
        did: 0,
        user: Some(User { id: user_id }),
        table_disguises: get_table_disguises(user_id),
        table_info: get_table_info(),
        guise_gen: get_guise_gen(),
    }
}

fn get_insert_guise_cols() -> Vec<String> {
    vec!["id".to_string(), "username".to_string()]
}

fn get_insert_guise_vals() -> Vec<Expr> {
    let gid = GUISE_ID.fetch_add(1, Ordering::SeqCst);
    vec![
        Expr::Value(Value::Number(gid.to_string())),
        Expr::Value(Value::String(gid.to_string())),
    ]
}

fn get_eq_pred(col: &str, val: String) -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::ColValCmp {
        col: col.to_string(),
        val: val,
        op: BinaryOperator::Eq,
    }]]
}

fn get_guise_gen() -> Arc<RwLock<HashMap<String, GuiseGen>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "users".to_string(),
        GuiseGen {
            col_generation: Box::new(get_insert_guise_cols),
            val_generation: Box::new(get_insert_guise_vals),
        },
    );
    Arc::new(RwLock::new(hm))
}

fn get_table_info() -> Arc<RwLock<HashMap<String, TableInfo>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "users".to_string(),
        TableInfo {
            name: "users".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["id".to_string()],
        },
    );
    hm.insert(
        "moderations".to_string(),
        TableInfo {
            name: "moderations".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["moderator_user_id".to_string(), "user_id".to_string()],
        },
    );
    hm.insert(
        "stories".to_string(),
        TableInfo {
            name: "stories".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
        },
    );
    Arc::new(RwLock::new(hm))
}

fn get_table_disguises(user_id: u64) -> HashMap<String, Arc<RwLock<Vec<Transform>>>> {
    let mut hm = HashMap::new();

    // REMOVE USER
    hm.insert(
        "users".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            Transform {
                pred: get_eq_pred("id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
        ])),
    );
    // DECOR MOD AND STORIES
    hm.insert(
        "moderations".to_string(),
        Arc::new(RwLock::new(vec![
            // only modify if a PC member
            Transform {
                pred: get_eq_pred("moderator_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Decor {
                    fk_name: "users".to_string(),
                    fk_col: "moderator_user_id".to_string(),
                })),
                global: false,
            },
            Transform {
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
            // only modify if a PC member
            Transform {
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
