use edna::GuiseGen;
use edna::spec::*;
use rand::Rng;
use sql_parser::ast::*;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

pub fn get_insert_guise_cols() -> Vec<String> {
    vec![
        "contactId".to_string(),
        "firstName".to_string(),
        "lastName".to_string(),
        "unaccentedName".to_string(),
        "email".to_string(),
        "preferredEmail".to_string(),
        "affiliation".to_string(),
        "phone".to_string(),
        "country".to_string(),
        "password".to_string(),
        "passwordTime".to_string(),
        "passwordUseTime".to_string(),
        "collaborators".to_string(),
        "updateTime".to_string(),
        "lastLogin".to_string(),
        "defaultWatch".to_string(),
        "roles".to_string(),
        "disabled".to_string(),
        "contactTags".to_string(),
        "data".to_string(),
    ]
}

pub fn get_insert_guise_vals() -> Vec<Expr> {
    let mut rng = rand::thread_rng();
    let gid: u64 = rng.gen::<u64>();
    let email: String = format!("{}@anon.com", gid);
    let pass: String = format!("{}pass", gid);
    vec![
        Expr::Value(Value::Number(gid.to_string())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(email)),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(pass)),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(2.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Number(0.to_string())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
    ]
}

pub fn get_guise_gen() -> Arc<RwLock<GuiseGen>> {
    Arc::new(RwLock::new(GuiseGen {
        guise_name: "ContactInfo".to_string(),
        guise_id_col: "ContactId".to_string(),
        col_generation: Box::new(get_insert_guise_cols),
        val_generation: Box::new(get_insert_guise_vals),
    }))
}

pub fn get_table_info() -> Arc<RwLock<HashMap<String, TableInfo>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "comments".to_string(),
        TableInfo {
            name: "comments".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
        },
    );
    hm.insert(
        "hat_requests".to_string(),
        TableInfo {
            name: "hat_requests".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
        },
    );
    hm.insert(
        "hats".to_string(),
        TableInfo {
            name: "hats".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["granted_by_user_id".to_string(), "user_id".to_string()],
        },
    );
    hm.insert(
        "hidden_stories".to_string(),
        TableInfo {
            name: "hidden_stories".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
    hm.insert(
        "invitations".to_string(),
        TableInfo {
            name: "invitations".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
    hm.insert(
        "messages".to_string(),
        TableInfo {
            name: "messages".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["author_user_id".to_string(), "recipient_user_id".to_string()],
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
        "read_ribbons".to_string(),
        TableInfo {
            name: "read_ribbons".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
    hm.insert(
        "saved_stories".to_string(),
        TableInfo {
            name: "saved_stories".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
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
     hm.insert(
        "suggested_taggings".to_string(),
        TableInfo {
            name: "suggested_taggings".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
     hm.insert(
        "suggested_titles".to_string(),
        TableInfo {
            name: "suggested_titles".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
     hm.insert(
        "tag_filters".to_string(),
        TableInfo {
            name: "tag_filters".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
      hm.insert(
        "users".to_string(),
        TableInfo {
            name: "user".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["id".to_string()],
         },
    );
      hm.insert(
        "votes".to_string(),
        TableInfo {
            name: "votes".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec!["user_id".to_string()],
         },
    );
 
    Arc::new(RwLock::new(hm))
}
