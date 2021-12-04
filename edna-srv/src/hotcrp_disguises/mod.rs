mod guise_gen;
pub mod gdpr_disguise;
pub mod conf_anon_disguise;

pub use guise_gen::*;
use edna::spec::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use edna::{DID, UID};
use std::str::FromStr;

pub fn get_disguise_with_ids(id: DID, uid: UID) -> Arc<Disguise> {
    if id == gdpr_disguise::get_disguise_id() {
        return Arc::new(gdpr_disguise::get_disguise(u64::from_str(&uid).unwrap()));
    }
    else if id == conf_anon_disguise::get_disguise_id() {
        return Arc::new(conf_anon_disguise::get_disguise());
    } 
    unimplemented!("Does not suppport disguise");
}

pub fn get_table_info() -> Arc<RwLock<HashMap<String, TableInfo>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "ContactInfo".to_string(),
        TableInfo {
            name: "ContactInfo".to_string(),
            id_cols: vec!["contactId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperWatch".to_string(),
        TableInfo {
            name: "PaperWatch".to_string(),
            id_cols: vec!["paperWatchId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperReviewPreference".to_string(),
        TableInfo {
            name: "PaperReviewPreference".to_string(),
            id_cols: vec!["paperRevPrefId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "Capability".to_string(),
        TableInfo {
            name: "Capability".to_string(),
            id_cols: vec!["salt".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperConflict".to_string(),
        TableInfo {
            name: "PaperConflict".to_string(),
            id_cols: vec!["paperConflictId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "TopicInterest".to_string(),
        TableInfo {
            name: "TopicInterest".to_string(),
            id_cols: vec!["topicInterestId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );

    hm.insert(
        "PaperReviewRefused".to_string(),
        TableInfo {
            name: "PaperReviewRefused".to_string(),
            id_cols: vec!["paperId".to_string(), "email".to_string()],
            owner_cols: vec!["requestedBy".to_string(), "refusedBy".to_string()],
        },
    );
    hm.insert(
        "ActionLog".to_string(),
        TableInfo {
            name: "ActionLog".to_string(),
            id_cols: vec!["logId".to_string()],
            owner_cols: vec![
                "contactId".to_string(),
                "destContactId".to_string(),
                "trueContactId".to_string(),
            ],
        },
    );
    hm.insert(
        "ReviewRating".to_string(),
        TableInfo {
            name: "ReviewRating".to_string(),
            id_cols: vec!["ratingId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );
    hm.insert(
        "PaperComment".to_string(),
        TableInfo {
            name: "PaperComment".to_string(),
            id_cols: vec!["commentId".to_string()],
            owner_cols: vec!["contactId".to_string()],
        },
    );

    hm.insert(
        "PaperReview".to_string(),
        TableInfo {
            name: "PaperReview".to_string(),
            id_cols: vec!["reviewId".to_string()],
            owner_cols: vec!["contactId".to_string(), "requestedBy".to_string()],
        },
    );

    hm.insert(
        "Paper".to_string(),
        TableInfo {
            name: "Paper".to_string(),
            id_cols: vec!["paperId".to_string()],
            owner_cols: vec![
                "leadContactId".to_string(),
                "managerContactId".to_string(),
                "shepherdContactId".to_string(),
            ],
        },
    );
    Arc::new(RwLock::new(hm))
}
