use decor::disguises::*;
use decor::helpers::*;
use crate::disguise::*;

pub const SCHEMA_UID_COL: &'static str = "contactID";
pub const SCHEMA_UID_TABLE: &'static str = "ContactInfo";

pub fn get_table_names() -> Vec<&'static str> {
    vec![
        // modified
        "PaperReviewRefused",
        "ActionLog",
        "ReviewRating",
        "PaperReview",
        "PaperComment",
        // deleted
        "PaperWatch",
        "PaperReviewPreference",
        "Capability",
        "PaperConflict",
        "TopicInterest",
        "PaperTag",
        "PaperTagAnno",
        "ContactInfo",
    ]
}

pub fn get_hotcrp_application(schema: &str, in_memory: bool) -> Application {
    let disguises = vec![Box::new(apply_conference_anon_disguise), Box::new(apply_gdpr_disguise)];

    Application {
        disguises: disguises,
        schema: get_create_schema_statements(schema, in_memory),
        vault: get_create_vault_statements(get_table_names(), in_memory),
    }
}
