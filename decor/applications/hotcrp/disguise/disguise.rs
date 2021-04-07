use decor::disguises::*;
use decor::helpers::*;

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
    let mut disguises = vec![];

    Application {
        disguises: disguises,
        schema: get_create_schema_statements(schema, in_memory),
        vault: get_create_vault_statements(get_table_names(), in_memory),
    }
}
