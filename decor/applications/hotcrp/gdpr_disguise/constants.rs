use decor::disguises::*;
use rand::{distributions::Alphanumeric, Rng};
use sql_parser::ast::*;

pub const ANON_PW: &'static str = "password123";

pub fn get_remove_names() -> Vec<&'static str> {
    vec![
        "ContactInfo",
        "PaperReviewPreference",
        "PaperWatch",
        "Capability",
        "PaperConflict",
        "TopicInterest",
        //"PaperTag",
        //"PaperTagAnno",
    ]
}

pub fn get_contact_info_cols() -> Vec<&'static str> {
    vec![
        "firstName",
        "lastName",
        "unaccentedName",
        "email",
        "preferredEmail",
        "affiliation",
        "phone",
        "country",
        "password",
        "passwordTime",
        "passwordUseTime",
        "collaborators",
        "updateTime",
        "lastLogin",
        "defaultWatch",
        "roles",
        "disabled",
        "contactTags",
        "data",
    ]
}

fn get_random_email() -> String {
    let randstr: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();
    format!("anonymous{}@secret.mail", randstr)
}

pub fn get_contact_info_vals() -> Vec<Expr> {
    vec![
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(get_random_email())),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Null),
        Expr::Value(Value::Null),
        Expr::Value(Value::String(ANON_PW.to_string())),
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

pub fn get_decor_names() -> Vec<TableFKs> {
    vec![
        TableFKs {
            name: "PaperReviewRefused".to_string(),
            fks: vec![
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "refusedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableFKs {
            name: "ActionLog".to_string(),
            fks: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "destContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "trueContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableFKs {
            name: "ReviewRating".to_string(),
            fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableFKs {
            name: "PaperComment".to_string(),
            fks: vec![FK {
                referencer_col: "contactId".to_string(),
                fk_name: "ContactInfo".to_string(),
                fk_col: "contactId".to_string(),
            }],
        },
        TableFKs {
            name: "PaperReview".to_string(),
            fks: vec![
                FK {
                    referencer_col: "contactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "requestedBy".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
        TableFKs {
            name: "Paper".to_string(),
            fks: vec![
                FK {
                    referencer_col: "leadContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "managerContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
                FK {
                    referencer_col: "shepherdContactId".to_string(),
                    fk_name: "ContactInfo".to_string(),
                    fk_col: "contactId".to_string(),
                },
            ],
        },
    ]
}
