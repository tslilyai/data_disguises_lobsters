use crate::datagen::*;
use crate::*;
use edna::disguises::*;
use edna::EdnaClient;
use sql_parser::ast::*;

pub fn get_insert_guise_contact_info_cols() -> Vec<&'static str> {
    vec![id, username, karma, is_guise]
}

pub fn get_insert_guise_contact_info_vals() -> Vec<Expr> {
    vec![
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::String(String::new())),
        Expr::Value(Value::Boolean(true)),
    ]
}

pub fn get_guise_gen() -> Arc<RwLock<GuiseGen>> {
    Arc::new(RwLock::new(GuiseGen {
        guise_name: "users".to_string(),
        guise_id_col: "id".to_string(),
        col_generation: Box::new(get_insert_guise_cols),
        val_generation: Box::new(get_insert_guise_vals),
    }))
}

pub fn get_did() -> DID {
    0
}

pub fn apply(
    edna: &mut EdnaClient,
    user: UID,
    decryption_cap: tokens::DecryptCap,
    loc_caps: Vec<tokens::LocCap>,
    is_baseline: bool,
) -> Result<
    (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ),
    mysql::Error,
> {
    if is_baseline {
        return Ok((HashMap::new(), HashMap::new()));
    }
    let gdpr_disguise = get_disguise(user);
    edna.apply_disguise(Arc::new(gdpr_disguise), decryption_cap, loc_caps)
}

pub fn reveal(
    edna: &mut EdnaClient,
    decryption_cap: tokens::DecryptCap,
    diff_loc_caps: Vec<tokens::LocCap>,
    own_loc_caps: Vec<tokens::LocCap>,
    is_baseline: bool,
) -> Result<(), mysql::Error> {
    if is_baseline {
        return Ok(());
    }
    edna.reverse_disguise(get_did(), decryption_cap, diff_loc_caps, own_loc_caps)
}

fn get_disguise(user_email: UID) -> Disguise {
    Disguise {
        did: 0,
        user: user_email.clone(),
        table_disguises: get_table_disguises(user_email),
        table_info: get_table_info(),
    }
}

fn get_table_disguises(
    user_email: String,
) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
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

pub fn get_table_info() -> Arc<RwLock<HashMap<String, TableInfo>>> {
    let mut hm = HashMap::new();
    hm.insert(
        "users".to_string(),
        TableInfo {
            name: "users".to_string(),
            id_cols: vec!["email".to_string()],
            owner_cols: vec!["email".to_string()],
        },
    );
    hm.insert(
        "lectures".to_string(),
        TableInfo {
            name: "lectures".to_string(),
            id_cols: vec!["id".to_string()],
            owner_cols: vec![],
        },
    );
    hm.insert(
        "questions".to_string(),
        TableInfo {
            name: "questions".to_string(),
            id_cols: vec!["lec".to_string(), "q".to_string()],
            owner_cols: vec![],
        },
    );
    hm.insert(
        "answers".to_string(),
        TableInfo {
            name: "answers".to_string(),
            id_cols: vec!["user".to_string(), "lec".to_string(), "q".to_string()],
            owner_cols: vec!["user".to_string()],
        },
    );
    Arc::new(RwLock::new(hm))
}

pub fn get_eq_pred(col: &str, val: String) -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::ColValCmp {
        col: col.to_string(),
        val: val,
        op: BinaryOperator::Eq,
    }]]
}
