use crate::helpers::*;
use crate::stats::QueryStat;
use crate::tokens::*;
use crate::{RowVal, DID, UID};
use log::warn;
use rand::{thread_rng, Rng};
use rsa::pkcs1::{FromRsaPublicKey, ToRsaPublicKey};
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use flamer::flame;

pub const REMOVE_GUISE: u64 = 1;
pub const DECOR_GUISE: u64 = 2;
pub const MODIFY_GUISE: u64 = 3;
pub const REMOVE_TOKEN: u64 = 5;
pub const MODIFY_TOKEN: u64 = 6;
pub const REMOVE_PRINCIPAL: u64 = 7;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DiffTokenWrapper {
    pub token_id: u64,
    pub did: DID,
    pub uid: UID,

    pub revealed: bool,
    pub is_global: bool,

    pub token_data: Vec<u8>,

    // FOR SECURITY DESIGN
    // for randomness
    pub nonce: u64,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EdnaDiffToken {
    pub token_id: u64,
    // metadata set by Edna
    pub update_type: u64,

    // guise information
    pub guise_name: String,
    pub guise_ids: Vec<RowVal>,

    // MODIFY/REMOVE : store old blobs
    pub old_value: Vec<RowVal>,

    // MODIFY: store new blobs
    pub new_value: Vec<RowVal>,

    // REMOVE/MODIFY
    pub old_token_blob: String,
    pub new_token_blob: String,

    // REMOVE PRINCIPAL
    pub uid: UID,
    pub pubkey: Vec<u8>,
    pub is_anon: bool,
    pub ownership_loc_caps: Vec<LocCap>,
    pub diff_loc_caps: Vec<LocCap>,
}

impl Hash for DiffTokenWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token_id.hash(state);
    }
}

impl Hash for EdnaDiffToken {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token_id.hash(state);
    }
}

pub fn diff_token_from_bytes(bytes: &Vec<u8>) -> DiffTokenWrapper {
    serde_json::from_slice(bytes).unwrap()
}

pub fn diff_token_to_bytes(token: &DiffTokenWrapper) -> Vec<u8> {
    let s = serde_json::to_string(token).unwrap();
    s.as_bytes().to_vec()
}

pub fn edna_diff_token_from_bytes(bytes: &Vec<u8>) -> EdnaDiffToken {
    serde_json::from_slice(bytes).unwrap()
}

pub fn edna_diff_token_to_bytes(token: &EdnaDiffToken) -> Vec<u8> {
    let s = serde_json::to_string(token).unwrap();
    s.as_bytes().to_vec()
}

// create diff token for generic data
pub fn new_generic_diff_token_wrapper(
    uid: &UID,
    did: DID,
    data: Vec<u8>,
    is_global: bool,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.nonce = thread_rng().gen();
    token.is_global = is_global;
    token.uid = uid.to_string();
    token.did = did;
    token.revealed = false;
    token.token_data = data;
    token
}

// create diff token for removing a principal
pub fn new_remove_principal_token_wrapper(
    uid: &UID,
    did: DID,
    pdata: &PrincipalData,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.nonce = thread_rng().gen();
    token.is_global = false;
    token.uid = uid.to_string();
    token.did = did;
    token.revealed = false;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.token_id = token.token_id;
    edna_token.uid = uid.clone();
    edna_token.pubkey = pdata.pubkey.to_pkcs1_der().unwrap().as_der().to_vec();
    edna_token.is_anon = pdata.is_anon.clone();
    edna_token.ownership_loc_caps = pdata.ownership_loc_caps.clone();
    edna_token.diff_loc_caps = pdata.diff_loc_caps.clone();
    edna_token.update_type = REMOVE_PRINCIPAL;

    token.token_data = edna_diff_token_to_bytes(&edna_token);
    token
}

// create diff tokens about diff tokens
pub fn new_token_modify(
    uid: UID,
    did: DID,
    old_token: &DiffTokenWrapper,
    changed_token: &DiffTokenWrapper,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.nonce = thread_rng().gen();
    token.is_global = false;
    token.uid = uid;
    token.did = did;
    token.revealed = false;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.update_type = MODIFY_TOKEN;
    edna_token.old_token_blob = serde_json::to_string(old_token).unwrap();
    edna_token.new_token_blob = serde_json::to_string(changed_token).unwrap();
    edna_token.token_id = token.token_id;

    token.token_data = edna_diff_token_to_bytes(&edna_token);
    token
}

pub fn new_token_remove(uid: UID, did: DID, changed_token: &DiffTokenWrapper) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.nonce = thread_rng().gen();
    token.is_global = false;
    token.uid = uid;
    token.did = did;
    token.revealed = false;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.update_type = REMOVE_TOKEN;
    edna_token.old_token_blob = serde_json::to_string(changed_token).unwrap();
    edna_token.token_id = token.token_id;

    token.token_data = edna_diff_token_to_bytes(&edna_token);
    token
}

// create diff tokens about db objects
pub fn new_delete_token_wrapper(
    did: DID,
    guise_name: String,
    guise_ids: Vec<RowVal>,
    old_value: Vec<RowVal>,
    is_global: bool,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.nonce = thread_rng().gen();
    token.did = did;
    token.revealed = false;
    token.is_global = is_global;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.update_type = REMOVE_GUISE;
    edna_token.guise_name = guise_name;
    edna_token.guise_ids = guise_ids;
    edna_token.old_value = old_value;
    edna_token.token_id = token.token_id;

    token.token_data = edna_diff_token_to_bytes(&edna_token);
    token
}

pub fn new_modify_token_wrapper(
    did: DID,
    guise_name: String,
    guise_ids: Vec<RowVal>,
    old_value: Vec<RowVal>,
    new_value: Vec<RowVal>,
    is_global: bool,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.nonce = thread_rng().gen();
    token.did = did;
    token.is_global = is_global;
    token.revealed = false;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.update_type = MODIFY_GUISE;
    edna_token.guise_name = guise_name;
    edna_token.guise_ids = guise_ids;
    edna_token.old_value = old_value;
    edna_token.new_value = new_value;
    edna_token.token_id = token.token_id;

    token.token_data = edna_diff_token_to_bytes(&edna_token);
    token
}

impl EdnaDiffToken {
    #[flame]
    pub fn reveal(
        &self,
        token_ctrler: &mut TokenCtrler,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<bool, mysql::Error> {
        match self.update_type {
            REMOVE_PRINCIPAL => {
                let pdata = PrincipalData {
                    pubkey: FromRsaPublicKey::from_pkcs1_der(&self.pubkey).unwrap(),
                    is_anon: self.is_anon,
                    ownership_loc_caps: self.ownership_loc_caps.clone(),
                    diff_loc_caps: self.diff_loc_caps.clone(),
                };
                token_ctrler.register_saved_principal(
                    &self.uid,
                    pdata.is_anon,
                    &pdata.pubkey,
                    pdata.ownership_loc_caps,
                    pdata.diff_loc_caps,
                    true,
                    conn,
                );
            }

            REMOVE_GUISE => {
                // get current guise in db
                let token_guise_selection = get_select_of_ids(&self.guise_ids);
                let selected = get_query_rows_str(
                    &str_select_statement(&self.guise_name, &token_guise_selection.to_string()),
                    conn,
                    stats.clone(),
                )?;

                // XXX data can be revealed even if it should've been disguised in the interim

                // item has been re-inserted, ignore
                if !selected.is_empty() {
                    // XXX true here because it's technically revealed?
                    return Ok(true);
                }

                // otherwise insert it
                let cols: Vec<String> = self.old_value.iter().map(|rv| rv.column.clone()).collect();
                let colstr = cols.join(",");
                let vals: Vec<String> = self
                    .old_value
                    .iter()
                    .map(|rv| {
                        if rv.value.is_empty() {
                            "\"\"".to_string()
                        } else {
                            for c in rv.value.chars() {
                                if !c.is_numeric() {
                                    return format!("\"{}\"", rv.value.clone());
                                }
                            }
                            rv.value.clone()
                        }
                    })
                    .collect();
                let valstr = vals.join(",");
                query_drop(
                    format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        self.guise_name, colstr, valstr
                    ),
                    conn,
                    stats.clone(),
                )?;
            }
            MODIFY_GUISE => {
                // get current guise in db
                let token_guise_selection = get_select_of_ids(&self.guise_ids);
                let selected = get_query_rows_str(
                    &str_select_statement(&self.guise_name, &token_guise_selection.to_string()),
                    conn,
                    stats.clone(),
                )?;

                // if field hasn't been modified, return it to original
                if selected.is_empty() || selected[0] != self.new_value {
                    warn!(
                    "DiffTokenWrapper Reveal: Modified value {:?} not equal to new value {:?}\n",
                    selected[0], self.new_value
                );
                    return Ok(false);
                }

                // ok, we can actually update this!
                let mut updates = vec![];
                for (i, newrv) in self.new_value.iter().enumerate() {
                    let new_val = newrv.value.clone();
                    let old_val = self.old_value[i].value.clone();
                    if new_val != old_val {
                        updates.push(Assignment {
                            id: Ident::new(newrv.column.clone()),
                            // XXX problem if it's not a string?
                            value: Expr::Value(Value::String(newrv.value.clone())),
                        })
                    }
                }
                query_drop(
                    Statement::Update(UpdateStatement {
                        table_name: string_to_objname(&self.guise_name),
                        assignments: updates,
                        selection: Some(token_guise_selection),
                    })
                    .to_string(),
                    conn,
                    stats.clone(),
                )?;
            }
            REMOVE_TOKEN => {
                // restore global token (may or may not have been revealed, but oh well!)
                let mut token: DiffTokenWrapper =
                    serde_json::from_str(&self.old_token_blob).unwrap();
                assert!(token.is_global);
                token_ctrler.insert_global_diff_token_wrapper(&mut token);
            }
            MODIFY_TOKEN => {
                let new_token: DiffTokenWrapper =
                    serde_json::from_str(&self.new_token_blob).unwrap();
                assert!(new_token.is_global);

                let (revealed, eq) = token_ctrler.check_global_diff_token_for_match(&new_token);

                // don't reveal if token has been modified
                if !eq {
                    return Ok(false);
                }

                // actually update token
                let old_token: DiffTokenWrapper =
                    serde_json::from_str(&self.old_token_blob).unwrap();
                token_ctrler.update_global_diff_token_from_old_to(&new_token, &old_token, None);

                // if token has been revealed, attempt to reveal old value of token
                if revealed {
                    let edna_old_token = edna_diff_token_from_bytes(&old_token.token_data);
                    return edna_old_token.reveal(token_ctrler, conn, stats.clone());
                }
            }
            _ => unimplemented!("Bad diff token update type?"), // do nothing for PRIV_KEY
        }
        Ok(true)
    }
}
