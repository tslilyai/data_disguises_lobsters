use crate::helpers::*;
use crate::tokens::*;
use crate::spec;
use crate::{RowVal, DID, UID};
use log::warn;
use log::error;
use mysql::prelude::*;
use rand::{thread_rng, Rng};
use rsa::pkcs1::{FromRsaPublicKey, ToRsaPublicKey};
use sql_parser::ast::*;
use std::collections::HashSet;
use std::collections::HashMap;
use std::time;
use serde::{Deserialize, Serialize};
use std::mem::size_of_val;

pub const REMOVE_GUISE: u8 = 0;
pub const DECOR_GUISE: u8 = 1;
pub const MODIFY_GUISE: u8 = 2;
pub const REMOVE_TOKEN: u8 = 3;
pub const MODIFY_TOKEN: u8 = 4;
pub const REMOVE_PRINCIPAL: u8 = 5;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DiffTokenWrapper {
    pub did: DID,
    pub uid: UID,
    pub token_data: Vec<u8>,

    // FOR SECURITY DESIGN
    // for randomness
    pub nonce: u64,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EdnaDiffToken {
    // metadata set by Edna
    pub typ: u8,

    // guise information
    pub table: String,
    pub tabids: Vec<String>,

    // MODIFY/REMOVE : store old blobs
    pub old_value: Vec<RowVal>,

    // MODIFY: store new blobs
    pub col: String,
    pub old_val: String,
    pub new_val: String,

    // REMOVE PRINCIPAL
    pub pubkey: Vec<u8>,
}

pub fn diff_tokens_from_bytes(bytes: &Vec<u8>) -> Vec<DiffTokenWrapper> {
    bincode::deserialize(bytes).unwrap()
}
pub fn diff_token_from_bytes(bytes: &Vec<u8>) -> DiffTokenWrapper {
    bincode::deserialize(bytes).unwrap()
}
pub fn diff_token_to_bytes(token: &DiffTokenWrapper) -> Vec<u8> {
    bincode::serialize(token).unwrap()
}
pub fn edna_diff_token_from_bytes(bytes: &Vec<u8>) -> EdnaDiffToken {
    bincode::deserialize(bytes).unwrap()
}
pub fn edna_diff_token_to_bytes(token: &EdnaDiffToken) -> Vec<u8> {
    bincode::serialize(token).unwrap()
}

// create diff token for generic data
pub fn new_generic_diff_token_wrapper(
    uid: &UID,
    did: DID,
    data: Vec<u8>,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.nonce = thread_rng().gen();
    token.uid = uid.to_string();
    token.did = did;
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
    token.nonce = thread_rng().gen();
    token.uid = uid.to_string();
    token.did = did;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.pubkey = pdata.pubkey.to_pkcs1_der().unwrap().as_der().to_vec();
    edna_token.typ = REMOVE_PRINCIPAL;
    token.token_data = edna_diff_token_to_bytes(&edna_token);
    error!("REMOVE PRINC: nonce {}, uid {}, did {}, pubkey {}, tp {}, all: {}", 
        size_of_val(&token.nonce),
        size_of_val(&*token.uid),
        size_of_val(&token.did),
        size_of_val(&*edna_token.pubkey),
        size_of_val(&edna_token.typ),
        size_of_val(&token),
    );
    token
}

// create diff tokens about diff tokens
/*pub fn new_token_modify(
    uid: UID,
    did: DID,
    old_token: &DiffTokenWrapper,
    changed_token: &DiffTokenWrapper,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.nonce = thread_rng().gen();
    token.uid = uid;
    token.did = did;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.typ = MODIFY_TOKEN;
    edna_token.oldblob = bincode::serialize(old_token).unwrap();
    edna_token.newblob = bincode::serialize(changed_token).unwrap();

    token.token_data = edna_diff_token_to_bytes(&edna_token);
        token
}

pub fn new_token_remove(uid: UID, did: DID, changed_token: &DiffTokenWrapper) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.nonce = thread_rng().gen();
    token.uid = uid;
    token.did = did;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.typ = REMOVE_TOKEN;
    edna_token.oldblob = bincode::serialize(changed_token).unwrap();

    token.token_data = edna_diff_token_to_bytes(&edna_token);
    token
}*/

// create diff tokens about db objects
pub fn new_delete_token_wrapper(
    did: DID,
    table: String,
    tabids: Vec<RowVal>,
    old_value: Vec<RowVal>,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.nonce = thread_rng().gen();
    token.did = did;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.typ = REMOVE_GUISE;
    edna_token.table = table;
    edna_token.tabids = tabids.iter().map(|rv| rv.value().clone()).collect();
    edna_token.old_value = old_value;
    token.token_data = edna_diff_token_to_bytes(&edna_token);
       
    // XXX Remove
    let mut old_val_rvs = 0;
    for v in &edna_token.old_value {
        old_val_rvs += size_of_val(&*v);
    }
    error!("REMOVE DATA: nonce {}, did {}, table {}, tableid {}, tp {}, oldvalblob {}, all: {}", 
        size_of_val(&token.nonce),
        size_of_val(&token.did),
        size_of_val(&*edna_token.table),
        size_of_val(&*edna_token.tabids),
        size_of_val(&edna_token.typ),
        size_of_val(&*edna_token.old_value) + old_val_rvs,
        size_of_val(&token),
    );
    token
}

pub fn new_modify_token_wrapper(
    did: DID,
    table: String,
    tabids: Vec<RowVal>,
    old_value: String,
    new_value: String,
    col: String,
) -> DiffTokenWrapper {
    let mut token: DiffTokenWrapper = Default::default();
    token.nonce = thread_rng().gen();
    token.did = did;

    let mut edna_token: EdnaDiffToken = Default::default();
    edna_token.typ = MODIFY_GUISE;
    edna_token.table = table;
    edna_token.tabids = tabids.iter().map(|rv| rv.value().clone()).collect();
    edna_token.col = col;
    edna_token.old_val = old_value;
    edna_token.new_val = new_value;
    token.token_data = edna_diff_token_to_bytes(&edna_token);

    error!("MODIFY DATA: nonce {}, did {}, table {}, tableids {}, tp {}, col {}, old_col_val {}, newval {}, all: {}", 
        size_of_val(&token.nonce),
        size_of_val(&token.did),
        size_of_val(&*edna_token.table),
        size_of_val(&*edna_token.tabids),
        size_of_val(&edna_token.typ),
        size_of_val(&*edna_token.col),
        size_of_val(&*edna_token.old_val),
        size_of_val(&*edna_token.new_val),
        size_of_val(&token),
    );
    token
}

impl EdnaDiffToken {
    pub fn reveal<Q: Queryable>(
        &self,
        timap: &HashMap<String, spec::TableInfo>,
        dtw: &DiffTokenWrapper,
        token_ctrler: &mut TokenCtrler,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        match self.typ {
            // only ever called for a real principal
            REMOVE_PRINCIPAL => {
                let start = time::Instant::now();
                let pdata = PrincipalData {
                    pubkey: FromRsaPublicKey::from_pkcs1_der(&self.pubkey).unwrap(),
                    is_anon: false,
                    loc_caps: HashSet::new(),
                };
                warn!("Going to reveal principal {}", dtw.uid);
                token_ctrler.register_saved_principal::<Q>(
                    &dtw.uid,
                    false,
                    &pdata.pubkey,
                    HashSet::new(),
                    true,
                    db,
                );
                warn!("Reveal removed principal: {}", start.elapsed().as_micros());
            }

            REMOVE_GUISE => {
                let start = time::Instant::now();
                // get current guise in db
                let table_info = timap.get(&self.table).unwrap();
                let token_guise_selection = get_select_of_ids_str(&table_info, &self.tabids);
                let selected = get_query_rows_str_q::<Q>(
                    &str_select_statement(&self.table, &token_guise_selection.to_string()),
                    db,
                )?;

                // XXX data can be revealed even if it should've been disguised in the interim

                // item has been re-inserted, ignore
                if !selected.is_empty() {
                    // XXX true here because it's technically revealed?
                    return Ok(true);
                }

                // otherwise insert it
                let cols: Vec<String> = self.old_value.iter().map(|rv| rv.column().clone()).collect();
                let colstr = cols.join(",");
                let vals: Vec<String> = self
                    .old_value
                    .iter()
                    .map(|rv| {
                        if rv.value().is_empty() {
                            "\"\"".to_string()
                        } else if rv.value() == "NULL" {
                            "NULL".to_string()
                        } else {
                            for c in rv.value().chars() {
                                if !c.is_numeric() {
                                    return format!("\"{}\"", rv.value().clone());
                                }
                            }
                            rv.value().clone()
                        }
                    })
                    .collect();
                let valstr = vals.join(",");
                db.query_drop(format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.table, colstr, valstr
                ))?;
                warn!(
                    "Reveal removed data for {}: {}",
                    self.table,
                    start.elapsed().as_micros()
                );
            }
            MODIFY_GUISE => {
                // get current guise in db
                let table_info = timap.get(&self.table).unwrap();
                let token_guise_selection = get_select_of_ids_str(&table_info, &self.tabids);
                let selected = get_query_rows_str_q(
                    &str_select_statement(&self.table, &token_guise_selection.to_string()),
                    db,
                )?;

                // if field hasn't been modified, return it to original
                if selected.is_empty() {
                    warn!("DiffTokenWrapper Reveal: Modified value no longer exists\n",);
                }
                for rv in &selected[0] {
                    if rv.column() == self.col {
                        if rv.value() != self.new_val {
                            warn!(
                                "DiffTokenWrapper Reveal: Modified value {:?} not equal to new value {:?}\n",
                                rv.value(), self.new_val
                            );
                            return Ok(false);
                        }
                    }
                }

                // ok, we can actually update this!
                db.query_drop(
                    Statement::Update(UpdateStatement {
                        table_name: string_to_objname(&self.table),
                        assignments: vec![Assignment {
                            id: Ident::new(self.col.clone()),
                            value: Expr::Value(Value::String(self.old_val.clone())),
                        }],
                        selection: Some(token_guise_selection),
                    })
                    .to_string(),
                )?;
            }
            REMOVE_TOKEN => {
                // restore global token (may or may not have been revealed, but oh well!)
                /*let mut token: DiffTokenWrapper =
                    bincode::deserialize(&self.oldblob).unwrap();
                assert!(token.is_global);
                //token_ctrler.insert_global_diff_token_wrapper(&mut token);*/
            }
            MODIFY_TOKEN => {
                /*let new_token: DiffTokenWrapper =
                    bincode::deserialize(&self.newblob).unwrap();
                assert!(new_token.is_global);

                //let (revealed, eq) = token_ctrler.check_global_diff_token_for_match(&new_token);

                // don't reveal if token has been modified
                if !eq {
                    return Ok(false);
                }

                // actually update token
                let old_token: DiffTokenWrapper =
                    bincode::deserialize(&self.oldblob).unwrap();
                //token_ctrler.update_global_diff_token_from_old_to(&new_token, &old_token, None);

                // if token has been revealed, attempt to reveal old value of token
                if revealed {
                    let edna_old_token = edna_diff_token_from_bytes(&old_token.token_data);
                    return edna_old_token.reveal::<Q>(token_ctrler, db);
                }*/
            }
            _ => unimplemented!("Bad diff token update type?"), // do nothing for PRIV_KEY
        }
        Ok(true)
    }
}
