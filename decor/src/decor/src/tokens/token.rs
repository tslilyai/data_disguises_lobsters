use crate::helpers::*;
use crate::stats::QueryStat;
use crate::tokens::*;
use crate::{DID, UID};
use rsa::{pkcs1::ToRsaPrivateKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub const REMOVE_GUISE: u64 = 1;
pub const DECOR_GUISE: u64 = 2;
pub const MODIFY_GUISE: u64 = 3;
pub const REMOVE_TOKEN: u64 = 5;
pub const MODIFY_TOKEN: u64 = 6;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrivKeyToken {
    pub did: DID,
    pub uid: UID,
    pub new_uid: UID,
    pub priv_key: Vec<u8>,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Token {
    // metadata set by Edna
    pub token_id: u64,
    pub did: DID,
    pub uid: UID,
    pub update_type: u64,
    pub revealed: bool,
    pub is_global: bool,

    // guise information
    pub guise_name: String,
    pub guise_ids: Vec<RowVal>,

    // DECOR/MODIFY/DELETE: store old blobs
    pub old_value: Vec<RowVal>,

    // DECOR
    pub referenced_name: String,
    // XXX assuming guise only has one id
    // could just use tableinfo
    pub referenced_id_col: String,

    // DECOR/MODIFY: store new blobs
    pub new_value: Vec<RowVal>,

    // TOKEN REMOVE/MODIFY
    pub old_token_blob: String,
    pub new_token_blob: String,

    // FOR SECURITY DESIGN
    // for randomness
    pub nonce: u64,
    // for linked-list
}

impl Hash for Token {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token_id.hash(state);
    }
}

pub fn privkeytoken_from_bytes(bytes: Vec<u8>) -> PrivKeyToken {
    serde_json::from_slice(&bytes).unwrap()
}

pub fn token_from_bytes(bytes: Vec<u8>) -> Token {
    serde_json::from_slice(&bytes).unwrap()
}

impl Token {
    pub fn new_token_modify(uid: UID, did:DID, old_token: &Token, changed_token: &Token) -> Token {
        let mut token: Token = Default::default();
        token.is_global = false;
        token.uid = uid;
        token.did = did;
        token.update_type = MODIFY_TOKEN;
        token.revealed = false;
        token.old_token_blob = serde_json::to_string(old_token).unwrap();
        token.new_token_blob = serde_json::to_string(changed_token).unwrap();
        token
    }

    pub fn new_token_remove(uid: UID, did: DID, changed_token: &Token) -> Token {
        let mut token: Token = Default::default();
        token.is_global = false;
        token.uid = uid;
        token.did = did;
        token.update_type = REMOVE_TOKEN;
        token.revealed = false;
        token.old_token_blob = serde_json::to_string(changed_token).unwrap();
        token
    }

    pub fn new_privkey_token(uid: UID, did:DID, new_uid: UID, priv_key: &RsaPrivateKey) -> PrivKeyToken {
        let mut token: PrivKeyToken = Default::default();
        token.uid = uid;
        token.did = did;
        token.priv_key = priv_key.to_pkcs1_der().unwrap().as_der().to_vec();
        token.new_uid = new_uid;
        token
    }

    pub fn new_decor_token(
        did: DID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        referenced_name: String,
        referenced_id_col: String,
        old_value: Vec<RowVal>,
        new_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.did = did;
        token.update_type = DECOR_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.referenced_name = referenced_name;
        token.referenced_id_col = referenced_id_col;
        token.old_value = old_value;
        token.new_value = new_value;
        token
    }

    pub fn new_delete_token(
        did: DID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        old_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.did = did;
        token.update_type = REMOVE_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.old_value = old_value;
        token
    }

    pub fn new_modify_token(
        did: DID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        old_value: Vec<RowVal>,
        new_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.did = did;
        token.update_type = MODIFY_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.old_value = old_value;
        token.new_value = new_value;
        token
    }

    pub fn token_to_bytes(token: &Token) -> Vec<u8> {
        let s = serde_json::to_string(token).unwrap();
        s.as_bytes().to_vec()
    }

    pub fn reveal(
        &self,
        token_ctrler: &mut TokenCtrler,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<bool, mysql::Error> {
        if !self.revealed {
            // get current guise in db
            let token_guise_selection = get_select_of_ids(&self.guise_ids);
            let selected = get_query_rows_str(
                &str_select_statement(&self.guise_name, &token_guise_selection.to_string()),
                conn,
                stats.clone(),
            )?;

            match self.update_type {
                REMOVE_GUISE => {
                    // XXX problematic case: data can be revealed even if it should've been
                    // disguised?

                    // item has been re-inserted, ignore
                    if !selected.is_empty() {
                        // XXX true here because it's technically revealed?
                        return Ok(true);
                    }

                    // otherwise insert it
                    let cols: Vec<String> =
                        self.old_value.iter().map(|rv| rv.column.clone()).collect();
                    let colstr = cols.join(",");
                    let vals: Vec<String> =
                        self.old_value.iter().map(|rv| rv.value.clone()).collect();
                    let valstr = vals.join(",");
                    query_drop(
                        format!(
                            "INSERT INTO {} COLUMNS ({}) VALUES ({})",
                            self.guise_name, colstr, valstr
                        ),
                        conn,
                        stats.clone(),
                    )?;
                }
                DECOR_GUISE => {
                    // rewrite it to original
                    // and if pseudoprincipal is still present, remove it
                    let mut new_val = "".to_string();
                    let mut old_val = "".to_string();
                    let mut owner_col = String::new();
                    for (i, newrv) in self.new_value.iter().enumerate() {
                        new_val = newrv.value.clone();
                        old_val = self.old_value[i].value.clone();
                        if new_val != old_val {
                            owner_col = newrv.column.clone();
                            break;
                        }
                    }
                    assert!(!owner_col.is_empty());

                    // if foreign key is rewritten, don't reverse anything
                    if selected.len() > 0 {
                        assert_eq!(selected.len(), 1);
                        let curval = get_value_of_col(&selected[0], &owner_col).unwrap();
                        if curval != new_val {
                            return Ok(false);
                        }
                    }
                    // if original entity does not exist, do not recorrelate
                    let selection = Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(
                            self.referenced_id_col.clone(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(old_val.clone()))),
                    };
                    let selected = get_query_rows_str(
                        &str_select_statement(&self.referenced_name, &selection.to_string()),
                        conn,
                        stats.clone(),
                    )?;
                    if selected.is_empty() {
                        return Ok(false);
                    }

                    // ok, we can actually update this to point to the original entity!
                    let updates = vec![Assignment {
                        id: Ident::new(owner_col.clone()),
                        value: Expr::Value(Value::Number(old_val)),
                    }];
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

                    // remove the pseudoprincipal
                    query_drop(
                        Statement::Delete(DeleteStatement {
                            table_name: string_to_objname(&self.referenced_name),
                            selection: Some(Expr::BinaryOp {
                                left: Box::new(Expr::Identifier(vec![Ident::new(
                                    self.referenced_id_col.to_string(),
                                )])),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(Value::Number(new_val.clone()))),
                            }),
                        })
                        .to_string(),
                        conn,
                        stats.clone(),
                    )?;
                    // remove the principal from being registered by the token ctrler
                    token_ctrler.remove_anon_principal(u64::from_str(&new_val).unwrap());
                }
                MODIFY_GUISE => {
                    // if field hasn't been modified, return it to original
                    if selected.is_empty() || selected[0] != self.new_value {
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
                    let mut token: Token = serde_json::from_str(&self.old_token_blob).unwrap();
                    assert!(token.is_global);
                    token_ctrler.insert_global_token(&mut token);
                }
                MODIFY_TOKEN => {
                    let new_token: Token = serde_json::from_str(&self.new_token_blob).unwrap();
                    assert!(new_token.is_global);

                    let (revealed, eq) = token_ctrler.check_global_token_for_match(&new_token);

                    // don't reveal if token has been modified
                    if !eq {
                        return Ok(false);
                    }

                    // actually update token
                    let old_token: Token = serde_json::from_str(&self.old_token_blob).unwrap();
                    token_ctrler.update_global_token_from_old_to(&new_token, &old_token, None);

                    // if token has been revealed, attempt to reveal old value of token
                    if revealed {
                        return old_token.reveal(token_ctrler, conn, stats.clone());
                    }
                }
                _ => (), // do nothing for PRIV_KEY
            }
        }
        Ok(true)
    }
}
