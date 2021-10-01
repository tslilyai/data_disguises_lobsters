use crate::diffs::*;
use crate::helpers::*;
use crate::stats::QueryStat;
use crate::{DID, UID};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncDiff {
    pub enc_key: Vec<u8>,
    pub enc_diff: EncData,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Diff {
    // metadata set by Edna
    pub diff_id: u64,
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
    pub old_diff_blob: String,
    pub new_diff_blob: String,

    // FOR SECURITY DESIGN
    // for randomness
    pub nonce: u64,
    // for linked-list
}

impl Hash for Diff {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.diff_id.hash(state);
    }
}

pub fn diff_from_bytes(bytes: &Vec<u8>) -> Diff {
    serde_json::from_slice(bytes).unwrap()
}

impl Diff {
    pub fn new_diff_modify(uid: UID, did: DID, old_diff: &Diff, changed_diff: &Diff) -> Diff {
        let mut diff: Diff = Default::default();
        diff.is_global = false;
        diff.uid = uid;
        diff.did = did;
        diff.update_type = MODIFY_TOKEN;
        diff.revealed = false;
        diff.old_diff_blob = serde_json::to_string(old_diff).unwrap();
        diff.new_diff_blob = serde_json::to_string(changed_diff).unwrap();
        diff
    }

    pub fn new_diff_remove(uid: UID, did: DID, changed_diff: &Diff) -> Diff {
        let mut diff: Diff = Default::default();
        diff.is_global = false;
        diff.uid = uid;
        diff.did = did;
        diff.update_type = REMOVE_TOKEN;
        diff.revealed = false;
        diff.old_diff_blob = serde_json::to_string(changed_diff).unwrap();
        diff
    }

    pub fn new_decor_diff(
        did: DID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        referenced_name: String,
        referenced_id_col: String,
        old_value: Vec<RowVal>,
        new_value: Vec<RowVal>,
    ) -> Diff {
        let mut diff: Diff = Default::default();
        diff.did = did;
        diff.update_type = DECOR_GUISE;
        diff.revealed = false;
        diff.guise_name = guise_name;
        diff.guise_ids = guise_ids;
        diff.referenced_name = referenced_name;
        diff.referenced_id_col = referenced_id_col;
        diff.old_value = old_value;
        diff.new_value = new_value;
        diff
    }

    pub fn new_delete_diff(
        did: DID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        old_value: Vec<RowVal>,
    ) -> Diff {
        let mut diff: Diff = Default::default();
        diff.did = did;
        diff.update_type = REMOVE_GUISE;
        diff.revealed = false;
        diff.guise_name = guise_name;
        diff.guise_ids = guise_ids;
        diff.old_value = old_value;
        diff
    }

    pub fn new_modify_diff(
        did: DID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        old_value: Vec<RowVal>,
        new_value: Vec<RowVal>,
    ) -> Diff {
        let mut diff: Diff = Default::default();
        diff.did = did;
        diff.update_type = MODIFY_GUISE;
        diff.revealed = false;
        diff.guise_name = guise_name;
        diff.guise_ids = guise_ids;
        diff.old_value = old_value;
        diff.new_value = new_value;
        diff
    }

    pub fn diff_to_bytes(diff: &Diff) -> Vec<u8> {
        let s = serde_json::to_string(diff).unwrap();
        s.as_bytes().to_vec()
    }

    pub fn reveal(
        &self,
        diff_ctrler: &mut DiffCtrler,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<bool, mysql::Error> {
        if !self.revealed {
            // get current guise in db
            let diff_guise_selection = get_select_of_ids(&self.guise_ids);
            let selected = get_query_rows_str(
                &str_select_statement(&self.guise_name, &diff_guise_selection.to_string()),
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
                        self.old_value.iter().map(|rv| 
                            if rv.value.is_empty() {
                                "\"\"".to_string()
                            } else {
                                for c in rv.value.chars() {
                                    if !c.is_numeric() {
                                        return format!("\"{}\"", rv.value.clone())
                                    }
                                }
                                rv.value.clone()
                            }).collect();
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
                            selection: Some(diff_guise_selection),
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
                    // remove the principal from being registered by the diff ctrler
                    diff_ctrler.remove_anon_principal(u64::from_str(&new_val).unwrap());
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
                            selection: Some(diff_guise_selection),
                        })
                        .to_string(),
                        conn,
                        stats.clone(),
                    )?;
                }
                REMOVE_TOKEN => {
                    // restore global diff (may or may not have been revealed, but oh well!)
                    let mut diff: Diff = serde_json::from_str(&self.old_diff_blob).unwrap();
                    assert!(diff.is_global);
                    diff_ctrler.insert_global_diff(&mut diff);
                }
                MODIFY_TOKEN => {
                    let new_diff: Diff = serde_json::from_str(&self.new_diff_blob).unwrap();
                    assert!(new_diff.is_global);

                    let (revealed, eq) = diff_ctrler.check_global_diff_for_match(&new_diff);

                    // don't reveal if diff has been modified
                    if !eq {
                        return Ok(false);
                    }

                    // actually update diff
                    let old_diff: Diff = serde_json::from_str(&self.old_diff_blob).unwrap();
                    diff_ctrler.update_global_diff_from_old_to(&new_diff, &old_diff, None);

                    // if diff has been revealed, attempt to reveal old value of diff
                    if revealed {
                        return old_diff.reveal(diff_ctrler, conn, stats.clone());
                    }
                }
                _ => (), // do nothing for PRIV_KEY
            }
        }
        Ok(true)
    }
}
