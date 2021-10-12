use crate::helpers::*;
use crate::stats::QueryStat;
use crate::tokens::*;
use crate::{DID, UID};
use log::warn;
use rsa::{pkcs1::ToRsaPrivateKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct OwnershipToken {
    pub token_id: u64,
    pub did: DID,
    pub uid: UID,
    pub new_uid: UID,
    pub priv_key: Vec<u8>,

    pub guise_name: String,
    pub guise_ids: Vec<RowVal>,
    pub referenced_name: String,
    pub referenced_id_col: String,
    pub fk_col: String,

    pub new_value: Vec<RowVal>,
    pub old_value: Vec<RowVal>,

    pub revealed: bool,
}

pub fn ownership_token_from_bytes(bytes: &Vec<u8>) -> OwnershipToken {
    serde_json::from_slice(bytes).unwrap()
}

pub fn new_ownership_token(
    did: DID,
    guise_name: String,
    guise_ids: Vec<RowVal>,
    referenced_name: String,
    referenced_id_col: String,
    fk_col: String,
    cur_uid: UID,
    new_uid: UID,
    priv_key: &RsaPrivateKey,
    old_value: Vec<RowVal>,
    new_value: Vec<RowVal>,
) -> OwnershipToken {
    let mut token: OwnershipToken = Default::default();
    token.uid = cur_uid;
    token.did = did;
    token.priv_key = priv_key.to_pkcs1_der().unwrap().as_der().to_vec();
    token.new_uid = new_uid;
    token.revealed = false;
    token.guise_name = guise_name;
    token.guise_ids = guise_ids;
    token.fk_col = fk_col;
    token.referenced_name = referenced_name;
    token.referenced_id_col = referenced_id_col;
    token.old_value = old_value;
    token.new_value = new_value;
    token
}

impl OwnershipToken {
    pub fn reveal(
        &self,
        token_ctrler: &mut TokenCtrler,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<bool, mysql::Error> {
        // if original entity does not exist, do not recorrelate
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(
                self.referenced_id_col.clone(),
            )])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number(self.uid.to_string()))),
        };
        let selected = get_query_rows_str(
            &str_select_statement(&self.referenced_name, &selection.to_string()),
            conn,
            stats.clone(),
        )?;
        if selected.is_empty() {
            warn!(
                "DiffToken Reveal: Original entity col {} id {} does not exist\n",
                self.referenced_id_col, self.uid
            );
            return Ok(false);
        }

        // if foreign key is rewritten, don't reverse anything
        let token_guise_selection = get_select_of_ids(&self.guise_ids);
        let selected = get_query_rows_str(
            &str_select_statement(&self.guise_name, &token_guise_selection.to_string()),
            conn,
            stats.clone(),
        )?;
        if selected.len() > 0 {
            assert_eq!(selected.len(), 1);
            let curval = get_value_of_col(&selected[0], &self.fk_col).unwrap();
            if curval != self.new_uid.to_string() {
                warn!(
                    "DiffToken Reveal: Foreign key col {} rewritten from {} to {}\n",
                    self.fk_col, self.new_uid, curval
                );
                return Ok(false);
            }
        }

        // ok, we can actually update this to point to the original entity!
        // rewrite it to original and if pseudoprincipal is still present, remove it
        let updates = vec![Assignment {
            id: Ident::new(self.fk_col.clone()),
            value: Expr::Value(Value::Number(self.uid.to_string())),
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
                    right: Box::new(Expr::Value(Value::Number(self.new_uid.to_string()))),
                }),
            })
            .to_string(),
            conn,
            stats.clone(),
        )?;
        // remove the principal from being registered by the token ctrler
        token_ctrler.remove_anon_principal(self.new_uid);
        Ok(true)
    }
}
