use crate::helpers::*;
use mysql::prelude::*;
use crate::tokens::*;
use crate::{RowVal, DID, UID};
use log::warn;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct OwnershipTokenWrapper {
    pub token_id: u64,
    pub revealed: bool,
    pub old_uid: UID,
    pub new_uid: UID,
    pub did: DID,
    pub nonce: u64,
    pub token_data: Vec<u8>,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EdnaOwnershipToken {
    pub did: DID,
    pub uid: UID,
    pub new_uid: UID,

    pub child_name: String,
    pub child_ids: Vec<RowVal>,
    pub pprincipal_name: String,
    pub pprincipal_id_col: String,
    pub fk_col: String,
}

pub fn edna_own_token_from_bytes(bytes: &Vec<u8>) -> Result<EdnaOwnershipToken, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn edna_own_token_to_bytes(token: &EdnaOwnershipToken) -> Vec<u8> {
    let s = serde_json::to_string(token).unwrap();
    s.as_bytes().to_vec()
}

pub fn ownership_token_from_bytes(bytes: &Vec<u8>) -> OwnershipTokenWrapper {
    serde_json::from_slice(bytes).unwrap()
}
pub fn ownership_tokens_from_bytes(bytes: &Vec<u8>) -> Vec<OwnershipTokenWrapper> {
    serde_json::from_slice(bytes).unwrap()
}
pub fn new_generic_ownership_token_wrapper(
    old_uid: UID,
    new_uid: UID,
    did: DID,
    data: Vec<u8>,
) -> OwnershipTokenWrapper {
    let mut token: OwnershipTokenWrapper = Default::default();
    token.token_id = thread_rng().gen();
    token.revealed = false;
    token.new_uid = new_uid;
    token.old_uid = old_uid;
    token.did = did;
    token.nonce = thread_rng().gen();
    token.token_data = data;
    token
}

pub fn new_edna_ownership_token(
    did: DID,
    child_name: String,
    child_ids: Vec<RowVal>,
    pprincipal_name: String,
    pprincipal_id_col: String,
    fk_col: String,
    cur_uid: UID,
    new_uid: UID,
) -> EdnaOwnershipToken {
    let mut edna_token: EdnaOwnershipToken = Default::default();
    edna_token.uid = cur_uid;
    edna_token.did = did;
    edna_token.new_uid = new_uid;
    edna_token.child_name = child_name;
    edna_token.child_ids = child_ids;
    edna_token.fk_col = fk_col;
    edna_token.pprincipal_name = pprincipal_name;
    edna_token.pprincipal_id_col = pprincipal_id_col;
    edna_token
}

impl EdnaOwnershipToken {
    pub fn reveal<Q:Queryable>(
        &self,
        token_ctrler: &mut TokenCtrler,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        // TODO we need to transfer the principal's tokens to the original principal's bag, and
        // reencrypt

        // if original entity does not exist, do not recorrelate
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(
                self.pprincipal_id_col.clone(),
            )])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number(self.uid.to_string()))),
        };
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&self.pprincipal_name, &selection.to_string()),
            db,
        )?;
        if selected.is_empty() {
            warn!(
                "OwnToken Reveal: Original entity col {} = {} does not exist\n",
                self.pprincipal_id_col, self.uid
            );
            return Ok(false);
        }

        // if foreign key is rewritten, don't reverse anything
        let token_guise_selection = get_select_of_ids(&self.child_ids);
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&self.child_name, &token_guise_selection.to_string()),
            db,
        )?;
        if selected.len() > 0 {
            assert_eq!(selected.len(), 1);
            let curval = get_value_of_col(&selected[0], &self.fk_col).unwrap();
            if curval != self.new_uid {
                warn!(
                    "OwnToken Reveal: Foreign key col {} rewritten from {} to {}\n",
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
        db.query_drop(
            Statement::Update(UpdateStatement {
                table_name: string_to_objname(&self.child_name),
                assignments: updates,
                selection: Some(token_guise_selection),
            })
            .to_string(),
        )?;

        // remove the pseudoprincipal
        db.query_drop(
            Statement::Delete(DeleteStatement {
                table_name: string_to_objname(&self.pprincipal_name),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        self.pprincipal_id_col.to_string(),
                    )])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(self.new_uid.clone()))),
                }),
            })
            .to_string(),
        )?;
        // remove the principal from being registered by the token ctrler
        //XXX LYT keep principal around for now until all locators are gone
        token_ctrler.remove_principal::<Q>(&self.new_uid, db);
        Ok(true)
    }
}
