use crate::helpers::*;
use mysql::prelude::*;
use crate::tokens::*;
use crate::spec;
use crate::{RowVal, DID, UID};
use log::warn;
//use log::error;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sql_parser::ast::*;
use crate::*;
use  std::mem::size_of_val;

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct OwnershipTokenWrapper {
    pub old_uid: UID,
    pub new_uid: UID,
    pub did: DID,
    pub nonce: u64,
    pub token_data: Vec<u8>,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EdnaOwnershipToken {
    pub cname: String,
    pub cids: Vec<String>,
    pub fk_col: String,
}

pub fn edna_own_token_from_bytes(bytes: &Vec<u8>) -> Result<EdnaOwnershipToken, bincode::Error> {
    bincode::deserialize(bytes)
}

pub fn edna_own_token_to_bytes(token: &EdnaOwnershipToken) -> Vec<u8> {
    bincode::serialize(token).unwrap()
}

pub fn ownership_token_from_bytes(bytes: &Vec<u8>) -> OwnershipTokenWrapper {
    bincode::deserialize(bytes).unwrap()
}
pub fn ownership_tokens_from_bytes(bytes: &Vec<u8>) -> Vec<OwnershipTokenWrapper> {
    bincode::deserialize(bytes).unwrap()
}
pub fn new_generic_ownership_token_wrapper(
    old_uid: UID,
    new_uid: UID,
    did: DID,
    data: Vec<u8>,
) -> OwnershipTokenWrapper {
    let mut token: OwnershipTokenWrapper = Default::default();
    token.new_uid = new_uid;
    token.old_uid = old_uid;
    token.did = did;
    token.nonce = thread_rng().gen();
    token.token_data = data;

    error!("OTW DATA: nonce {}, old_uid {}, new_uid {}, did {}, all: {}", 
        size_of_val(&token.nonce),
        size_of_val(&token.did),
        size_of_val(&*token.new_uid),
        size_of_val(&*token.old_uid),
        size_of_val(&token),
    );
 
    token
}

pub fn new_edna_ownership_token(
    cname: String,
    cids: Vec<RowVal>,
    fk_col: String,
) -> EdnaOwnershipToken {
    let mut edna_token: EdnaOwnershipToken = Default::default();
    edna_token.cname = cname;
    edna_token.cids = cids.iter().map(|rv| rv.value().clone()).collect();
    edna_token.fk_col = fk_col;

    error!("EDNA OT: cn {}, cids {}, fkcol {}, total {}",
        size_of_val(&*edna_token.cname),
        size_of_val(&*edna_token.cids),
        size_of_val(&*edna_token.fk_col),
        size_of_val(&edna_token),
    );
    edna_token
}

impl EdnaOwnershipToken {
    pub fn reveal<Q:Queryable>(
        &self,
        timap: &HashMap<String, spec::TableInfo>,
        otw: &OwnershipTokenWrapper,
        guise_gen: &GuiseGen,
        token_ctrler: &mut TokenCtrler,
        db: &mut Q,
    ) -> Result<bool, mysql::Error> {
        // TODO we need to transfer the principal's tokens to the original principal's bag, and
        // reencrypt

        // if original entity does not exist, do not recorrelate
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(
                guise_gen.guise_id_col.clone(),
            )])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number(otw.old_uid.to_string()))),
        };
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&guise_gen.guise_name, &selection.to_string()),
            db,
        )?;
        if selected.is_empty() {
            warn!(
                "OwnToken Reveal: Original entity col {} = {}, original otw {:?} does not exist\n",
                guise_gen.guise_id_col, otw.old_uid, otw,
            );
            return Ok(false);
        }

        // if foreign key is rewritten, don't reverse anything
        let table_info = timap.get(&self.cname).unwrap();
        let token_guise_selection = get_select_of_ids_str(&table_info, &self.cids);
        let selected = get_query_rows_str_q::<Q>(
            &str_select_statement(&self.cname, &token_guise_selection.to_string()),
            db,
        )?;
        if selected.len() > 0 {
            assert_eq!(selected.len(), 1);
            let curval = get_value_of_col(&selected[0], &self.fk_col).unwrap();
            if curval != otw.new_uid {
                warn!(
                    "OwnToken Reveal: Foreign key col {} rewritten from {} to {}\n",
                    self.fk_col, otw.new_uid, curval
                );
                return Ok(false);
            }
        }

        // ok, we can actually update this to point to the original entity!
        // rewrite it to original and if pseudoprincipal is still present, remove it
        let updates = vec![Assignment {
            id: Ident::new(self.fk_col.clone()),
            value: Expr::Value(Value::Number(otw.old_uid.to_string())),
        }];
        db.query_drop(
            Statement::Update(UpdateStatement {
                table_name: string_to_objname(&self.cname),
                assignments: updates,
                selection: Some(token_guise_selection),
            })
            .to_string(),
        )?;

        // remove the pseudoprincipal
        db.query_drop(
            Statement::Delete(DeleteStatement {
                table_name: string_to_objname(&guise_gen.guise_name),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        guise_gen.guise_id_col.to_string(),
                    )])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(otw.new_uid.clone()))),
                }),
            })
            .to_string(),
        )?;
        // remove the principal from being registered by the token ctrler
        //XXX LYT keep principal around for now until all locators are gone
        token_ctrler.remove_principal::<Q>(&otw.new_uid, db);
        Ok(true)
    }
}
