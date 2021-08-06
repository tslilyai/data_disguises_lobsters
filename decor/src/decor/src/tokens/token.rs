use crate::helpers::*;
use crate::{DID, UID};
use rsa::{pkcs1::ToRsaPrivateKey, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::hash::{Hash, Hasher};

pub const INSERT_GUISE: u64 = 0;
pub const REMOVE_GUISE: u64 = 1;
pub const DECOR_GUISE: u64 = 2;
pub const UPDATE_GUISE: u64 = 3;
pub const PRIV_KEY: u64 = 4;

pub static TOKEN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub enum TokenType {
    PrivKey,
    Data,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Token {
    // metadata set by Edna
    pub token_id: u64,
    pub disguise_id: DID,
    pub user_id: UID,
    pub update_type: u64,
    pub revealed: bool,
    pub is_global: bool,

    // guise information
    pub guise_name: String,
    pub guise_ids: Vec<RowVal>,

    // DECOR/UPDATE/DELETE: store old blobs
    pub old_value: Vec<RowVal>,

    // DECOR
    pub referenced_name: String,

    // INSERT
    pub referencer_name: String,

    // DECOR/UPDATE/INSERT: store new blobs
    pub new_value: Vec<RowVal>,

    // PRIV_KEY
    pub priv_key: Vec<u8>,
    pub new_user_id: UID,

    // for randomness
    pub nonce: u64,

    // for linked-list
    pub last_tail: u64,
}

impl Hash for Token {
    fn hash<H:Hasher>(&self, state:&mut H) {
        self.token_id.hash(state);
    }
}

impl Token {
    pub fn new_privkey_token(did: DID, uid: UID, new_uid: UID, priv_key: &RsaPrivateKey) -> Token {
        let mut token: Token = Default::default();
        token.user_id = uid;
        token.disguise_id = did;
        token.update_type = PRIV_KEY;
        token.revealed = false;
        token.priv_key = priv_key.to_pkcs1_der().unwrap().as_der().to_vec();
        token.new_user_id = new_uid;
        token
    }

    pub fn new_decor_token(
        did: DID,
        uid: UID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        referenced_name: String,
        old_value: Vec<RowVal>,
        new_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.user_id = uid;
        token.disguise_id = did;
        token.update_type = DECOR_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.referenced_name = referenced_name;
        token.old_value = old_value;
        token.new_value = new_value;
        token
    }

    pub fn new_delete_token(
        did: DID,
        uid: UID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        old_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.user_id = uid;
        token.disguise_id = did;
        token.update_type = REMOVE_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.old_value = old_value;
        token
    }

    pub fn new_update_token(
        did: DID,
        uid: UID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        old_value: Vec<RowVal>,
        new_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.user_id = uid;
        token.disguise_id = did;
        token.update_type = UPDATE_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.old_value = old_value;
        token.new_value = new_value;
        token
    }

    pub fn new_insert_token(
        did: DID,
        uid: UID,
        guise_name: String,
        guise_ids: Vec<RowVal>,
        referencer_name: String,
        new_value: Vec<RowVal>,
    ) -> Token {
        let mut token: Token = Default::default();
        token.user_id = uid;
        token.disguise_id = did;
        token.update_type = INSERT_GUISE;
        token.revealed = false;
        token.guise_name = guise_name;
        token.guise_ids = guise_ids;
        token.referencer_name = referencer_name;
        token.new_value = new_value;
        token
    }

    pub fn token_to_bytes(token: &Token) -> Vec<u8> {
        let s = serde_json::to_string(token).unwrap();
        s.as_bytes().to_vec()
    }

    pub fn token_from_bytes(bytes: Vec<u8>) -> Token {
        serde_json::from_slice(&bytes).unwrap()
    }

    /*fn reinsert_guise(
        &self,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<(), mysql::Error> {
        let mut cols = vec![];
        let mut vals = vec![];
        for rv in &self.old_value {
            cols.push(Ident::new(rv.column.clone()));
            // XXX treating everything like a string might backfire
            vals.push(Expr::Value(Value::String(rv.value.clone())));
        }
        query_drop(
            Statement::Insert(InsertStatement {
                table_name: string_to_objname(&self.guise_name),
                columns: cols,
                source: InsertSource::Query(Box::new(values_query(vec![vals]))),
            })
            .to_string(),
            conn,
            stats,
        )
    }

    fn recorrelate_guise(
        &self,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<(), mysql::Error> {
        warn!(
            "Recorrelating guise of table {} to {}, user {}",
            self.guise_name, self.fk_name, self.user_id
        );

        // this may be none if this token entry is an insert, and not a modification
        let owner_col = &self.modified_cols[0];
        let new_val: String;
        let old_val: String;
        match get_value_of_col(&self.new_value, owner_col) {
            Some(n) => new_val = n,
            None => unimplemented!("Bad col name?"),
        }
        match get_value_of_col(&self.old_value, owner_col) {
            Some(n) => old_val = n,
            None => unimplemented!("Bad col name?"),
        }
        let updates = vec![Assignment {
            id: Ident::new(owner_col),
            value: Expr::Value(Value::Number(old_val)),
        }];
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(owner_col)])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number(new_val.clone()))),
        };
        query_drop(
            Statement::Update(UpdateStatement {
                table_name: string_to_objname(&self.guise_name),
                assignments: updates,
                selection: Some(selection),
            })
            .to_string(),
            conn,
            stats.clone(),
        )?;
        //insert_reversed_token_entry(&ve, conn, stats.clone());

        /*
         * Delete created guises if objects in this table had been decorrelated
         * TODO can make per-guise-table, rather than assume that only users are guises
         */
        // delete guise
        query_drop(
            Statement::Delete(DeleteStatement {
                table_name: string_to_objname(&self.fk_name),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(self.fk_col.to_string())])),
                    op: BinaryOperator::Eq,
                    // XXX assuming guise is a user... only has one id
                    right: Box::new(Expr::Value(Value::Number(new_val.clone()))),
                }),
            })
            .to_string(),
            conn,
            stats.clone(),
        )?;
        // mark token entries as reversed
        //insert_reversed_token_entry(&ve, conn, stats.clone());
        Ok(())
    }

    pub fn restore_ownership(
        &self,
        conn: &mut mysql::PooledConn,
        stats: Arc<Mutex<QueryStat>>,
    ) -> Result<(), mysql::Error> {
        match self.update_type {
            DECOR_GUISE => self.recorrelate_guise(conn, stats.clone())?,
            DELETE_GUISE => self.reinsert_guise(conn, stats.clone())?,
            _ => unimplemented!("Bad update type"),
        }
        Ok(())
    }

    // if this token entry modifies or removes something that this disguise predicate
    // depends on, then we have a RAW conflict
    pub fn conflicts_with(&self, disguise: &Disguise) -> bool {
        // a disguise can only conflict with prior disguises of lower priority
        if self.priority >= disguise.priority {
            return false;
        }
        if self.modified_cols.is_empty() {
            return false;
        }
        if self.update_type == DELETE_GUISE || self.update_type == DECOR_GUISE {
            for (_, td) in disguise.table_disguises.clone() {
                let td_locked = td.read().unwrap();
                // if this table disguise isn't of the conflicting table, ignore
                if self.guise_name != td_locked.name {
                    continue;
                }
                for t in &td_locked.transforms {
                    for col in &self.modified_cols {
                        if t.pred.contains(col) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }*/
}

/*pub fn reverse_decor_ve(
    referencer_table: &str,
    referencer_col: &str,
    fktable: &str,
    fkcol: &str,
    conn: &mut mysql::PooledConn,
    stats: Arc<Mutex<QueryStat>>,
) -> Result<Vec<Token>, mysql::Error> {
    // TODO assuming that all FKs point to users

    /*
     * Undo modifications to objects of this table
     * TODO undo any token modifications that were dependent on this one, namely "filters" that
     * join with this "filter" (any updates that happened after this?)
     */
}*/
