use mysql::prelude::*;
use sql_parser::ast::*;
use super::helpers;
use std::sync::atomic::Ordering;
use std::*;
use log::{warn};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap};
use crypto::digest::Digest;
use crypto::sha3::Sha3;

const GHOST_ID_START : u64 = 1<<20;
const GHOST_TABLE_NAME : &'static str = "ghosts";
const GHOST_USER_COL : &'static str = "user_id";
const GHOST_ID_COL: &'static str = "ghost_id";

fn set_initial_gid_query() -> String {
    format!(
        r"ALTER TABLE {} AUTO_INCREMENT={};",
        GHOST_TABLE_NAME, GHOST_ID_START)
}

fn create_ghosts_query(in_memory: bool) -> String {
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` int unsigned)", 
        GHOST_TABLE_NAME, GHOST_ID_COL, GHOST_USER_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    q
}

pub fn create_ghosts_table(db: &mut mysql::Conn, in_memory: bool) -> Result<(), mysql::Error> {
    db.query_drop("DROP TABLE IF EXISTS ghosts;")?;
    db.query_drop(create_ghosts_query(in_memory))?;
    db.query_drop(set_initial_gid_query())?;
    warn!("drop/create/alter ghosts table");
    Ok(())
}

pub struct GhostsMap{
    // caches
    unsubscribed: HashMap<u64,String>,
    uid2gids: HashMap<u64, Vec<u64>>,
    gid2uid: HashMap<u64, u64>,
    latest_gid: AtomicU64,
    hasher : Sha3,
    
    pub nqueries: u64,
}

impl GhostsMap{
    pub fn new() -> Self {
        GhostsMap{
            unsubscribed: HashMap::new(),
            hasher : Sha3::sha3_256(),
            uid2gids: HashMap::new(),
            gid2uid: HashMap::new(),
            latest_gid: AtomicU64::new(GHOST_ID_START),
            nqueries: 0,
        }
    }   
    
    /* 
     * Inserts the UID unsubscribing.
     * Returns a hash of the list of ghosts if user is not yet unsubscribed,
     * else None if the user is already unsubscribed
     */
    pub fn unsubscribe(&mut self, uid:u64) -> Result<Option<Vec<u64>>, mysql::Error> {
        if self.unsubscribed.get(&uid).is_none() {
            self.cache_uid2gids_for_uids(&vec![uid])?;
            if let Some(gids) = self.uid2gids.remove(&uid) {
                // cache the hash of the gids
                let serialized = serde_json::to_string(&gids).unwrap();
                self.hasher.input_str(&serialized);
                let result = self.hasher.result_str();
                self.hasher.reset();
                self.unsubscribed.insert(uid, result); 
           
                // return the gids
                return Ok(Some(gids));
            } else {
                // no gids for this user
                self.unsubscribed.insert(uid, String::new()); 
                return Ok(Some(vec![]));
            }
        }
        Ok(None)
    }
 
    /* 
     * Removes the UID unsubscribing.
     * Returns true if was unsubscribed 
     */
    pub fn resubscribe(&mut self, uid:u64, gids: Vec<u64>, db: &mut mysql::Conn) -> bool {
        false
        //self.unsubscribed.remove(&uid)
    }

    pub fn insert_gid_into_caches(&mut self, uid:u64, gid:u64) {
        match self.uid2gids.get_mut(&uid) {
            Some(gids) => (*gids).push(gid),
            None => {
                self.uid2gids.insert(uid, vec![gid]);
            }
        }
        self.gid2uid.insert(gid, uid);
    }
 
    pub fn update_uid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let mut gids_to_delete = vec![];
        for (uid, gid) in pairs {
            // delete current mapping
            if let Some(olduid) = self.gid2uid.get(gid) {
                if let Some(gids) = self.uid2gids.get_mut(&olduid) {
                    gids.retain(|x| *x != *gid);
                }
                self.gid2uid.remove(gid);
            }

            // delete from datatable if uid is none (set to NULL)
            if uid.is_none() {
                gids_to_delete.push(Expr::Value(Value::Number(gid.to_string())));
            }

            // update if there is a new mapping
            else if let Some(newuid) = uid {
                self.insert_gid_into_caches(*newuid, *gid);
                
                // XXX what if the value IS a GID??? should we just remove this GID?
                let update_stmt = Statement::Update(UpdateStatement {
                    table_name: helpers::string_to_objname(GHOST_TABLE_NAME),
                    assignments: vec![Assignment{
                        id: Ident::new(GHOST_USER_COL),
                        value: Expr::Value(Value::Number(newuid.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(
                                      helpers::string_to_idents(GHOST_ID_COL))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(gid.to_string()))),
                    }),
                });
                warn!("issue_update_dt_stmt: {}", update_stmt);
                db.query_drop(format!("{}", update_stmt))?;
                self.nqueries+=1;
            }
            self.latest_gid.fetch_max(*gid, Ordering::SeqCst);
        }
        if !gids_to_delete.is_empty() {
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&GHOST_TABLE_NAME),
                selection: Some(Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ID_COL))),
                    list: gids_to_delete,
                    negated: false,
                }),
            });
            warn!("issue_update_dt_stmt: {}", delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;
        }
        Ok(())
    }

    pub fn get_gids_for_uid(&mut self, uid: u64) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        self.cache_uid2gids_for_uids(&vec![uid])?;
        let gids = self.uid2gids.get(&uid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: uid not present in cache?")))?;
        Ok(gids.to_vec())
    }

    pub fn get_gids_for_uids(&mut self, uids: &Vec<u64>) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        self.cache_uid2gids_for_uids(uids)?;
        let mut gid_vecs = vec![];
        for uid in uids {
            let gids = self.uid2gids.get(&uid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: uid not present in cache?")))?;
            gid_vecs.push((*uid, gids.to_vec()));
        }
        Ok(gid_vecs)
    }

    /* 
     * Add uid->gid mapping to cache if mapping not yet present
     * by querying the ghosts mapping table
     */
    pub fn cache_uid2gids_for_uids(&mut self, uids: &Vec<u64>) -> Result<(), mysql::Error>
    {
        let mut uncached_uids = vec![];
        for uid in uids {
            if self.uid2gids.get(&uid) == None {
                uncached_uids.push(uids[0])
            }
        }
        let selection : Expr;
        if uids.len() == 1 {
            selection = Expr::BinaryOp{
                left: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_USER_COL))),
                op: BinaryOperator::Eq, 
                right: Box::new(Expr::Value(Value::Number(uids[0].to_string()))),
            };
        } else {
            selection =  Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_USER_COL))),
                list: uncached_uids.iter().map(|u| Expr::Value(Value::Number(u.to_string()))).collect(),
                negated: false, 
            };
        }
        if uncached_uids.len() > 0 {
            unimplemented!("ghosts always should be in cache (as a MV)");
        }
        Ok(())
    }

    fn insert_gid_for_uid(&mut self, uid: u64, db: &mut mysql::Conn) -> Result<u64, mysql::Error> {
        // user ids are always ints
        let insert_query = &format!("INSERT INTO {} ({}) VALUES ({});", 
                            GHOST_TABLE_NAME, GHOST_USER_COL, uid);
        warn!("insert_gid_for_uid: {}", insert_query);
        let res = db.query_iter(insert_query)?;
        self.nqueries+=1;
        
        // we want to insert the GID in place of the UID
        let gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
      
        // insert into cache
        self.insert_gid_into_caches(uid, gid);

        Ok(gid)
    }
    
    pub fn insert_uid2gids_for_values(&mut self, values: &mut Vec<Vec<Value>>, ucol_indices: &Vec<usize>, db: &mut mysql::Conn) 
        -> Result<(), mysql::Error>
    {
        if ucol_indices.is_empty() {
            return Ok(());
        }         
        for row in 0..values.len() {
            for col in 0..values[row].len() {
                // add entry to ghosts table
                if ucol_indices.contains(&col) {
                    // NULL check: don't add ghosts entry if new UID value is NULL
                    if values[row][col] != Value::Null {
                        let uid = helpers::parser_val_to_u64(&values[row][col]);
                        let gid = self.insert_gid_for_uid(uid, db)?;
                        values[row][col] = Value::Number(gid.to_string());
                    }
                }
            }
        }
        Ok(())
    }
}
