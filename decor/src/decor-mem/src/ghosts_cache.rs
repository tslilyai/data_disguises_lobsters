use mysql::prelude::*;
use sql_parser::ast::*;
use super::helpers;
use std::sync::atomic::Ordering;
use std::*;
use log::{warn};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap, HashSet};

pub struct GhostsCache{
    // caches
    unsubscribed: HashSet<u64>,
    uid2gids: HashMap<u64, Vec<u64>>,
    gid2uid: HashMap<u64, u64>,
    latest_gid: AtomicU64,
    
    pub nqueries: u64,
}

impl GhostsCache{
    pub fn new() -> Self {
        GhostsCache{
            unsubscribed: HashSet::new(),
            uid2gids: HashMap::new(),
            gid2uid: HashMap::new(),
            latest_gid: AtomicU64::new(super::GHOST_ID_START),
            nqueries: 0,
        }
    }   
     
    pub fn unsubscribe(&mut self, uid:u64) -> bool {
        self.unsubscribed.insert(uid)
    }

    pub fn resubscribe(&mut self, uid:u64) -> bool {
        self.unsubscribed.remove(&uid)
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
 
    pub fn update_uid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>)
        -> Result<(), mysql::Error> 
    {
        for (uid, gid) in pairs {
            // delete current mapping
            if let Some(olduid) = self.gid2uid.get(gid) {
                if let Some(gids) = self.uid2gids.get_mut(&olduid) {
                    gids.retain(|x| *x != *gid);
                }
                self.gid2uid.remove(gid);
            }

            // update if there is a new mapping
            if let Some(newuid) = uid {
                self.insert_gid_into_caches(*newuid, *gid);
            }
            self.latest_gid.fetch_max(*gid, Ordering::SeqCst);
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
                left: Box::new(Expr::Identifier(helpers::string_to_idents(&super::GHOST_USER_COL))),
                op: BinaryOperator::Eq, 
                right: Box::new(Expr::Value(Value::Number(uids[0].to_string()))),
            };
        } else {
            selection =  Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(&super::GHOST_USER_COL))),
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
                            super::GHOST_TABLE_NAME, super::GHOST_USER_COL, uid);
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
    
    pub fn insert_uid2gids_for_values(&mut self, values: &mut Vec<Vec<Expr>>, ucol_indices: &Vec<usize>, db: &mut mysql::Conn) 
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
                    if values[row][col] != Expr::Value(Value::Null) {
                        let uid = helpers::parser_expr_to_u64(&values[row][col])?;
                        let gid = self.insert_gid_for_uid(uid, db)?;
                        values[row][col] = Expr::Value(Value::Number(gid.to_string()));
                    }
                }
            }
        }
        Ok(())
    }
}
