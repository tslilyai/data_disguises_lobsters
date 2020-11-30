use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{helpers, views};
use std::sync::atomic::Ordering;
use std::*;
use log::{warn};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap};
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use msql_srv::{QueryResultWriter};

pub const GHOST_ID_START : u64 = 1<<20;
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

pub fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    gids: &Vec<u64>) 
    -> Result<(), mysql::Error> 
{
    let cols : Vec<_> = vec![msql_srv::Column {
        table : GHOST_TABLE_NAME.to_string(),
        column : GHOST_ID_COL.to_string(),
        coltype: msql_srv::ColumnType::MYSQL_TYPE_LONGLONG,
        colflags: msql_srv::ColumnFlags::empty(),
    }];
    let mut writer = results.start(&cols)?;
    for &gid in gids {
        writer.write_col(mysql_common::value::Value::UInt(gid))?;
        writer.end_row()?;
    }
    writer.finish()?;
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
    pub fn unsubscribe(&mut self, uid:u64, db: &mut mysql::Conn) -> Result<Option<Vec<u64>>, mysql::Error> {
        warn!("Unsubscribing {}", uid);
        if self.unsubscribed.get(&uid).is_none() {
            self.cache_uid2gids_for_uids(&vec![uid])?;
            if let Some(gids) = self.uid2gids.remove(&uid) {
                // remove gids from reverse mapping
                for gid in &gids {
                    self.gid2uid.remove(gid);
                }

                // cache the hash of the gids
                let serialized = serde_json::to_string(&gids).unwrap();
                self.hasher.input_str(&serialized);
                let result = self.hasher.result_str();
                self.hasher.reset();
                // TODO should we persist the hash?
                self.unsubscribed.insert(uid, result); 

                // delete from ghosts table
                let delete_stmt = Statement::Delete(DeleteStatement{
                    table_name: helpers::string_to_objname(&GHOST_TABLE_NAME),
                    selection: Some(Expr::InList{
                        expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ID_COL))),
                        list: gids.iter().map(|g| Expr::Value(Value::Number(g.to_string()))).collect(),
                        negated: false,
                    }),
                });
                warn!("issue_update_dt_stmt: {}", delete_stmt);
                db.query_drop(format!("{}", delete_stmt))?;
                self.nqueries+=1;
           
                // return the gids
                return Ok(Some(gids));
            } else {
                // no gids for this user
                self.unsubscribed.insert(uid, String::new()); 
                return Ok(Some(vec![]));
            }
        } else {
            warn!("{} already unsubscribed", uid);
        }
        Ok(None)
    }
 
    /* 
     * Removes the UID unsubscribing.
     * Returns true if was unsubscribed 
     */
    pub fn resubscribe(&mut self, uid:u64, gids: &Vec<u64>, db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
        // check hash and ensure that user has been unsubscribed
        // TODO could also use MAC to authenticate user
        warn!("Resubscribing {}", uid);
        match self.unsubscribed.get(&uid) {
            Some(gidshash) => {
                let serialized = serde_json::to_string(&gids).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *gidshash != hashed {
                    warn!("Resubscribing {} hash mismatch {}, {}", uid, gidshash, hashed);
                    return Ok(false);
                }
                self.hasher.reset();
                self.unsubscribed.remove(&uid); 
            }
            None => return Ok(false),
        }

        let mut pairs = String::new();

        // insert mappings
        // no mappings should exist!
        if let Some(gids) = self.uid2gids.insert(uid, gids.clone()) {
            warn!("GIDS for {} are not empty???: {:?}", uid, gids);
            // XXX This can happen if we're still allow this "user" to insert stories/comments...
            assert!(gids.is_empty());
        }
        for i in 0..gids.len() {
            self.gid2uid.insert(gids[i], uid);
            
            // save values to insert into ghosts table
            pairs.push_str(&format!("({}, {})", gids[i], uid));
            if i < gids.len()-1 {
                pairs.push_str(", ");
            }
        }

        // insert into ghost table
        let insert_query = &format!("INSERT INTO {} ({}, {}) VALUES {};", 
                            GHOST_TABLE_NAME, GHOST_ID_COL, GHOST_USER_COL, pairs);
        warn!("insert_gid_for_uid {}: {}", uid, insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;

        Ok(true)
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
                warn!("issue_update_uid2gids_stmt: {}", update_stmt);
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
        /*let selection : Expr;
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
        }*/
        if uncached_uids.len() > 0 {
            unimplemented!("ghosts always should be in cache (as a MV)");
        }
        Ok(())
    }

    fn insert_gid_for_uid(&mut self, uid: u64, db: &mut mysql::Conn) -> Result<u64, mysql::Error> {
        // user ids are always ints
        let insert_query = &format!("INSERT INTO {} ({}) VALUES ({});", 
                            GHOST_TABLE_NAME, GHOST_USER_COL, uid);
        let start = time::Instant::now();
        let res = db.query_iter(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        warn!("insert_gid_for_uid {}: {}us", uid, dur.as_millis());
        
        // we want to insert the GID in place of the UID
        let gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
      
        // insert into cache
        self.insert_gid_into_caches(uid, gid);

        Ok(gid)
    }
    
    pub fn insert_uid2gids_for_values(&mut self, values: &views::RowPtrs, ucol_indices: &Vec<usize>, db: &mut mysql::Conn) 
        -> Result<Vec<Vec<Expr>>, mysql::Error>
    {
        let start = time::Instant::now();
        let mut gid_rows = vec![];
        if !ucol_indices.is_empty() {
            for row in 0..values.len() {
                let mut gid_vals = vec![];
                let valrow = values[row].borrow();
                for col in 0..valrow.len() {
                    let mut found = false;
                    // add entry to ghosts table
                    if ucol_indices.contains(&col) {
                        // NULL check: don't add ghosts entry if new UID value is NULL
                        if valrow[col] != Value::Null {
                            let uid = helpers::parser_val_to_u64(&valrow[col]);
                            let gid = self.insert_gid_for_uid(uid, db)?;
                            gid_vals.push(Expr::Value(Value::Number(gid.to_string())));
                            found = true;
                        }
                    } 
                    if !found {
                        gid_vals.push(Expr::Value(valrow[col].clone()));
                    }
                }
                gid_rows.push(gid_vals);
            }
        }
        let dur = start.elapsed();
        warn!("insert_uid2gids_for_values: {}us", dur.as_micros());
        Ok(gid_rows)
    }
}
