use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{helpers};
use std::sync::atomic::Ordering;
use std::*;
use log::{warn};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap, HashSet};
use msql_srv::{QueryResultWriter};

pub const GHOST_ID_START : u64 = 1<<20;
pub const GHOST_ID_MAX: u64 = 1<<25;

// the ghosts table contains ALL ghost identifiers which map from any entity to its ghosts
// this assumes that all entities have an integer identifying key
const GHOST_ENTITY_COL : &'static str = "entity_id";
const GHOST_ID_COL: &'static str = "ghost_id";

pub fn create_ghosts_table(name: String, db: &mut mysql::Conn, in_memory: bool) -> Result<(), mysql::Error> {
    db.query_drop(&format!("DROP TABLE IF EXISTS {};", name))?;
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` int unsigned)", 
        name, GHOST_ID_COL, GHOST_ENTITY_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    db.query_drop(q)?;
    let q = format!(r"ALTER TABLE {} AUTO_INCREMENT={};",
        name, GHOST_ID_START);
    db.query_drop(q)?;
    warn!("drop/create/alter ghosts table {}", name);
    Ok(())
}

pub fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    gids: &Vec<(String, Option<u64>, u64)>) 
    -> Result<(), mysql::Error> 
{
    let cols : Vec<_> = vec![
        msql_srv::Column {
            table : "ghosts".to_string(),
            column : "entity_type".to_string(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: msql_srv::ColumnFlags::empty(),
        },
        msql_srv::Column {
            table : "ghosts".to_string(),
            column : GHOST_ENTITY_COL.to_string(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_LONGLONG,
            colflags: msql_srv::ColumnFlags::empty(),
        },
        msql_srv::Column {
            table : "ghosts".to_string(),
            column : GHOST_ID_COL.to_string(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_LONGLONG,
            colflags: msql_srv::ColumnFlags::empty(),
        },
    ];
    let mut writer = results.start(&cols)?;
    for (entity_type, eid, gid) in gids {
        writer.write_col(mysql_common::value::Value::Bytes(entity_type.as_bytes().to_vec()))?;
        if let Some(eid) = eid {
            writer.write_col(mysql_common::value::Value::UInt(*eid))?;
        }
        else {
            writer.write_col(mysql_common::value::Value::NULL)?;
        }
        writer.write_col(mysql_common::value::Value::UInt(*gid))?;
        writer.end_row()?;
    }
    writer.finish()?;
    Ok(())
}

pub struct GhostsMap{
    name: String,
    // only those entities that actually had gids are marked
    // as unsubscribed
    unsubscribed: HashSet<u64>,
    eid2gids: HashMap<u64, Vec<u64>>,
    gid2eid: HashMap<u64, u64>,
    latest_gid: AtomicU64,
    
    pub nqueries: u64,
}

impl GhostsMap {
    pub fn new(table_name: String, db: &mut mysql::Conn, in_memory: bool) -> Self {
        let name = format!("ghost{}", table_name);
        create_ghosts_table(name.clone(), db, in_memory).unwrap();
        GhostsMap{
            name: name.clone(),
            unsubscribed: HashSet::new(),
            eid2gids: HashMap::new(),
            gid2eid: HashMap::new(),
            latest_gid: AtomicU64::new(GHOST_ID_START),
            nqueries: 0,
        }
    }   
    
    /* 
     * Inserts the eid unsubscribing.
     * Returns a hash of the list of ghosts if user is not yet unsubscribed,
     * else None if the user is already unsubscribed
     */
    pub fn unsubscribe(&mut self, eid:u64, db: &mut mysql::Conn) -> Result<Option<Vec<u64>>, mysql::Error> {
        warn!("{} Unsubscribing {}", self.name, eid);
        let start = time::Instant::now();
        if self.unsubscribed.get(&eid).is_none() {
            //self.cache_eid2gids_for_eids(&vec![eid])?;
            if let Some(gids) = self.eid2gids.remove(&eid) {
                // remove gids from reverse mapping
                for gid in &gids {
                    self.gid2eid.remove(gid);
                }

                if !gids.is_empty() {
                    self.unsubscribed.insert(eid); 
                }

                // delete from ghosts table
                let delete_stmt = Statement::Delete(DeleteStatement{
                    table_name: helpers::string_to_objname(&self.name),
                    selection: Some(Expr::InList{
                        expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ID_COL))),
                        list: gids.iter().map(|g| Expr::Value(Value::Number(g.to_string()))).collect(),
                        negated: false,
                    }),
                });
                db.query_drop(format!("{}", delete_stmt))?;
                self.nqueries+=1;
                warn!("{} issue_update_dt_stmt: {}, dur {}us", self.name, delete_stmt, start.elapsed().as_micros());
           
                // return the gids
                return Ok(Some(gids));
            } else {
                return Ok(None);
            }
        } else {
            warn!("{}: {} already unsubscribed", self.name, eid);
        }
        Ok(None)
    }
 
    /* 
     * Removes the eid unsubscribing.
     * Returns true if was unsubscribed 
     */
    pub fn resubscribe(&mut self, eid: u64, gids: &Vec<u64>, db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
        // check hash and ensure that user has been unsubscribed
        // TODO could also use MAC to authenticate user
        warn!("{} Resubscribing {}", self.name, eid);
        let start = time::Instant::now();
        if !self.unsubscribed.remove(&eid) {
            return Ok(false);
        }

        let mut pairs = String::new();

        // insert mappings
        // no mappings should exist!
        if let Some(gids) = self.eid2gids.insert(eid, gids.clone()) {
            warn!("{} GIDS for {} are not empty???: {:?}", self.name, eid, gids);
            // XXX This can happen if we're still allow this "user" to insert stories/comments...
            assert!(gids.is_empty());
        }
        for i in 0..gids.len() {
            self.gid2eid.insert(gids[i], eid);
            
            // save values to insert into ghosts table
            pairs.push_str(&format!("({}, {})", gids[i], eid));
            if i < gids.len()-1 {
                pairs.push_str(", ");
            }
        }

        // insert into ghost table
        let insert_query = &format!("INSERT INTO {} ({}, {}) VALUES {};", 
                            self.name, GHOST_ID_COL, GHOST_ENTITY_COL, pairs);
        db.query_iter(insert_query)?;
        self.nqueries+=1;
        warn!("RESUB {} insert_gid_for_eid {}: {}, dur {}us", self.name, eid, insert_query, start.elapsed().as_micros());

        Ok(true)
    }

    pub fn insert_gid_into_caches(&mut self, eid:u64, gid:u64) {
        match self.eid2gids.get_mut(&eid) {
            Some(gids) => (*gids).push(gid),
            None => {
                self.eid2gids.insert(eid, vec![gid]);
            }
        }
        self.gid2eid.insert(gid, eid);
    }
 
    pub fn update_eid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let mut gids_to_delete = vec![];
        for (eid, gid) in pairs {
            // delete current mapping
            if let Some(oldeid) = self.gid2eid.get(gid) {
                if let Some(gids) = self.eid2gids.get_mut(&oldeid) {
                    gids.retain(|x| *x != *gid);
                }
                self.gid2eid.remove(gid);
            }

            // delete from datatable if eid is none (set to NULL)
            if eid.is_none() {
                gids_to_delete.push(Expr::Value(Value::Number(gid.to_string())));
            }

            // update if there is a new mapping
            else if let Some(neweid) = eid {
                self.insert_gid_into_caches(*neweid, *gid);
                
                // XXX what if the value IS a GID??? should we just remove this GID?
                let update_stmt = Statement::Update(UpdateStatement {
                    table_name: helpers::string_to_objname(&self.name),
                    assignments: vec![Assignment{
                        id: Ident::new(GHOST_ENTITY_COL),
                        value: Expr::Value(Value::Number(neweid.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(
                                      helpers::string_to_idents(GHOST_ID_COL))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(gid.to_string()))),
                    }),
                });
                warn!("{} issue_update_eid2gids_stmt: {}", self.name, update_stmt);
                db.query_drop(format!("{}", update_stmt))?;
                self.nqueries+=1;
            }
            self.latest_gid.fetch_max(*gid, Ordering::SeqCst);
        }
        if !gids_to_delete.is_empty() {
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.name),
                selection: Some(Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ID_COL))),
                    list: gids_to_delete,
                    negated: false,
                }),
            });
            warn!("{} update eid2gids_with : {}", self.name, delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;
        }
        Ok(())
    }

    pub fn get_gids_for_eid(&mut self, eid: u64) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        //self.cache_eid2gids_for_eids(&vec![eid])?;
        let gids = self.eid2gids.get(&eid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: eid not present in cache?")))?;
        Ok(gids.to_vec())
    }

    pub fn get_gids_for_eids(&mut self, eids: &Vec<u64>) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        //self.cache_eid2gids_for_eids(eids)?;
        let mut gid_vecs = vec![];
        for eid in eids {
            let gids = self.eid2gids.get(&eid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: eid not present in cache?")))?;
            gid_vecs.push((*eid, gids.to_vec()));
        }
        Ok(gid_vecs)
    }

    /* 
     * Add eid->gid mapping to cache if mapping not yet present
     * by querying the ghosts mapping table
     */
    /*pub fn cache_eid2gids_for_eids(&mut self, eids: &Vec<u64>) -> Result<(), mysql::Error>
    {
        let mut uncached_eids = vec![];
        for eid in eids {
            if self.eid2gids.get(&eid) == None {
                uncached_eids.push(eids[0])
            }
        }
        /*let selection : Expr;
        if eids.len() == 1 {
            selection = Expr::BinaryOp{
                left: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ENTITY_COL))),
                op: BinaryOperator::Eq, 
                right: Box::new(Expr::Value(Value::Number(eids[0].to_string()))),
            };
        } else {
            selection =  Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ENTITY_COL))),
                list: uncached_eids.iter().map(|u| Expr::Value(Value::Number(u.to_string()))).collect(),
                negated: false, 
            };
        }
        if uncached_eids.len() > 0 {
            unimplemented!("ghosts always should be in cache (as a MV)");
        }*/
        Ok(())
    }*/

    pub fn insert_gid_for_eid(&mut self, eid: u64, db: &mut mysql::Conn) -> Result<u64, mysql::Error> {
        // user ids are always ints
        let insert_query = &format!("INSERT INTO {} ({}) VALUES ({});", 
                            self.name, GHOST_ENTITY_COL, eid);
        let start = time::Instant::now();
        let res = db.query_iter(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        warn!("{} insert_gid_for_eid {}: {}us", self.name, eid, dur.as_millis());
        
        // we want to insert the GID in place of the eid
        let gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
      
        // insert into cache
        self.insert_gid_into_caches(eid, gid);

        Ok(gid)
    }
}
