use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{helpers, policy, policy::EntityGhostPolicies, views::{Views, RowPtr, HashedRowPtrs, RowPtrs}, ID_COL};
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
    hash1: String,
    hash2: String)
    -> Result<(), mysql::Error> 
{
    let cols : Vec<_> = vec![
        msql_srv::Column {
            table : "".to_string(),
            column : "ghosts".to_string(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: msql_srv::ColumnFlags::empty(),
        },
        msql_srv::Column {
            table : "".to_string(),
            column : "entities".to_string(),
            coltype: msql_srv::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: msql_srv::ColumnFlags::empty(),
        },
    ];
    warn!("GM Serialized values are {}, {}", hash1, hash2);
    let mut writer = results.start(&cols)?;
    writer.write_col(mysql_common::value::Value::Bytes(hash1.as_bytes().to_vec()))?;
    writer.write_col(mysql_common::value::Value::Bytes(hash2.as_bytes().to_vec()))?;
    writer.end_row()?;
    writer.finish()?;
    Ok(())
}

pub struct GhostsMap{
    table_name: String,
    name: String,
    // only those entities that actually had gids are marked
    // as unsubscribed
    unsubscribed: HashSet<u64>,
    eid2gids: HashMap<u64, Vec<(u64, RowPtr)>>,
    gid2eid: HashMap<u64, (u64, RowPtr)>,
    latest_gid: AtomicU64,
    
    pub nqueries: usize,
}


impl GhostsMap {
    pub fn new(table_name: String, db: &mut mysql::Conn, in_memory: bool) -> Self {
        let name = format!("ghost{}", table_name);
        create_ghosts_table(name.clone(), db, in_memory).unwrap();
        GhostsMap{
            table_name: table_name.clone(),
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
    pub fn unsubscribe(&mut self, eid:u64, db: &mut mysql::Conn) -> Result<Option<(Vec<u64>, RowPtrs)>, mysql::Error> {
        warn!("{} Unsubscribing {}", self.name, eid);
        let start = time::Instant::now();
        if self.unsubscribed.get(&eid).is_none() {
            //self.cache_eid2gids_for_eids(&vec![eid])?;
            if let Some(gidrptrs) = self.eid2gids.remove(&eid) {
                let mut rowptrs = vec![];
                let mut gids = vec![];
                
                // remove gids from reverse mapping
                for (gid, vals) in &gidrptrs {
                    self.gid2eid.remove(gid);
                    rowptrs.push(vals.clone());
                    gids.push(*gid);
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
                warn!("{} Found {} gids for {}", self.name, gids.len(), eid);
                return Ok(Some((gids, rowptrs)));
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
    pub fn resubscribe(&mut self, eid: u64, index: usize, gidrptrs: &RowPtrs, db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
        // check hash and ensure that user has been unsubscribed
        // TODO could also use MAC to authenticate user
        warn!("{} Resubscribing {}", self.name, eid);
        let start = time::Instant::now();
        if !self.unsubscribed.remove(&eid) {
            return Ok(false);
        }
        let mut ghosts_of_eid = vec![];
        let mut pairs = vec![];
        for grptr in gidrptrs {
            let gid = helpers::parser_val_to_u64(&grptr.borrow()[index]);
            self.gid2eid.insert(gid, (eid, grptr.clone()));

            // insert into backward map
            ghosts_of_eid.push((gid, grptr.clone()));

            // save values to insert into ghosts table
            pairs.push(format!("({}, {})", gid, eid));
        }
        // insert into foward map
        assert!(self.eid2gids.insert(eid, ghosts_of_eid).is_none());

        // insert into ghost table
        let insert_query = &format!("INSERT INTO {} ({}, {}) VALUES {};", self.name, GHOST_ID_COL, GHOST_ENTITY_COL, pairs.join(","));
        warn!("Inserting into ghosts table {}", insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;
        warn!("RESUB {} insert_gid_for_eid {}: {}, dur {}us", self.name, eid, insert_query, start.elapsed().as_micros());
        Ok(true)
    }

    pub fn insert_gid_into_caches(&mut self, eid:u64, gid:u64, vals: RowPtr) {
        match self.eid2gids.get_mut(&eid) {
            Some(gids) => (*gids).push((gid,vals.clone())),
            None => {
                self.eid2gids.insert(eid, vec![(gid, vals.clone())]);
            }
        }
        self.gid2eid.insert(gid, (eid, vals));
    }
 
    pub fn update_eid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let mut gids_to_delete = vec![];
        for (eid, gid) in pairs {
            // delete current mapping
            let mut vals: Option<RowPtr> = None;
            if let Some((oldeid, oldvals)) = self.gid2eid.get(gid) {
                vals = Some(oldvals.clone());
                if let Some(gids) = self.eid2gids.get_mut(&oldeid) {
                    gids.retain(|(x, _)| *x != *gid);
                }
                self.gid2eid.remove(gid);
            }

            // delete from datatable if eid is none (set to NULL)
            if eid.is_none() {
                gids_to_delete.push(Expr::Value(Value::Number(gid.to_string())));
            }

            // update if there is a new mapping
            else if let Some(neweid) = eid {
                self.insert_gid_into_caches(*neweid, *gid, vals.unwrap());
                
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

                // note that the ghost entry in the parent datatable can stay constant; it's just
                // pointed to by a different child entity now
            }
            self.latest_gid.fetch_max(*gid, Ordering::SeqCst);
        }
        if !gids_to_delete.is_empty() {
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.name),
                selection: Some(Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ID_COL))),
                    list: gids_to_delete.clone(),
                    negated: false,
                }),
            });
            warn!("{} update eid2gids_with : {}", self.name, delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;

            // REMOVE From parent table too
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.table_name),
                selection: Some(Expr::InList {
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&ID_COL))),
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
        Ok(gids.iter().map(|(g,_)| *g).collect())
    }

    pub fn get_gids_for_eids(&mut self, eids: &Vec<u64>) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        //self.cache_eid2gids_for_eids(eids)?;
        let mut gid_vecs = vec![];
        for eid in eids {
            let gids = self.eid2gids.get(&eid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: eid not present in cache?")))?;
            let gids = gids.iter().map(|(g,_)| *g).collect();
            gid_vecs.push((*eid, gids));
        }
        Ok(gid_vecs)
    }

    pub fn insert_gid_for_eid(&mut self, 
                                views: &Views,
                                gp: &EntityGhostPolicies,
                                from_vals: RowPtr,
                                eid: u64, db: &mut mysql::Conn) 
        -> Result<u64, mysql::Error> 
    {
        // user ids are always ints
        let insert_query = &format!("INSERT INTO {} ({}) VALUES ({});", self.name, GHOST_ENTITY_COL, eid);
        let start = time::Instant::now();
        let res = db.query_iter(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        warn!("{} insert_gid_for_eid {}: {}us", self.name, eid, dur.as_millis());
        
        // we want to insert the GID in place of the eid
        let gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
        drop(res);
      
        // actually generate parent ghost entities for this table
        let mut generated_eid_gids = vec![];
        let new_entities = policy::generate_new_entities_from(
            views, gp, db, &mut generated_eid_gids, &self.table_name, from_vals, &vec![Value::Number(gid.to_string())], None, &mut self.nqueries)?;
        let new_rows = &new_entities[0].2;
        assert!(new_rows.len() == 1);
        // insert into cache
        self.insert_gid_into_caches(eid, gid, new_rows[0].clone());

        Ok(gid)
    }

    /*pub fn insert_gids_for_eids(&mut self, 
                                views: &Views,
                                gp: &GhostPolicy,
                                from_vals: RowPtr,
                                eids: &Vec<u64>, 
                                db: &mut mysql::Conn) 
        -> Result<Vec<u64>, mysql::Error> 
    {
        // user ids are always ints
        let eid_strs = vec![];
        for eid in eids{
            eid_strs.push(format!("({})", eid.to_string()));
        }
        let insert_query = &format!("INSERT INTO {} ({}) VALUES {};", 
                            self.name, GHOST_ENTITY_COL, eid_strs.join(','));
        let start = time::Instant::now();
        let res = db.query_iter(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        warn!("{} insert_gid_for_eids {:?}: {}us", self.name, eids, dur.as_millis());
        
        // we want to insert the GID in place of the eid
        let last_gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
        let gids : Vec<u64> = (last_gid +1 - eids.len())..(last_gid+1).collect();

        // actually generate parent ghost entities for this table
        let mut generated_eid_gids = vec![];
        let new_entities = policy::generate_new_entities_from(
            views, gp, db, &mut generated_eid_gids, &self.table_name, from_vals, 
            eids.iter().map(|e| Value::Number(e.to_string())).collect(), 
            None, &mut self.nqueries)?;
        
        // insert new ghost entities into cache
        for i in 0..new_entities.len() {
            self.insert_gid_into_caches(eids[i], gids[i], new_entities[i].2);
        }
        Ok(gids)
    }*/

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
}

pub struct GhostMaps{
    ghost_maps: HashMap<String, GhostsMap> // table name to ghost map
}

impl GhostMaps{
    pub fn new() -> Self {
        GhostMaps{
            ghost_maps: HashMap::new()
        }
    }

    pub fn insert(&mut self, name: String, db: &mut mysql::Conn, in_mem: bool) {
        self.ghost_maps.insert(name.to_string(), GhostsMap::new(name.to_string(), db, in_mem));
    }

    pub fn insert_gid_for_eid(&mut self, 
                     views: &Views,
                     gp: &EntityGhostPolicies,
                     from_vals: RowPtr,
                    eid: u64, db: &mut mysql::Conn, parent_str: &str) -> Result<u64, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(parent_str).unwrap();
        gm.insert_gid_for_eid(views, gp, from_vals, eid, db)
    }

    /*pub fn insert_gids_for_eids(&mut self, eids: &Vec<u64>, db: &mut mysql::Conn, parent_str: &str) -> Result<Vec<u64>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_str);
        gm.insert_gids_for_eids(eids, db)
    }*/

    pub fn get_nqueries(&mut self) -> usize {
        let mut n = 0;
        for (_, gm) in self.ghost_maps.iter_mut() {
            n += gm.nqueries;
            gm.nqueries = 0;
        }
        n
    }

    pub fn get_gids_for_eid(&mut self, eid: u64, parent_table: &str) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.get_gids_for_eid(eid)
    }

    pub fn get_gids_for_eids(&mut self, eids: &Vec<u64>, parent_table: &str) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.get_gids_for_eids(eids)
    }

 
    pub fn update_eid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn, parent_table: &str)
        -> Result<(), mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.update_eid2gids_with(pairs, db)
    }

    pub fn unsubscribe(&mut self, eid:u64, db: &mut mysql::Conn, parent_table: &str) -> Result<Option<(Vec<u64>, RowPtrs)>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.unsubscribe(eid, db)
    }
    pub fn resubscribe(&mut self, eid: u64, index: usize, gidrptrs: &RowPtrs, db: &mut mysql::Conn, parent_table: &str) -> Result<bool, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.resubscribe(eid, index, gidrptrs, db)
    }
}
