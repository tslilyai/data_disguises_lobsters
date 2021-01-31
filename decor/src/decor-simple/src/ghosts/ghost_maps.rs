use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{ghosts, helpers, policy::ObjectGhostPolicies, views::{Views, RowPtr}, ID_COL};
use crate::ghosts::{GhostOidMapping, GhostFamily, TemplateObject};
use std::sync::atomic::Ordering;
use std::*;
use log::{warn};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap};

// the ghosts table contains ALL ghost identifiers which map from any object to its ghosts
// this assumes that all entities have an integer identifying key
const GHOST_OID_COL : &'static str = "object_id";
const GHOST_ID_COL: &'static str = "ghost_id";
const GHOST_DATA_COL: &'static str = "ghost_data";

fn create_ghosts_table(name: String, db: &mut mysql::Conn, in_memory: bool) -> Result<(), mysql::Error> {
    db.query_drop(&format!("DROP TABLE IF EXISTS {};", name))?;
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` int unsigned, 
            `{}` varchar(4096), INDEX oid (`{}`))", 
        name, GHOST_ID_COL, GHOST_OID_COL, GHOST_DATA_COL, GHOST_OID_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    warn!("drop/create/alter ghosts table {}: {}", name, q);
    db.query_drop(q)?;
    let q = format!(r"ALTER TABLE {} AUTO_INCREMENT={};",
        name, ghosts::GHOST_ID_START);
    db.query_drop(q)?;
    Ok(())
}

/*
 * INVARIANTS:
 *  - ghost map contains only mappings between real object oids and their ghost counterpart GIDs;
 *      these mappings only exist for edges that are *preemptively* created because the edge type
 *      can be decorrelated.
 *  - these GIDs always correspond to actual ghosts in the datatables, which always exist
 *  - these ghost sibling entities MAY have parents who are also ghosts which are not present in
 *      the mapping to ensure referential integrity; these rptrs are contained in the mapping
 */
pub struct GhostMap{
    table_name: String,
    name: String,
    oid2gids: HashMap<u64, Vec<GhostFamily>>,
    gid2oid: HashMap<u64, u64>,
    latest_gid: AtomicU64,
    
    pub nqueries: usize,
}


impl GhostMap {
    pub fn new(table_name: String, db: Option<&mut mysql::Conn>, in_memory: bool) -> Self {
        let name = format!("ghost{}", table_name);
        if let Some(db) = db {
            create_ghosts_table(name.clone(), db, in_memory).unwrap();
        }
        GhostMap{
            table_name: table_name.clone(),
            name: name.clone(),
            oid2gids: HashMap::new(),
            gid2oid: HashMap::new(),
            latest_gid: AtomicU64::new(ghosts::GHOST_ID_START),
            nqueries: 0,
        }
    }   
    
    /* 
     * Inserts the oid unsubscribing.
     * Returns a hash of the list of ghosts if user is not yet unsubscribed,
     * else None if the user is already unsubscribed
     */
    fn unsubscribe(&mut self, oid:u64, db: &mut mysql::Conn) -> Result<Option<Vec<GhostFamily>>, mysql::Error> {
        warn!("{} Unsubscribing {}", self.name, oid);
        let start = time::Instant::now();
        if let Some(ghost_families) = self.oid2gids.remove(&oid) {
            let mut gids = vec![];
            let mut families : Vec<GhostFamily> = vec![];
            
            // remove gids from reverse mapping
            for family in ghost_families {
                self.gid2oid.remove(&family.root_gid);
                families.push(family.clone());
                gids.push(family.root_gid);
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
            warn!("{} Found {} gids for {}", self.name, gids.len(), oid);
            return Ok(Some(families));
        } 
        return Ok(None);
    }  
    /* 
     * Removes the oid unsubscribing.
     * Returns true if was unsubscribed 
     */
    pub fn resubscribe(&mut self, 
                       oid: u64, 
                       ghost_families: &Vec<GhostFamily>,
                       db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
        warn!("{} Resubscribing {}", self.name, oid);
        let start = time::Instant::now();
       
        self.regenerate_cache_entry(oid, ghost_families);

        let mut pairs = vec![];
        for family in ghost_families {
            pairs.push(family.ghost_family_to_db_string(oid));
        }
        // insert into ghost table
        let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    self.name, GHOST_ID_COL, GHOST_OID_COL, GHOST_DATA_COL, pairs.join(","));
        warn!("Inserting into ghosts table {}", insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;
        warn!("RESUB {} insert_gid_for_oid {}: {}, dur {}us", self.name, oid, insert_query, start.elapsed().as_micros());
        Ok(true)
    }

    fn regenerate_cache_entry(&mut self, oid: u64, ghost_families: &Vec<GhostFamily>) {
        let mut families_of_oid = vec![];
        for family in ghost_families {
            self.gid2oid.insert(family.root_gid, oid);
            // save to insert into forward map
            families_of_oid.push(family.clone());
            self.latest_gid.fetch_max(family.root_gid, Ordering::SeqCst);
        }

        if let Some(families) = self.oid2gids.get_mut(&oid) {
            families.append(&mut families_of_oid);
        } else {
            self.oid2gids.insert(oid, families_of_oid);
        }
    }

    pub fn insert_gid_into_caches(&mut self, oid:u64, gfam: GhostFamily) {
        match self.oid2gids.get_mut(&oid) {
            Some(gfams) => (*gfams).push(gfam.clone()),
            None => {
                self.oid2gids.insert(oid, vec![gfam.clone()]);
            }
        }
        self.gid2oid.insert(gfam.root_gid, oid);
    }
 
    pub fn update_oid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let mut gids_to_delete = vec![];
        for (oid, gid) in pairs {
            // delete current mapping
            let mut vals: Option<GhostFamily> = None;
            if let Some(oldoid) = self.gid2oid.get(gid) {
                if let Some(gfams) = self.oid2gids.get_mut(&oldoid) {
                    let pos = gfams.iter().position(|gfam| gfam.root_gid == *gid).unwrap();
                    vals = Some(gfams.remove(pos));
                }
                self.gid2oid.remove(&gid);
            }

            // delete from datatable if oid is none (set to NULL)
            if oid.is_none() {
                gids_to_delete.push(Expr::Value(Value::Number(gid.to_string())));
            }

            // update if there is a new mapping
            else if let Some(newoid) = oid {
                self.insert_gid_into_caches(*newoid, vals.unwrap());
                
                // XXX what if the value IS a GID??? should we just remove this GID?
                let update_stmt = Statement::Update(UpdateStatement {
                    table_name: helpers::string_to_objname(&self.name),
                    assignments: vec![Assignment{
                        id: Ident::new(GHOST_OID_COL),
                        value: Expr::Value(Value::Number(newoid.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(
                                      helpers::string_to_idents(GHOST_ID_COL))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(gid.to_string()))),
                    }),
                });
                warn!("{} issue_update_oid2gids_stmt: {}", self.name, update_stmt);
                db.query_drop(format!("{}", update_stmt))?;
                self.nqueries+=1;

                // note that the ghost entry in the parent datatable can stay constant; it's just
                // pointed to by a different child object now
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
            warn!("{} update oid2gids_with : {}", self.name, delete_stmt);
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
            warn!("{} update oid2gids_with : {}", self.name, delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;
        }
        Ok(())
    }

    pub fn take_one_ghost_family_for_oid(&mut self, oid: u64, db: &mut mysql::Conn) -> 
        Result<Option<GhostFamily>, mysql::Error> 
    {
        let ghost_fams = self.oid2gids.get_mut(&oid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: oid not present in cache?")))?;

        if let Some(fam) = ghost_fams.pop() {
            // delete mapping
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.name),
                selection: Some(Expr::BinaryOp{
                    left: Box::new(Expr::Identifier(helpers::string_to_idents(&GHOST_ID_COL))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(fam.root_gid.to_string()))),
                }),
            });
            warn!("{} delete from oid2gids_with : {}", self.name, delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;
            Ok(Some(fam))
        } else {
            Ok(None)
        }
    }

    pub fn get_gids_for_oid(&mut self, oid: u64) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        //self.cache_oid2gids_for_oids(&vec![oid])?;
        let gids = self.oid2gids.get(&oid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: oid not present in cache?")))?;
        Ok(gids.iter().map(|fam| fam.root_gid).collect())
    }

    pub fn get_gids_for_oids(&mut self, oids: &Vec<u64>) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        //self.cache_oid2gids_for_oids(oids)?;
        let mut gid_vecs = vec![];
        for oid in oids {
            let gids = self.oid2gids.get(&oid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: oid not present in cache?")))?;
            let gids = gids.iter().map(|fam| fam.root_gid).collect();
            gid_vecs.push((*oid, gids));
        }
        Ok(gid_vecs)
    }

    pub fn insert_ghost_for_oid(&mut self, 
                                views: &Views,
                                gp: &ObjectGhostPolicies,
                                from_vals: RowPtr,
                                oid: u64, 
                                db: &mut mysql::Conn) 
        -> Result<u64, mysql::Error> 
    {
        let start = time::Instant::now();
       
        let gid = self.latest_gid.fetch_add(1, Ordering::SeqCst) + 1;

        let new_entities = ghosts::generate_new_ghosts_from(
            views, gp, 
            &TemplateObject{
                table: self.table_name.clone(), 
                oid: oid,
                row: from_vals, 
                fixed_colvals: None,
            }, 1)?;
            //&vec![Value::Number(gid.to_string())], 
            //&mut self.nqueries)?;
        // TODO will need to insert into datatables

        let new_family = GhostFamily{
            root_table: self.table_name.clone(),
            root_gid: gid,
            family_members: new_entities,
        };
        let dbentry = new_family.ghost_family_to_db_string(oid);
        
        // insert into in-memory cache
        self.insert_gid_into_caches(oid, new_family);

        // insert into DB
        let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    self.name, GHOST_ID_COL, GHOST_OID_COL, GHOST_DATA_COL, dbentry);
        warn!("Inserting into ghosts table {}", insert_query);
        db.query_drop(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        
        warn!("{} insert_ghost_for_oid {}, {}: {}us", self.name, gid, oid, dur.as_millis());
        Ok(gid)
    }
}

pub struct GhostMaps{
    ghost_maps: HashMap<String, GhostMap> // table name to ghost map
}

impl GhostMaps {
    pub fn new() -> Self {
        GhostMaps{
            ghost_maps: HashMap::new()
        }
    }

    pub fn new_ghost_map(&mut self, name: String, db: &mut mysql::Conn, in_mem: bool) {
        self.ghost_maps.insert(name.to_string(), GhostMap::new(name.to_string(), Some(db), in_mem));
    }

    pub fn new_ghost_map_cache_only(&mut self, name: String) {
        self.ghost_maps.insert(name.to_string(), GhostMap::new(name.to_string(), None, true));
    }

    pub fn insert_ghost_for_oid(&mut self, 
                     views: &Views,
                     gp: &ObjectGhostPolicies,
                     from_vals: RowPtr,
                    oid: u64, db: &mut mysql::Conn, table_name: &str) -> Result<u64, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(table_name).unwrap();
        gm.insert_ghost_for_oid(views, gp, from_vals, oid, db)
    }

    pub fn get_nqueries(&mut self) -> usize {
        let mut n = 0;
        for (_, gm) in self.ghost_maps.iter_mut() {
            n += gm.nqueries;
            gm.nqueries = 0;
        }
        n
    }

    pub fn get_gids_for_oid(&mut self, oid: u64, parent_table: &str) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.get_gids_for_oid(oid)
    }

    pub fn get_gids_for_oids(&mut self, oids: &Vec<u64>, parent_table: &str) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.get_gids_for_oids(oids)
    }

 
    pub fn update_oid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn, parent_table: &str)
        -> Result<(), mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.update_oid2gids_with(pairs, db)
    }

    pub fn unsubscribe(&mut self, oid:u64, db: &mut mysql::Conn, parent_table: &str) -> Result<Option<Vec<GhostFamily>>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.unsubscribe(oid, db)
    }
    pub fn resubscribe(&mut self, oid: u64, families: &Vec<GhostFamily>, db: &mut mysql::Conn, parent_table: &str) -> Result<bool, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.resubscribe(oid, families, db)
    }

    pub fn take_one_ghost_family_for_oid(&mut self, oid: u64, db: &mut mysql::Conn, parent_table: &str) -> 
        Result<Option<GhostFamily>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.take_one_ghost_family_for_oid(oid, db)
    }

    pub fn get_ghost_oid_mappings(&mut self, db: &mut mysql::Conn, table: &str) -> 
        Result<Vec<GhostOidMapping>, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(table).unwrap();
        let mut mappings = vec![]; 

        let res = db.query_iter(format!("SELECT * FROM {}", gm.name))?;
        for row in res {
            let vals = row.unwrap().unwrap();
            assert!(vals.len()==3);
           
            // TODO this is wrong
            let root_gids = vec![helpers::mysql_val_to_u64(&vals[0])?];
            let oid = helpers::mysql_val_to_u64(&vals[1])?;
            let ghostdata = helpers::mysql_val_to_string(&vals[2]);
            let ghostdata = ghostdata.trim_end_matches('\'').trim_start_matches('\'');
            let family_ghost_names = serde_json::from_str(&ghostdata).unwrap();
            let mapping = GhostOidMapping {
                table: table.to_string(),
                oid: oid,
                root_gids: root_gids,
                ghosts: family_ghost_names,
            };
            mappings.push(mapping);
        }
        Ok(mappings)
    }

    pub fn regenerate_cache_entries(&mut self, table_to_oid_to_fam: &Vec<(String, u64, GhostFamily)>) {
        // get rows corresponding to these ghost family names
        for (table, oid, ghost_family) in table_to_oid_to_fam {
            let gm = self.ghost_maps.get_mut(table).unwrap();
            gm.regenerate_cache_entry(*oid, &vec![ghost_family.clone()]);
        }
    }
}
