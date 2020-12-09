use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{ghost, helpers, policy::EntityGhostPolicies, views::{Views, RowPtr}, ID_COL};
use crate::ghost::{GhostEidMapping, GhostFamily, TemplateEntity};
use std::sync::atomic::Ordering;
use std::*;
use log::{warn, error};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap};
use msql_srv::{QueryResultWriter};

pub const GHOST_ID_START : u64 = 1<<20;
pub const GHOST_ID_MAX: u64 = 1<<30;

// the ghosts table contains ALL ghost identifiers which map from any entity to its ghosts
// this assumes that all entities have an integer identifying key
const GHOST_ENTITY_COL : &'static str = "entity_id";
const GHOST_ID_COL: &'static str = "ghost_id";
const GHOST_DATA_COL: &'static str = "ghost_data";

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
fn create_ghosts_table(name: String, db: &mut mysql::Conn, in_memory: bool) -> Result<(), mysql::Error> {
    db.query_drop(&format!("DROP TABLE IF EXISTS {};", name))?;
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` int unsigned, 
            `{}` varchar(4096), INDEX eid (`{}`))", 
        name, GHOST_ID_COL, GHOST_ENTITY_COL, GHOST_DATA_COL, GHOST_ENTITY_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    warn!("drop/create/alter ghosts table {}: {}", name, q);
    db.query_drop(q)?;
    let q = format!(r"ALTER TABLE {} AUTO_INCREMENT={};",
        name, GHOST_ID_START);
    db.query_drop(q)?;
    Ok(())
}

/*
 * INVARIANTS:
 *  - ghost map contains only mappings between real entity EIDs and their ghost counterpart GIDs;
 *      these mappings only exist for edges that are *preemptively* created because the edge type
 *      can be decorrelated.
 *  - these GIDs always correspond to actual ghosts in the datatables, which always exist
 *  - these ghost sibling entities MAY have parents who are also ghosts which are not present in
 *      the mapping to ensure referential integrity; these rptrs are contained in the mapping
 */
pub struct GhostsMap{
    table_name: String,
    name: String,
    eid2gids: HashMap<u64, Vec<GhostFamily>>,
    gid2eid: HashMap<u64, u64>,
    latest_gid: AtomicU64,
    
    pub nqueries: usize,
}


impl GhostsMap {
    pub fn new(table_name: String, db: Option<&mut mysql::Conn>, in_memory: bool) -> Self {
        let name = format!("ghost{}", table_name);
        if let Some(db) = db {
            create_ghosts_table(name.clone(), db, in_memory).unwrap();
        }
        GhostsMap{
            table_name: table_name.clone(),
            name: name.clone(),
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
    fn unsubscribe(&mut self, eid:u64, db: &mut mysql::Conn) -> Result<Option<Vec<GhostFamily>>, mysql::Error> {
        warn!("{} Unsubscribing {}", self.name, eid);
        let start = time::Instant::now();
        if let Some(ghost_families) = self.eid2gids.remove(&eid) {
            let mut gids = vec![];
            let mut families : Vec<GhostFamily> = vec![];
            
            // remove gids from reverse mapping
            for family in ghost_families {
                self.gid2eid.remove(&family.root_gid);
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
            warn!("{} Found {} gids for {}", self.name, gids.len(), eid);
            return Ok(Some(families));
        } 
        return Ok(None);
    }  
    /* 
     * Removes the eid unsubscribing.
     * Returns true if was unsubscribed 
     */
    pub fn resubscribe(&mut self, 
                       eid: u64, 
                       ghost_families: &Vec<GhostFamily>,
                       db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
        warn!("{} Resubscribing {}", self.name, eid);
        let start = time::Instant::now();
       
        self.regenerate_cache_entry(eid, ghost_families);

        let mut pairs = vec![];
        for family in ghost_families {
            pairs.push(family.ghost_family_to_db_string(eid));
        }
        // insert into ghost table
        let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    self.name, GHOST_ID_COL, GHOST_ENTITY_COL, GHOST_DATA_COL, pairs.join(","));
        warn!("Inserting into ghosts table {}", insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;
        warn!("RESUB {} insert_gid_for_eid {}: {}, dur {}us", self.name, eid, insert_query, start.elapsed().as_micros());
        Ok(true)
    }

    fn regenerate_cache_entry(&mut self, eid: u64, ghost_families: &Vec<GhostFamily>) {
        let mut families_of_eid = vec![];
        for family in ghost_families {
            self.gid2eid.insert(family.root_gid, eid);
            // save to insert into forward map
            families_of_eid.push(family.clone());
        }

        if let Some(families) = self.eid2gids.get_mut(&eid) {
            families.append(&mut families_of_eid);
        } else {
            self.eid2gids.insert(eid, families_of_eid);
        }
    }

    pub fn insert_gid_into_caches(&mut self, eid:u64, gfam: GhostFamily) {
        match self.eid2gids.get_mut(&eid) {
            Some(gfams) => (*gfams).push(gfam.clone()),
            None => {
                self.eid2gids.insert(eid, vec![gfam.clone()]);
            }
        }
        self.gid2eid.insert(gfam.root_gid, eid);
    }
 
    pub fn update_eid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let mut gids_to_delete = vec![];
        for (eid, gid) in pairs {
            // delete current mapping
            let mut vals: Option<GhostFamily> = None;
            if let Some(oldeid) = self.gid2eid.get(gid) {
                if let Some(gfams) = self.eid2gids.get_mut(&oldeid) {
                    let pos = gfams.iter().position(|gfam| gfam.root_gid == *gid).unwrap();
                    vals = Some(gfams.remove(pos));
                }
                self.gid2eid.remove(&gid);
            }

            // delete from datatable if eid is none (set to NULL)
            if eid.is_none() {
                gids_to_delete.push(Expr::Value(Value::Number(gid.to_string())));
            }

            // update if there is a new mapping
            else if let Some(neweid) = eid {
                self.insert_gid_into_caches(*neweid, vals.unwrap());
                
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

    pub fn take_one_ghost_family_for_eid(&mut self, eid: u64, db: &mut mysql::Conn) -> 
        Result<Option<GhostFamily>, mysql::Error> 
    {
        let ghost_fams = self.eid2gids.get_mut(&eid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: eid not present in cache?")))?;

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
            warn!("{} delete from eid2gids_with : {}", self.name, delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;
            Ok(Some(fam))
        } else {
            Ok(None)
        }
    }

    pub fn get_gids_for_eid(&mut self, eid: u64) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        //self.cache_eid2gids_for_eids(&vec![eid])?;
        let gids = self.eid2gids.get(&eid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: eid not present in cache?")))?;
        Ok(gids.iter().map(|fam| fam.root_gid).collect())
    }

    pub fn get_gids_for_eids(&mut self, eids: &Vec<u64>) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        //self.cache_eid2gids_for_eids(eids)?;
        let mut gid_vecs = vec![];
        for eid in eids {
            let gids = self.eid2gids.get(&eid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: eid not present in cache?")))?;
            let gids = gids.iter().map(|fam| fam.root_gid).collect();
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
        let start = time::Instant::now();
       
        let gid = self.latest_gid.fetch_add(1, Ordering::SeqCst);
        let new_entities = ghost::generate_new_ghosts_with_gids(
            views, gp, db, &TemplateEntity{
                table: self.table_name.clone(), 
                row: from_vals, 
                fixed_colvals: None,
            },
            &vec![Value::Number(gid.to_string())], &mut self.nqueries)?;

        let new_family = GhostFamily{
            root_table: self.table_name.clone(),
            root_gid: gid,
            family_members: new_entities,
        };
        let dbentry = new_family.ghost_family_to_db_string(eid);
        
        // insert into in-memory cache
        self.insert_gid_into_caches(eid, new_family);

        // insert into DB
        let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    self.name, GHOST_ID_COL, GHOST_ENTITY_COL, GHOST_DATA_COL, dbentry);
        warn!("Inserting into ghosts table {}", insert_query);
        db.query_drop(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        
        warn!("{} insert_gid_for_eid {}, {}: {}us", self.name, gid, eid, dur.as_millis());
        Ok(gid)
    }
}

pub struct GhostMaps{
    ghost_maps: HashMap<String, GhostsMap> // table name to ghost map
}

impl GhostMaps {
    pub fn new() -> Self {
        GhostMaps{
            ghost_maps: HashMap::new()
        }
    }

    pub fn new_ghost_map(&mut self, name: String, db: &mut mysql::Conn, in_mem: bool) {
        self.ghost_maps.insert(name.to_string(), GhostsMap::new(name.to_string(), Some(db), in_mem));
    }

    pub fn new_ghost_map_cache_only(&mut self, name: String) {
        self.ghost_maps.insert(name.to_string(), GhostsMap::new(name.to_string(), None, true));
    }

    pub fn insert_gid_for_eid(&mut self, 
                     views: &Views,
                     gp: &EntityGhostPolicies,
                     from_vals: RowPtr,
                    eid: u64, db: &mut mysql::Conn, table_name: &str) -> Result<u64, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(table_name).unwrap();
        gm.insert_gid_for_eid(views, gp, from_vals, eid, db)
    }

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

    pub fn unsubscribe(&mut self, eid:u64, db: &mut mysql::Conn, parent_table: &str) -> Result<Option<Vec<GhostFamily>>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.unsubscribe(eid, db)
    }
    pub fn resubscribe(&mut self, eid: u64, families: &Vec<GhostFamily>, db: &mut mysql::Conn, parent_table: &str) -> Result<bool, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.resubscribe(eid, families, db)
    }

    pub fn take_one_ghost_family_for_eid(&mut self, eid: u64, db: &mut mysql::Conn, parent_table: &str) -> 
        Result<Option<GhostFamily>, mysql::Error> {
        let gm = self.ghost_maps.get_mut(parent_table).unwrap();
        gm.take_one_ghost_family_for_eid(eid, db)
    }

    pub fn get_ghost_eid_mappings(&mut self, db: &mut mysql::Conn, table: &str) -> 
        Result<Vec<GhostEidMapping>, mysql::Error> 
    {
        let gm = self.ghost_maps.get_mut(table).unwrap();
        let mut mappings = vec![]; 

        let res = db.query_iter(format!("SELECT * FROM {}", gm.name))?;
        for row in res {
            let vals = row.unwrap().unwrap();
            assert!(vals.len()==3);
            
            let root_gid = helpers::mysql_val_to_u64(&vals[0])?;
            let eid = helpers::mysql_val_to_u64(&vals[1])?;
            let ghostdata = helpers::mysql_val_to_string(&vals[2]);
            let ghostdata = ghostdata.trim_end_matches('\'').trim_start_matches('\'');
            let family_ghost_names = serde_json::from_str(&ghostdata).unwrap();
            let mapping = GhostEidMapping {
                table: table.to_string(),
                eid2gidroot: Some((eid, root_gid)),
                ghosts: family_ghost_names,
            };
            mappings.push(mapping);
        }
        Ok(mappings)
    }

    pub fn regenerate_cache_entries(&mut self, table_to_eid_to_fam: &Vec<(String, u64, GhostFamily)>) {
        // get rows corresponding to these ghost family names
        for (table, eid, ghost_family) in table_to_eid_to_fam {
            let gm = self.ghost_maps.get_mut(table).unwrap();
            gm.regenerate_cache_entry(*eid, &vec![ghost_family.clone()]);
        }
    }
}
