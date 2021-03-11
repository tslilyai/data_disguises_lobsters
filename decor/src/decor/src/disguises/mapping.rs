use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{guises, helpers, policy::ObjectGuisePolicies, ID_COL};
use crate::types::ID;
use crate::disguises::*;
use std::sync::atomic::Ordering;
use std::*;
use log::{warn};
use std::sync::atomic::{AtomicU64};
use std::collections::{HashMap};

// the guises table contains ALL guise identifiers which map from any object to its guises
// this assumes that all entities have an integer identifying key
const LOID_COL : &'static str = "lobject_id";
const GID_COL: &'static str = "guise_id";
const DATA_COL: &'static str = "guise_data";

fn create_mapping_table(table_name: String, db: &mut mysql::Conn, in_memory: bool) -> Result<(), mysql::Error> {
    let name = format!("guise{}", table_name);
    db.query_drop(&format!("DROP TABLE IF EXISTS {};", name))?;
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` int unsigned, 
            `{}` varchar(4096), INDEX oid (`{}`))", 
        name, GID_COL, LOID_COL, DATA_COL, LOID_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    warn!("drop/create/alter guises table {}: {}", name, q);
    db.query_drop(q)?;
    let q = format!(r"ALTER TABLE {} AUTO_INCREMENT={};",
        name, guises::GID_START);
    db.query_drop(q)?;
    Ok(())
}

fn insert_into_mapping_table(table_name: String, db: &mut mysql::Conn, data: Vec<String>) -> Result<(), mysql::Error> {
    let name = format!("guise{}", table_name);
    let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    name, GID_COL, LOID_COL, DATA_COL,
        warn!("Inserting into guises table {}", insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;

    db.query_drop(q)?;
    let q = format!(r"ALTER TABLE {} AUTO_INCREMENT={};",
        name, guises::GID_START);
    db.query_drop(q)?;
    Ok(())
}

fn read_from_mapping_table(table_name: String, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let name = format!("guise{}", table_name);
    let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    name, GID_COL, LOID_COL, DATA_COL, pairs.join(","));
        warn!("Inserting into guises table {}", insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;

    db.query_drop(q)?;
    let q = format!(r"ALTER TABLE {} AUTO_INCREMENT={};",
        name, guises::GID_START);
    db.query_drop(q)?;
    Ok(())
}

/*
 * INVARIANTS:
 *  - guise map contains only mappings between loids and their guise counterpart GIDs;
 *  - these GIDs always correspond to actual guises in the datatables, which always exist
 *  - these guise sibling entities MAY have parents who are also guises which are not present in
 *      the mapping to ensure referential integrity; these rptrs are contained in the mapping
 */
pub struct GuiseMap{
    table_name: String,
    name: String,
    loid2gids: HashMap<u64, Vec<ID>>,
    gid2loid: HashMap<u64, u64>,
    latest_gid: AtomicU64,
    
    pub nqueries: usize,
}


impl GuiseMap {
    pub fn new(table_name: String, db: Option<&mut mysql::Conn>, in_memory: bool) -> Self {
        if let Some(db) = db {
            create_mapping_table(name.clone(), db, in_memory).unwrap();
        }
        GuiseMap{
            table_name: table_name.clone(),
            name: name.clone(),
            loid2gids: HashMap::new(),
            gid2loid: HashMap::new(),
            latest_gid: AtomicU64::new(guises::GID_START),
            nqueries: 0,
        }
    }   
    
    /* 
     * Inserts the oid unsubscribing.
     * Returns a hash of the list of guises if user is not yet unsubscribed,
     * else None if the user is already unsubscribed
     */
    fn unsubscribe(&mut self, oid:u64, db: &mut mysql::Conn) -> Result<Option<Vec<ID>>, mysql::Error> {
        warn!("{} Unsubscribing {}", self.name, oid);
        let start = time::Instant::now();
        if let Some(guise_families) = self.loid2gids.remove(&oid) {
            let mut gids = vec![];
            let mut families : Vec<ID> = vec![];
            
            // remove gids from reverse mapping

            // delete from guises table
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.name),
                selection: Some(Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GID_COL))),
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
                       db: &mut mysql::Conn) -> Result<bool, mysql::Error> {
        warn!("{} Resubscribing {}", self.name, oid);
        let start = time::Instant::now();
       
        self.regenerate_cache_entry(oid, guise_families);

        // TODO
        let mut pairs = vec![];
        // insert into guise table
        let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    self.name, GID_COL, LOID_COL, DATA_COL, pairs.join(","));
        warn!("Inserting into guises table {}", insert_query);
        db.query_iter(insert_query)?;
        self.nqueries+=1;
        warn!("RESUB {} insert_gid_for_oid {}: {}, dur {}us", self.name, oid, insert_query, start.elapsed().as_micros());
        Ok(true)
    }

    fn regenerate_cache_entry(&mut self, oid: u64, gids: &Vec<ID>) {
        //TODO
    }

    pub fn insert_gid_into_caches(&mut self, oid:u64, gid: ID) {
        match self.loid2gids.get_mut(&oid) {
            Some(gfams) => (*gfams).push(gid.clone()),
            None => {
                self.loid2gids.insert(oid, vec![gid.clone()]);
            }
        }
        self.gid2loid.insert(gid, oid);
    }
 
    pub fn update_loid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn)
        -> Result<(), mysql::Error> 
    {
        let mut gids_to_delete = vec![];
        for (oid, gid) in pairs {
            // delete current mapping
            let mut vals: Option<ID> = None;
            if let Some(oldoid) = self.gid2loid.get(gid) {
                if let Some(gfams) = self.loid2gids.get_mut(&oldoid) {
                    let pos = gfams.iter().position(|gfam| gfam.root_gid == *gid).unwrap();
                    vals = Some(gfams.remove(pos));
                }
                self.gid2loid.remove(&gid);
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
                        id: Ident::new(LOID_COL),
                        value: Expr::Value(Value::Number(newoid.to_string())),
                    }],
                    selection: Some(Expr::BinaryOp{
                        left: Box::new(Expr::Identifier(
                                      helpers::string_to_idents(GID_COL))),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(gid.to_string()))),
                    }),
                });
                warn!("{} issue_update_loid2gids_stmt: {}", self.name, update_stmt);
                db.query_drop(format!("{}", update_stmt))?;
                self.nqueries+=1;

                // note that the guise entry in the parent datatable can stay constant; it's just
                // pointed to by a different child object now
            }
            self.latest_gid.fetch_max(*gid, Ordering::SeqCst);
        }
        if !gids_to_delete.is_empty() {
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.name),
                selection: Some(Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&GID_COL))),
                    list: gids_to_delete.clone(),
                    negated: false,
                }),
            });
            warn!("{} update loid2gids_with : {}", self.name, delete_stmt);
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
            warn!("{} update loid2gids_with : {}", self.name, delete_stmt);
            db.query_drop(format!("{}", delete_stmt))?;
            self.nqueries+=1;
        }
        Ok(())
    }

    pub fn take_one_guise_for_oid(&mut self, oid: u64, db: &mut mysql::Conn) -> 
        Result<Option<ID>, mysql::Error> 
    {
        let guise_fams = self.loid2gids.get_mut(&oid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: oid not present in cache?")))?;

        if let Some(fam) = guise_fams.pop() {
            // delete mapping
            let delete_stmt = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&self.name),
                selection: Some(Expr::BinaryOp{
                    left: Box::new(Expr::Identifier(helpers::string_to_idents(&GID_COL))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(fam.root_gid.to_string()))),
                }),
            });
            warn!("{} delete from loid2gids_with : {}", self.name, delete_stmt);
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
        //self.cache_loid2gids_for_oids(&vec![oid])?;
        let gids = self.loid2gids.get(&oid).ok_or(
                mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, "get_gids: oid not present in cache?")))?;
        Ok(gids.iter().map(|fam| fam.root_gid).collect())
    }

    pub fn get_gids_for_oids(&mut self, oids: &Vec<u64>) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        //self.cache_loid2gids_for_oids(oids)?;
        let mut gid_vecs = vec![];
        for oid in oids {
            let gids = self.loid2gids.get(&oid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: oid not present in cache?")))?;
            let gids = gids.iter().map(|fam| fam.root_gid).collect();
            gid_vecs.push((*oid, gids));
        }
        Ok(gid_vecs)
    }

    pub fn insert_guise_for_oid(&mut self, 
                                views: &Views,
                                gp: &ObjectGuisePolicies,
                                from_vals: RowPtr,
                                oid: u64, 
                                db: &mut mysql::Conn) 
        -> Result<u64, mysql::Error> 
    {
        let start = time::Instant::now();
       
        let gid = self.latest_gid.fetch_add(1, Ordering::SeqCst) + 1;

        let new_entities = guises::generate_new_guises_from(
            views, gp, 
            &TemplateObject{
                name: ObjectIdentifier {
                    table: self.table_name.clone(), 
                    oid: oid,
                },
                row: from_vals, 
                fixed_colvals: None,
            }, 1)?;
            //&vec![Value::Number(gid.to_string())], 
            //&mut self.nqueries)?;
        // TODO will need to insert into datatables

        let new_family = GuiseFamily{
            root_table: self.table_name.clone(),
            root_gid: gid,
            family_members: new_entities,
        };
        let dbentry = new_family.guise_family_to_db_string(oid);
        
        // insert into in-memory cache
        self.insert_gid_into_caches(oid, new_family);

        // insert into DB
        let insert_query = &format!("INSERT INTO {} ({}, {}, {}) VALUES {};", 
                                    self.name, GID_COL, LOID_COL, DATA_COL, dbentry);
        warn!("Inserting into guises table {}", insert_query);
        db.query_drop(insert_query)?;
        self.nqueries+=1;
        let dur = start.elapsed();
        
        warn!("{} insert_guise_for_oid {}, {}: {}us", self.name, gid, oid, dur.as_millis());
        Ok(gid)
    }
}

pub struct GuiseMaps{
    guise_maps: HashMap<String, GuiseMap> // table name to guise map
}

impl GuiseMaps {
    pub fn new() -> Self {
        GuiseMaps{
            guise_maps: HashMap::new()
        }
    }

    pub fn new_guise_map(&mut self, name: String, db: &mut mysql::Conn, in_mem: bool) {
        self.guise_maps.insert(name.to_string(), GuiseMap::new(name.to_string(), Some(db), in_mem));
    }

    pub fn new_guise_map_cache_only(&mut self, name: String) {
        self.guise_maps.insert(name.to_string(), GuiseMap::new(name.to_string(), None, true));
    }

    pub fn insert_guise_for_oid(&mut self, 
                     views: &Views,
                     gp: &ObjectGuisePolicies,
                     from_vals: RowPtr,
                    oid: u64, db: &mut mysql::Conn, table_name: &str) -> Result<u64, mysql::Error> 
    {
        let gm = self.guise_maps.get_mut(table_name).unwrap();
        gm.insert_guise_for_oid(views, gp, from_vals, oid, db)
    }

    pub fn get_nqueries(&mut self) -> usize {
        let mut n = 0;
        for (_, gm) in self.guise_maps.iter_mut() {
            n += gm.nqueries;
            gm.nqueries = 0;
        }
        n
    }

    pub fn get_gids_for_oid(&mut self, oid: u64, parent_table: &str) -> 
        Result<Vec<u64>, mysql::Error> 
    {
        let gm = self.guise_maps.get_mut(parent_table).unwrap();
        gm.get_gids_for_oid(oid)
    }

    pub fn get_gids_for_oids(&mut self, oids: &Vec<u64>, parent_table: &str) -> 
        Result<Vec<(u64, Vec<u64>)>, mysql::Error> {
        let gm = self.guise_maps.get_mut(parent_table).unwrap();
        gm.get_gids_for_oids(oids)
    }

 
    pub fn update_loid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>, db: &mut mysql::Conn, parent_table: &str)
        -> Result<(), mysql::Error> 
    {
        let gm = self.guise_maps.get_mut(parent_table).unwrap();
        gm.update_loid2gids_with(pairs, db)
    }

    pub fn unsubscribe(&mut self, oid:u64, db: &mut mysql::Conn, parent_table: &str) -> Result<Option<Vec<GuiseFamily>>, mysql::Error> {
        let gm = self.guise_maps.get_mut(parent_table).unwrap();
        gm.unsubscribe(oid, db)
    }
    pub fn resubscribe(&mut self, oid: u64, families: &Vec<GuiseFamily>, db: &mut mysql::Conn, parent_table: &str) -> Result<bool, mysql::Error> {
        let gm = self.guise_maps.get_mut(parent_table).unwrap();
        gm.resubscribe(oid, families, db)
    }

    pub fn take_one_guise_family_for_oid(&mut self, oid: u64, db: &mut mysql::Conn, parent_table: &str) -> 
        Result<Option<GuiseFamily>, mysql::Error> {
        let gm = self.guise_maps.get_mut(parent_table).unwrap();
        gm.take_one_guise_family_for_oid(oid, db)
    }

    pub fn get_guise_oid_mappings(&mut self, db: &mut mysql::Conn, table: &str) -> 
        Result<Vec<GuiseOidMapping>, mysql::Error> 
    {
        let gm = self.guise_maps.get_mut(table).unwrap();
        let mut mappings = vec![]; 

        let res = db.query_iter(format!("SELECT * FROM {}", gm.name))?;
        for row in res {
            let vals = row.unwrap().unwrap();
            assert!(vals.len()==3);
           
            // TODO this is wrong
            let root_gids = vec![helpers::mysql_val_to_u64(&vals[0])?];
            let oid = helpers::mysql_val_to_u64(&vals[1])?;
            let guisedata = helpers::mysql_val_to_string(&vals[2]);
            let guisedata = guisedata.trim_end_matches('\'').trim_start_matches('\'');
            let family_guise_names = serde_json::from_str(&guisedata).unwrap();
            let mapping = GuiseOidMapping {
                name: ObjectIdentifier {
                    table: table.to_string(),
                    oid: oid,
                },
                root_gids: root_gids,
                guises: family_guise_names,
            };
            mappings.push(mapping);
        }
        Ok(mappings)
    }

    pub fn regenerate_cache_entries(&mut self, table_to_oid_to_fam: &Vec<(String, u64, GuiseFamily)>) {
        // get rows corresponding to these guise family names
        for (table, oid, guise_family) in table_to_oid_to_fam {
            let gm = self.guise_maps.get_mut(table).unwrap();
            gm.regenerate_cache_entry(*oid, &vec![guise_family.clone()]);
        }
    }
}
