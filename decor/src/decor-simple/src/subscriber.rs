use crypto::digest::Digest;
use crypto::sha3::Sha3;
use mysql::prelude::*;
use std::collections::{HashMap, HashSet};
use std::*;
use log::{warn};
use msql_srv::{QueryResultWriter};
use crate::{helpers, EntityData, ghosts::GhostEidMapping, querier::TraversedEntity}; 

const EID_COL: &'static str = "entity_id";
const GM_HASH_COL: &'static str = "ghost_mappings";
const ED_HASH_COL: &'static str = "entity_data";
const UNSUB_TABLE_NAME: &'static str = "unsubscribed";

pub struct Subscriber{
    pub unsubscribed: HashMap<u64, (String, String)>,
    pub hasher: Sha3,
    pub nqueries: usize,
}

pub fn delete_from_unsubscribed_table(db: &mut mysql::Conn, eid: u64) -> Result<(), mysql::Error> {
    let q = format!(r"DELETE FROM {} WHERE {} = {};",
        UNSUB_TABLE_NAME, EID_COL, eid);
    warn!("delete from unsubscribed table {}", q);
    db.query_drop(q)?;
    Ok(())
}

pub fn insert_into_unsubscribed_table(db: &mut mysql::Conn, eid: u64, hash1: &str, hash2: &str) -> Result<(), mysql::Error> {
    let q = format!(r"INSERT INTO {} ({}, {}, {}) VALUES ({}, '{}', '{}');",
        UNSUB_TABLE_NAME, EID_COL, GM_HASH_COL, ED_HASH_COL, 
        eid, helpers::escape_quotes_mysql(hash1), helpers::escape_quotes_mysql(hash2));
    warn!("insert into unsubscribed table {}", q);
    db.query_drop(q)?;
    Ok(())
}

pub fn create_unsubscribed_table(db: &mut mysql::Conn, in_memory: bool) -> Result<(), mysql::Error> {
    db.query_drop(&format!("DROP TABLE IF EXISTS {};", UNSUB_TABLE_NAME))?;
    let mut q = format!(
        r"CREATE TABLE IF NOT EXISTS {} (
            `{}` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            `{}` varchar(4096), 
            `{}` varchar(4096), INDEX eid (`{}`))", 
        UNSUB_TABLE_NAME, EID_COL, GM_HASH_COL, ED_HASH_COL, EID_COL);
    if in_memory {
        q.push_str(" ENGINE = MEMORY");
    }
    warn!("drop/create unsubscribed table {}", q);
    db.query_drop(q)?;
    Ok(())
}

pub fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    serialized1: String,
    serialized2: String)
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
    warn!("Subscriber: Serialized values are {}, {}", serialized1, serialized2);
    let mut writer = results.start(&cols)?;
    writer.write_col(mysql_common::value::Value::Bytes(serialized1.as_bytes().to_vec()))?;
    writer.write_col(mysql_common::value::Value::Bytes(serialized2.as_bytes().to_vec()))?;
    writer.end_row()?;
    writer.finish()?;
    Ok(())
}

pub fn traversed_entity_to_entitydata(entity: &TraversedEntity) -> EntityData {
    EntityData{
        table: entity.table_name.clone(),
        eid: entity.eid,
        row_strs: entity.hrptr.row().borrow().iter().map(|v| v.to_string()).collect(),
    }
}

impl Subscriber {
    pub fn new() -> Self {
        Subscriber {
            unsubscribed: HashMap::new(),
            hasher : Sha3::sha3_256(),
            nqueries: 0,
        }
    }

    pub fn init(&mut self, db: &mut mysql::Conn, prime: bool, in_memory: bool) -> Result<(), mysql::Error> {
        if prime {
            create_unsubscribed_table(db, in_memory)
        } else {
            self.restore_unsubscribed_cache(db)
        }
    }

    pub fn record_unsubbed_user_and_return_results<W: io::Write>(&mut self,
        writer: QueryResultWriter<W>,
        eid: u64,
        ghost_eid_mappings: &mut Vec<GhostEidMapping>,
        entity_data: &mut HashSet<EntityData>,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        // cache the hash of the gids we are returning
        ghost_eid_mappings.sort();
        let serialized1 = serde_json::to_string(&ghost_eid_mappings).unwrap();
        self.hasher.input_str(&serialized1);
        let result1 = self.hasher.result_str();
        warn!("Hashing {}, got {}", serialized1, result1);
        self.hasher.reset();
    
        // note, the recipient has to just return the entities in order...
        let mut entity_data : Vec<&EntityData> = entity_data.iter().collect();
        entity_data.sort();
        let serialized2 = serde_json::to_string(&entity_data).unwrap();
        self.hasher.input_str(&serialized2);
        let result2 = self.hasher.result_str();
        warn!("Hashing {}, got {}", serialized2, result2);
        self.hasher.reset();
        
        insert_into_unsubscribed_table(db, eid, &result1, &result2)?;
        self.nqueries+=1;
        self.unsubscribed.insert(eid, (result1, result2));
        answer_rows(writer, serialized1, serialized2)
    }

    pub fn check_and_sort_resubscribed_data(&mut self,
        eid: u64,
        ghost_eid_mappings: &mut Vec<GhostEidMapping>,
        entity_data: &mut Vec<EntityData>,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        match self.unsubscribed.get(&eid) {
            Some((gidshash, datahash)) => {
                ghost_eid_mappings.sort();
                let serialized = serde_json::to_string(&ghost_eid_mappings).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *gidshash != hashed {
                    warn!("Resubscribing {} gidshash {} mismatch {}, {}", eid, serialized, gidshash, hashed);
                    return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!(
                                    "User attempting to resubscribe with bad data {} {}", eid, serialized))));
                }
                self.hasher.reset();

                entity_data.sort();
                let serialized = serde_json::to_string(&entity_data).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *datahash != hashed {
                    warn!("Resubscribing {} datahash {} mismatch {}, {}", eid, serialized, datahash, hashed);
                    return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!(
                                    "User attempting to resubscribe with bad data {} {}", eid, serialized))));
                }
                self.hasher.reset();
                self.unsubscribed.remove(&eid); 
                delete_from_unsubscribed_table(db, eid)?;
                self.nqueries+=1;
            }
            None => {
                return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!("User not unsubscribed {}", eid))));
            }
        }
        Ok(())
    }

    pub fn restore_unsubscribed_cache(&mut self, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        let res = db.query_iter(format!("SELECT * FROM {}", UNSUB_TABLE_NAME))?;
        self.nqueries+=1;
        for row in res {
            let vals = row.unwrap().unwrap();
            assert!(vals.len()==3);
            
            let eid = helpers::mysql_val_to_u64(&vals[0])?;
            let gmapping_hash = helpers::mysql_val_to_string(&vals[1]);
            let edata_hash = helpers::mysql_val_to_string(&vals[2]);
            self.unsubscribed.insert(eid, (gmapping_hash, edata_hash));
        }
        Ok(())
    }

    pub fn get_nqueries(&mut self) -> usize {
        let n = self.nqueries;
        self.nqueries = 0;
        n
    }
}
