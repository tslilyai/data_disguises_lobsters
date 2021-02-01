use crypto::digest::Digest;
use crypto::sha3::Sha3;
use mysql::prelude::*;
use std::collections::{HashMap, HashSet};
use std::*;
use log::{warn};
use msql_srv::{QueryResultWriter};
use crate::{helpers, ghosts::GhostOidMapping, types::{ObjectData, TraversedObject}}; 

const OID_COL: &'static str = "object_id";
const GM_HASH_COL: &'static str = "ghost_mappings";
const ED_HASH_COL: &'static str = "object_data";
const UNSUB_TABLE_NAME: &'static str = "unsubscribed";

pub struct Subscriber{
    pub unsubscribed: HashMap<u64, (String, String)>,
    pub hasher: Sha3,
    pub nqueries: usize,
}

pub fn delete_from_unsubscribed_table(db: &mut mysql::Conn, oid: u64) -> Result<(), mysql::Error> {
    let q = format!(r"DELETE FROM {} WHERE {} = {};",
        UNSUB_TABLE_NAME, OID_COL, oid);
    warn!("delete from unsubscribed table {}", q);
    db.query_drop(q)?;
    Ok(())
}

pub fn insert_into_unsubscribed_table(db: &mut mysql::Conn, oid: u64, hash1: &str, hash2: &str) -> Result<(), mysql::Error> {
    let q = format!(r"INSERT INTO {} ({}, {}, {}) VALUES ({}, '{}', '{}');",
        UNSUB_TABLE_NAME, OID_COL, GM_HASH_COL, ED_HASH_COL, 
        oid, helpers::escape_quotes_mysql(hash1), helpers::escape_quotes_mysql(hash2));
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
            `{}` varchar(4096), INDEX oid (`{}`))", 
        UNSUB_TABLE_NAME, OID_COL, GM_HASH_COL, ED_HASH_COL, OID_COL);
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
        oid: u64,
        ghost_oid_mappings: &mut Vec<GhostOidMapping>,
        object_data: &mut HashSet<ObjectData>,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        // cache the hash of the gids we are returning
        ghost_oid_mappings.sort();
        let serialized1 = serde_json::to_string(&ghost_oid_mappings).unwrap();
        self.hasher.input_str(&serialized1);
        let result1 = self.hasher.result_str();
        warn!("Hashing {}, got {}", serialized1, result1);
        self.hasher.reset();
    
        // note, the recipient has to just return the entities in order...
        let mut object_data : Vec<&ObjectData> = object_data.iter().collect();
        object_data.sort();
        let serialized2 = serde_json::to_string(&object_data).unwrap();
        self.hasher.input_str(&serialized2);
        let result2 = self.hasher.result_str();
        warn!("Hashing {}, got {}", serialized2, result2);
        self.hasher.reset();
        
        insert_into_unsubscribed_table(db, oid, &result1, &result2)?;
        self.nqueries+=1;
        self.unsubscribed.insert(oid, (result1, result2));
        answer_rows(writer, serialized1, serialized2)
    }

    pub fn check_and_sort_resubscribed_data(&mut self,
        oid: u64,
        ghost_oid_mappings: &mut Vec<GhostOidMapping>,
        object_data: &mut Vec<ObjectData>,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        match self.unsubscribed.get(&oid) {
            Some((gidshash, datahash)) => {
                ghost_oid_mappings.sort();
                let serialized = serde_json::to_string(&ghost_oid_mappings).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *gidshash != hashed {
                    warn!("Resubscribing {} gidshash {} mismatch {}, {}", oid, serialized, gidshash, hashed);
                    return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!(
                                    "User attempting to resubscribe with bad data {} {}", oid, serialized))));
                }
                self.hasher.reset();

                object_data.sort();
                let serialized = serde_json::to_string(&object_data).unwrap();
                self.hasher.input_str(&serialized);
                let hashed = self.hasher.result_str();
                if *datahash != hashed {
                    warn!("Resubscribing {} datahash {} mismatch {}, {}", oid, serialized, datahash, hashed);
                    return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!(
                                    "User attempting to resubscribe with bad data {} {}", oid, serialized))));
                }
                self.hasher.reset();
                self.unsubscribed.remove(&oid); 
                delete_from_unsubscribed_table(db, oid)?;
                self.nqueries+=1;
            }
            None => {
                return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::Other, format!("User not unsubscribed {}", oid))));
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
            
            let oid = helpers::mysql_val_to_u64(&vals[0])?;
            let gmapping_hash = helpers::mysql_val_to_string(&vals[1]);
            let edata_hash = helpers::mysql_val_to_string(&vals[2]);
            self.unsubscribed.insert(oid, (gmapping_hash, edata_hash));
        }
        Ok(())
    }

    pub fn get_nqueries(&mut self) -> usize {
        let n = self.nqueries;
        self.nqueries = 0;
        n
    }
}
