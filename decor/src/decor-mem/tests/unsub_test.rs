extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use log::warn;
use decor_mem::{GhostMappingShard, EntityDataShard, helpers};
mod policies;
//use crate::policies;

const SCHEMA : &'static str = include_str!("./schema.sql");
const GHOST_ID_START : u64 = 1<<20;
fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[test]
fn test_unsub_noop() {
    init_logger();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            decor_mem::Shim::run_on_tcp(
                    "decor_noop", SCHEMA, policies::noop_policy(),
                    decor_mem::TestParams{
                        testname: "test_normal".to_string(), 
                        translate:true, parse:true, in_memory: true}, s).unwrap();
        }
    });

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db("decor_noop"), true);

    let mut db_actual = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    assert_eq!(db_actual.ping(), true);
    assert_eq!(db_actual.select_db("decor_noop"), true);
    
    /* 
     * Create graph as follows:
     *      user1 <- story1, story2
     *      user2 <- story3, story4
     *      user1 <- mod1, mod2
     *      user2 <- mod2, mod1
     *      story1 <- mod1
     *      story2 <- mod2
     */
    db.query_drop(r"INSERT INTO users (username) VALUES ('hello_1'), ('hello_2');").unwrap();
    db.query_drop(r"INSERT INTO stories (user_id, url) VALUES (1, 'google.com');").unwrap();
    db.query_drop(r"INSERT INTO stories (user_id, url) VALUES (1, 'bing.com');").unwrap();
    db.query_drop(r"INSERT INTO stories (user_id, url) VALUES (2, 'reddit.com');").unwrap();
    db.query_drop(r"INSERT INTO stories (user_id, url) VALUES (2, 'fb.com');").unwrap();
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (1, 1, 2, 'bad story!');").unwrap();
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (2, 2, 1, 'worst story!');").unwrap();

    /* 
     * Check: No ghost entities have been created
     */
    let res = db_actual.query_iter(r"SELECT * FROM ghostusers ORDER BY entity_id;").unwrap();
    for row in res {
        warn!("Found row for ghostusers {:?}", row);
        assert!(false);
    }
    let res = db_actual.query_iter(r"SELECT * FROM ghoststories ORDER BY entity_id;").unwrap();
    for row in res {
        warn!("Found row for ghoststories {:?}", row);
        assert!(false);
    }
    let res = db_actual.query_iter(r"SELECT * FROM ghostmoderations ORDER BY entity_id;").unwrap();
    for row in res {
        warn!("Found row for ghostmods {:?}", row);
        assert!(false);
    }

    /* 
     *  Unsubscribe of user 1 does nothing
     */
    let mut unsubscribed_gids : GhostMappingShard = vec![];
    let mut entity_data : EntityDataShard = vec![];
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", 1)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        let s1 = s1.trim_end_matches('\'').trim_start_matches('\'');
        let s2 = s2.trim_end_matches('\'').trim_start_matches('\'');
        warn!("Serialized values are {}, {}", s1, s2);
        unsubscribed_gids = serde_json::from_str(s1).unwrap();
        entity_data = serde_json::from_str(s2).unwrap();
        assert!(unsubscribed_gids.is_empty());
        assert!(entity_data.is_empty());
    }
    
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", helpers::mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", helpers::mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", helpers::mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", helpers::mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", helpers::mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], 
            ("'2'".to_string(), 
            "'2'".to_string(), 
            "'2'".to_string(), 
            "'1'".to_string(), 
            "'worst story!'".to_string()));
    assert_eq!(results[1],
            ("'1'".to_string(), 
            "'1'".to_string(), 
            "'1'".to_string(), 
            "'2'".to_string(), 
            "'bad story!'".to_string()));
  
    /* 
     *  Unsubscribe of user 2 does nothing
     */
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", 1)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        let s1 = s1.trim_end_matches('\'').trim_start_matches('\'');
        let s2 = s2.trim_end_matches('\'').trim_start_matches('\'');
        warn!("Serialized values are {}, {}", s1, s2);
        unsubscribed_gids = serde_json::from_str(s1).unwrap();
        entity_data = serde_json::from_str(s2).unwrap();
        assert!(unsubscribed_gids.is_empty());
        assert!(entity_data.is_empty());
    }
    
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", helpers::mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", helpers::mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", helpers::mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", helpers::mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", helpers::mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], 
            ("'2'".to_string(), 
            "'2'".to_string(), 
            "'2'".to_string(), 
            "'1'".to_string(), 
            "'worst story!'".to_string()));
    assert_eq!(results[1],
            ("'1'".to_string(), 
            "'1'".to_string(), 
            "'1'".to_string(), 
            "'2'".to_string(), 
            "'bad story!'".to_string()));
  
    /*
     * Check users for the heck of it
     */
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY id ASC;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = format!("{}", helpers::mysql_val_to_parser_val(&vals[0]));
        results.push(uid);
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], format!("'{}'", 1));
    assert_eq!(results[1], format!("'{}'", 2));

    drop(db);
    jh.join().unwrap();
}
