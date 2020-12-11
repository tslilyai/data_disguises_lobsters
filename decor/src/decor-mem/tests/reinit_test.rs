extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use log::warn;
use decor_mem::{ghosts::GhostEidMapping, EntityData, helpers, policy::ApplicationPolicy};
use std::collections::{HashSet};
use std::str::FromStr;
mod policies;
mod transformer_test;

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

fn init_dbs(name: &'static str, policy: ApplicationPolicy, db: &mut mysql::Conn, db_actual: &mut mysql::Conn) {
    init_logger();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let _jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            decor_mem::Shim::run_on_tcp(
                    name, SCHEMA, policy, 
                    decor_mem::TestParams{
                        testname: name.to_string(), 
                        translate:true, parse:true, in_memory: true,
                        prime: true,}, s).unwrap();
        }
    });

    *db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db(name), true);

    *db_actual = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    assert_eq!(db_actual.ping(), true);
    assert_eq!(db_actual.select_db(name), true);

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
}

fn restore_db(name: &'static str, policy: ApplicationPolicy, db: &mut mysql::Conn) {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let _jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            decor_mem::Shim::run_on_tcp(
                    name, SCHEMA, policy, 
                    decor_mem::TestParams{
                        testname: name.to_string(), 
                        translate:true, parse:true, in_memory: true,
                        prime: false,}, s).unwrap();
        }
    });

    *db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db(name), true);
}

#[test]
fn test_init_noop() {
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    let mut db_actual: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    init_dbs("test_reinit_noop", policies::noop_policy(), &mut db, &mut db_actual);

    // perform some inserts, updates
    db.query_drop(r"INSERT INTO stories (user_id, url) VALUES (1, 'nytimes.com');").unwrap();
    db.query_drop(r"INSERT INTO stories (user_id, url) VALUES (2, 'nypost.com');").unwrap();
    db.query_drop(r"UPDATE users SET karma=10 WHERE username='hello_1'").unwrap();

    // check that database state makes sense: no ghost users or ghost anything
    let mut results = vec![];
    let res = db_actual.query_iter("SHOW tables;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let name = format!("{}", helpers::mysql_val_to_string(&vals[0]));
        let trimmed = name.trim_end_matches("\'").trim_start_matches("\'").to_string();
        results.push(trimmed);
    }
    let tables = vec![
        "ghoststories", 
        "ghostusers", 
        "ghostmoderations", 
        "stories", 
        "users", 
        "moderations", 
        "unsubscribed", 
    ];
    assert_eq!(results.len(), tables.len());
    for tab in results {
        assert!(tables.iter().any(|tt| &tab == *tt));
    }
   
    // check that users were updated correctly
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM users WHERE users.username='hello_1' OR users.username='hello_2' ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 3);
        let id = format!("{}", helpers::mysql_val_to_string(&vals[0]));
        let username = format!("{}", helpers::mysql_val_to_string(&vals[1]));
        let karma = format!("{}", helpers::mysql_val_to_string(&vals[2]));
        results.push((id, username, karma));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("1".to_string(), "hello_1".to_string(), "10".to_string()));
    assert_eq!(results[1], ("2".to_string(), "hello_2".to_string(), "0".to_string()));

    //let mut results = vec![];
    let res = db.query_iter(r"SELECT user_id, url FROM stories ORDER BY id").unwrap();
    let mut results = vec![];
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        results.push(vals);
    }
    assert_eq!(results.len(), 6);

    // drop db
    drop(db);
    
    // reinitialize
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    restore_db("test_reinit_noop", policies::noop_policy(), &mut db);
 
    // check that all data returns correctly
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM users WHERE users.username='hello_1' OR users.username='hello_2' ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 3);
        let id = format!("{}", helpers::mysql_val_to_string(&vals[0]));
        let username = format!("{}", helpers::mysql_val_to_string(&vals[1]));
        let karma = format!("{}", helpers::mysql_val_to_string(&vals[2]));
        results.push((id, username, karma));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("1".to_string(), "hello_1".to_string(), "10".to_string()));
    assert_eq!(results[1], ("2".to_string(), "hello_2".to_string(), "0".to_string()));

    //let mut results = vec![];
    let mut results = vec![];
    let res = db.query_iter(r"SELECT user_id, url FROM stories ORDER BY id").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        results.push(vals);
    }
    assert_eq!(results.len(), 6);
}
