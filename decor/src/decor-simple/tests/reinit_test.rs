extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use decor_simple::{ghosts::GhostEidMapping, EntityData, helpers};
use std::collections::{HashSet};
use std::str::FromStr;
mod policies;

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

fn init_dbs(name: &'static str, policy: policies::PolicyType, db: &mut mysql::Conn, db_actual: &mut mysql::Conn) {
    init_logger();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let _jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let app_policy = match policy {
                policies::PolicyType::Noop => policies::noop_policy(),
                policies::PolicyType::Combined => policies::combined_policy(),
            };

            decor_simple::Shim::run_on_tcp(
                    name, SCHEMA, app_policy, 
                    decor_simple::TestParams{
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

fn restore_db(name: &'static str, policy: policies::PolicyType, db: &mut mysql::Conn) {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let _jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let app_policy = match policy {
                policies::PolicyType::Noop => policies::noop_policy(),
                policies::PolicyType::Combined => policies::combined_policy(),
            };

            decor_simple::Shim::run_on_tcp(
                    name, SCHEMA, app_policy, 
                    decor_simple::TestParams{
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
    init_dbs("test_reinit_noop", policies::PolicyType::Noop, &mut db, &mut db_actual);

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
    restore_db("test_reinit_noop", policies::PolicyType::Noop, &mut db);
 
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
    let res = db.query_iter(r"SELECT stories.user_id, stories.url FROM stories 
                            JOIN users ON users.id = stories.user_id 
                            WHERE users.id = 1 
                            ORDER BY stories.id").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        results.push(vals);
    }
    assert_eq!(results.len(), 3);
}

#[test]
fn test_init_complex() {
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    let mut db_actual: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    init_dbs("test_reinit_complex", policies::PolicyType::Combined, &mut db, &mut db_actual);

    // one more update just for fun
    db.query_drop(r"UPDATE users SET karma=10 WHERE username='hello_1'").unwrap();

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
    assert_eq!(results.len(), 4);

    // drop db
    drop(db);
    
    // reinitialize
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    restore_db("test_reinit_complex", policies::PolicyType::Combined, &mut db);
 
    // check that all data returns correctly
    let mod1 : (String, String, String, String, String) = 
        ("1".to_string(), 
         "1".to_string(), 
         "1".to_string(), 
         "2".to_string(), 
         "bad story!".to_string());
    let mod2 : (String, String, String, String, String) = 
        ("2".to_string(), 
         "2".to_string(), 
         "2".to_string(), 
         "1".to_string(), 
         "worst story!".to_string());
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], mod2);
    assert_eq!(results[1], mod1);

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

    let mut results = vec![];
    let res = db.query_iter(r"SELECT stories.user_id, stories.url FROM stories 
                            JOIN users ON users.id = stories.user_id 
                            WHERE users.id = 1 
                            ORDER BY stories.id").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        results.push(vals);
    }
    assert_eq!(results.len(), 2);

    // unsubscribe
    let mut gidshardstr : String = String::new(); 
    let mut entitydatastr : String = String::new();
    let mut unsubscribed_gids : Vec<GhostEidMapping> = vec![];
    let mut entity_data : Vec<EntityData> = vec![];
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", 1)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        gidshardstr = s1.trim_end_matches('\'').trim_start_matches('\'').to_string();
        entitydatastr = s2.trim_end_matches('\'').trim_start_matches('\'').to_string();
        unsubscribed_gids = serde_json::from_str(&gidshardstr).unwrap();
        entity_data = serde_json::from_str(&entitydatastr).unwrap();
    }
    for mapping in &unsubscribed_gids {
        if mapping.table == "users" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
        } else if mapping.table == "stories" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
        } else if mapping.table == "moderations" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 2);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 2);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 4);
        } else {
            assert!(false, "bad table! {}", mapping.table);
        }
    }
    assert_eq!(unsubscribed_gids.len(), 8);
    assert_eq!(entity_data.len(), 3);
    assert_eq!(entity_data[0], 
               EntityData{
                    table: "stories".to_string(),
                    eid: 0,
                    row_strs: vec!["1".to_string(), "1".to_string(), "'google.com'".to_string(), "0".to_string()],
               });
    assert_eq!(entity_data[1], 
               EntityData{
                    table: "stories".to_string(),
                    eid: 0,
                    row_strs: vec!["2".to_string(), "1".to_string(), "'bing.com'".to_string(), "0".to_string()],
               });
    assert_eq!(entity_data[2], 
               EntityData{
                    table: "users".to_string(), 
                    eid: 0,
                    row_strs: vec!["1".to_string(), "'hello_1'".to_string(), "10".to_string()],
                });


    // drop db
    drop(db);
 
    // reinitialize
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    restore_db("test_reinit_complex", policies::PolicyType::Combined, &mut db);

    // check unsubscribed restored state
    let mut results = HashSet::new();
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.insert((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 6);
    assert_eq!(results.iter().filter(|r| r.0 == "2".to_string() && r.1 == "2".to_string() && r.4 == "worst story!".to_string()
                                  && u64::from_str(&r.2).unwrap() >= GHOST_ID_START && u64::from_str(&r.3).unwrap() >= GHOST_ID_START).count(), 1);
    assert_eq!(results.iter().filter(|r| r.0 == "1".to_string() && r.3 == "2".to_string() && r.4 == "bad story!".to_string()
                                  && u64::from_str(&r.1).unwrap() >= GHOST_ID_START && u64::from_str(&r.2).unwrap() >= GHOST_ID_START).count(), 1);
    
    // users modified appropriately: ghosts added to users 
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = helpers::mysql_val_to_string(&vals[0]);
        results.push(uid);
    }
    assert_eq!(results.len(), 15);
    assert_eq!(results[0], format!("{}", 2));
    assert_eq!(results[1], format!("{}", GHOST_ID_START));
    assert_eq!(results[2], format!("{}", GHOST_ID_START+1));
  
    // resubscribe
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", 1, 
                            helpers::escape_quotes_mysql(&gidshardstr),
                            helpers::escape_quotes_mysql(&entitydatastr))).unwrap();
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_string(&vals[0]);
        let mod_id = helpers::mysql_val_to_string(&vals[1]);
        let story_id = helpers::mysql_val_to_string(&vals[2]);
        let user_id = helpers::mysql_val_to_string(&vals[3]);
        let action = helpers::mysql_val_to_string(&vals[4]);
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("1".to_string(), 
                            format!("{}", 1), 
                            "1".to_string(), 
                            "2".to_string(), 
                            "bad story!".to_string()));
    assert_eq!(results[1], ("2".to_string(), 
                            "2".to_string(), 
                            "2".to_string(), 
                            format!("{}", 1), 
                            "worst story!".to_string()));
}
