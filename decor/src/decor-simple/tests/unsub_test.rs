extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use log::warn;
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

#[test]
fn test_unsub_noop() {
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    let mut db_actual: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    init_dbs("test_unsub_noop", policies::PolicyType::Noop, &mut db, &mut db_actual);
    
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

    /* 
     *  Unsubscribe of user 1 does nothing
     */
    let mut uid = 0; 
    let mut unsubscribed_gids : Vec<GhostEidMapping>; 
    let mut entity_data : Vec<EntityData>; 
    let res = db.query_iter(r"SELECT id FROM users WHERE username = 'hello_1';").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", uid)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        let s1 = s1.trim_end_matches('\'').trim_start_matches('\'');
        let s2 = s2.trim_end_matches('\'').trim_start_matches('\'');
        unsubscribed_gids = serde_json::from_str(s1).unwrap();
        entity_data = serde_json::from_str(s2).unwrap();
        for entity in &entity_data {
            println!("Entity! {:?}", entity);
        }
        assert_eq!(entity_data.len(), 6);
        assert_eq!(unsubscribed_gids.len(), 6);
    }
    
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
    //assert_eq!(results[0], mod2);
    //assert_eq!(results[1], mod1);
  
    /* 
     *  Unsubscribe of user 2 does nothing
     */
    let res = db.query_iter(r"SELECT id FROM users WHERE username = 'hello_2';").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", uid)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        let s1 = s1.trim_end_matches('\'').trim_start_matches('\'');
        let s2 = s2.trim_end_matches('\'').trim_start_matches('\'');
        unsubscribed_gids = serde_json::from_str(s1).unwrap();
        entity_data = serde_json::from_str(s2).unwrap();
        for entity in &entity_data {
            println!("Entity! {:?}", entity);
        }
        // note: we don't ghost entities twice
        assert_eq!(entity_data.len(), 2);
        assert_eq!(unsubscribed_gids.len(), 2);
    }
    
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
    //assert_eq!(results[0], mod2);
    //assert_eq!(results[1], mod1);
  
    /*
     * Check users for the heck of it
     */
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY id ASC;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        results.push(uid);
    }
    assert_eq!(results.len(), 2);
    assert!(results[0] >= GHOST_ID_START);
    assert!(results[1] >= GHOST_ID_START);

    drop(db);
    //jh.join().unwrap();
}

#[test]
fn test_complex() {
    let mut db: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    let mut db_actual: mysql::Conn = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    init_dbs("test_complex", policies::PolicyType::Combined, &mut db, &mut db_actual);

    /* 
     *  Test 1: Unsubscribe of user 1 
     *  GhostMappingShard:
     *      4 GIDs returned for user 1, 0 ancestors
     *      1 GID returned for story 1, 1 ghost user ancestor
     *      1 GID returned for story 2, 1 ghost user ancestor
     *      Created: for each real moderation, generate 2 moderations, 2 stories, 6 ghost user entities (2 for stories, 2 for moderations)
     *  EntityData:
     *      User 1
     *      Story 1
     *      Story 2
     */
    let mut gidshardstr : String = String::new(); 
    let mut entitydatastr : String = String::new();
    let mut unsubscribed_gids : Vec<GhostEidMapping> = vec![];
    let mut entity_data : Vec<EntityData> = vec![];
    let mut user_counts = 0;
    let mut story_counts = 0;
    let mut mod_counts = 0;
    
    let mut uid = 0; 
    let res = db.query_iter(r"SELECT id FROM users WHERE username = 'hello_1';").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", uid)).unwrap();
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
    warn!("user 1 gidshard: {:?}", unsubscribed_gids);
    warn!("user 1 entity data: {:?}", entity_data);
    for mapping in &unsubscribed_gids {
        if mapping.table == "users" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
            user_counts += 1;
        } else if mapping.table == "stories" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
            story_counts += 1;
        } else if mapping.table == "moderations" {
            warn!("Mapping for moderations table: {:?}", mapping);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 2);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 2);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 4);
            mod_counts += 1;
        } else {
            assert!(false, "bad table! {}", mapping.table);
        }
    }
    assert_eq!(unsubscribed_gids.len(), 8);
    assert_eq!(user_counts, 4);
    assert_eq!(story_counts, 2); // decorrelated two stories
    assert_eq!(mod_counts, 2); // generated two moderations
    assert_eq!(entity_data.len(), 3);
    assert_eq!(entity_data[0], 
               EntityData{
                    table: "stories".to_string(),
                    row_strs: vec!["1".to_string(), "1".to_string(), "'google.com'".to_string(), "0".to_string()],
               });
    assert_eq!(entity_data[1], 
               EntityData{
                    table: "stories".to_string(),
                    row_strs: vec!["2".to_string(), "1".to_string(), "'bing.com'".to_string(), "0".to_string()],
               });
    assert_eq!(entity_data[2], 
               EntityData{
                    table: "users".to_string(), 
                    row_strs: vec!["1".to_string(), "'hello_1'".to_string(), "0".to_string()],
                });
   
    /*
     * Check that two of the moderations still in the data table have one real user parent, and
     * stories and other user which are ghosts
     */
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
    warn!("Moderations returned {:?}", results);
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
    warn!("Users returned {:?}", results);
    assert_eq!(results.len(), 15);
    assert_eq!(results[0], format!("{}", 2));
    assert_eq!(results[1], format!("{}", GHOST_ID_START));
    assert_eq!(results[2], format!("{}", GHOST_ID_START+1));
  
    /* 
     *  Test 2: Resubscribe of user 1 adds uid to user table, removes gids from user table, 
     *  unanonymizes both moderation entries
     */
    warn!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", 1, 
          helpers::escape_quotes_mysql(&gidshardstr), 
          helpers::escape_quotes_mysql(&entitydatastr));
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

    // users are restored in users
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY id ASC;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = format!("{}", helpers::mysql_val_to_string(&vals[0]));
        results.push(uid);
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], format!("{}", 1));
    assert_eq!(results[1], format!("{}", 2));

    /*
     * Test 3: Updating userID value updates autoinc appropriately
     */
    db.query_drop("UPDATE users SET id=10 WHERE id=1;").unwrap();
    db.query_drop("INSERT INTO users (username) VALUES ('hello_3');").unwrap();

    // ghosts added to users
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY id ASC;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = helpers::mysql_val_to_string(&vals[0]);
        results.push(uid);
    }
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], format!("{}", 2));
    assert_eq!(results[1], format!("{}", 10));
    assert_eq!(results[2], format!("{}", 11));
    drop(db);
    //jh.join().unwrap();
}
