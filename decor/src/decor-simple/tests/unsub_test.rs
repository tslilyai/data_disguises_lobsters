extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use log::warn;
use decor_simple::{ghosts::GhostEidMapping, EntityData, helpers};
use std::collections::{HashSet};
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

    /* 
     *  Unsubscribe of user 1 does nothing
     */
    let mut uid = 0; 
    let mut unsubscribed_gids : Vec<GhostEidMapping>; 
    let mut entity_data : Vec<EntityData> = vec![]; 
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
        for ugid in &unsubscribed_gids{
            println!("User1 {:?}", ugid);
        }
        for entity in &entity_data { 
            println!("Entity! {:?}", entity);
        }
        assert_eq!(entity_data.len(), 5); // user, two stories, two moderations
        assert_eq!(unsubscribed_gids.len(), 5);
    }
    assert_eq!(entity_data[0], 
       EntityData{
            table: "moderations".to_string(),
            eid: 1,
            row_strs: vec!["1".to_string(), "1".to_string(), "1".to_string(), "2".to_string(), "'bad story!'".to_string()],
       });
    assert_eq!(entity_data[1], 
       EntityData{
            table: "moderations".to_string(),
            eid: 2,
            row_strs: vec!["2".to_string(), "2".to_string(), "2".to_string(), "1".to_string(), "'worst story!'".to_string()],
       });
    assert_eq!(entity_data[2], 
       EntityData{
            table: "stories".to_string(),
            eid: 1,
            row_strs: vec!["1".to_string(), "1".to_string(), "'google.com'".to_string(), "0".to_string()],
       });
    assert_eq!(entity_data[3], 
       EntityData{
            table: "stories".to_string(),
            eid: 2,
            row_strs: vec!["2".to_string(), "1".to_string(), "'bing.com'".to_string(), "0".to_string()],
       });
    assert_eq!(entity_data[4], 
       EntityData{
            table: "users".to_string(), 
            eid: 1,
            row_strs: vec!["1".to_string(), "'hello_1'".to_string(), "0".to_string()],
        });
    
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        let story_id = helpers::mysql_val_to_u64(&vals[2]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[3]).unwrap();
        assert!((user_id >= GHOST_ID_START && mod_id == 2)
            || (user_id == 2 && mod_id >= GHOST_ID_START));
        assert!(id >= GHOST_ID_START);
        assert!((story_id > 2 && story_id < GHOST_ID_START) || story_id >= GHOST_ID_START);
        results.push((id, mod_id, story_id, user_id));
    }
    assert_eq!(results.len(), 2);
  
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
        for ugid in &unsubscribed_gids{
            println!("User2 {:?}", ugid);
        }
        for entity in &entity_data {
            println!("Entity! {:?}", entity);
        }
        // note: we don't ghost entities twice, so we're only going to see user + two stories
        assert_eq!(entity_data.len(), 3);
        assert_eq!(unsubscribed_gids.len(), 3);
        assert_eq!(entity_data[0], 
           EntityData{
                table: "stories".to_string(),
                eid: 3,
                row_strs: vec!["3".to_string(), "2".to_string(), "'reddit.com'".to_string(), "0".to_string()],
           });
        assert_eq!(entity_data[1], 
           EntityData{
                table: "stories".to_string(),
                eid: 4,
                row_strs: vec!["4".to_string(), "2".to_string(), "'fb.com'".to_string(), "0".to_string()],
           });
        assert_eq!(entity_data[2], 
           EntityData{
                table: "users".to_string(), 
                eid: 2,
                row_strs: vec!["2".to_string(), "'hello_2'".to_string(), "0".to_string()],
            });
    }
    
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        let story_id = helpers::mysql_val_to_u64(&vals[2]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[3]).unwrap();
        assert!(user_id >= GHOST_ID_START && mod_id >= GHOST_ID_START);
        assert!(id >= GHOST_ID_START);
        assert!(story_id >= GHOST_ID_START);
        results.push((id, mod_id, story_id, user_id));
    }
    assert_eq!(results.len(), 2);
  
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
fn test_unsub_complex() {
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
    let mut gidshardstr1 : String = String::new(); 
    let mut entitydatastr1 : String = String::new();
    let mut gidshardstr2 : String = String::new(); 
    let mut entitydatastr2 : String = String::new();

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
        gidshardstr1 = s1.trim_end_matches('\'').trim_start_matches('\'').to_string();
        entitydatastr1 = s2.trim_end_matches('\'').trim_start_matches('\'').to_string();
        unsubscribed_gids = serde_json::from_str(&gidshardstr1).unwrap();
        entity_data = serde_json::from_str(&entitydatastr1).unwrap();
    }
    for ugid in &unsubscribed_gids{
        println!("User1 {:?}", ugid);
    }
    for entity in &entity_data {
        println!("User1 {:?}", entity);
    }
    for mapping in &unsubscribed_gids {
        if mapping.table == "users" {
            // four ghosts for two stories, two moderations
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 4);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
            user_counts += 1;
        } else if mapping.table == "stories" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
            story_counts += 1;
        } else if mapping.table == "moderations" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 0);
            mod_counts += 1;
        } else {
            assert!(false, "bad table! {}", mapping.table);
        }
    }
    assert_eq!(unsubscribed_gids.len(), 5);
    assert_eq!(user_counts, 1); 
    assert_eq!(story_counts, 2); 
    assert_eq!(mod_counts, 2); 
    assert_eq!(entity_data.len(), 5); // one user, two stories, two moderations
    assert_eq!(entity_data[0], 
       EntityData{
            table: "moderations".to_string(),
            eid: 1,
            row_strs: vec!["1".to_string(), "1".to_string(), "2".to_string(), "2".to_string(), "'bad story!'".to_string()],
       });
    assert_eq!(entity_data[1], 
       EntityData{
            table: "moderations".to_string(),
            eid: 2,
            row_strs: vec!["2".to_string(), "2".to_string(), "1".to_string(), "1".to_string(), "'worst story!'".to_string()],
       });
    assert_eq!(entity_data[2], 
       EntityData{
            table: "stories".to_string(),
            eid: 1,
            row_strs: vec!["1".to_string(), "1".to_string(), "'google.com'".to_string(), "0".to_string()],
       });
    assert_eq!(entity_data[3], 
       EntityData{
            table: "stories".to_string(),
            eid: 2,
            row_strs: vec!["2".to_string(), "1".to_string(), "'bing.com'".to_string(), "0".to_string()],
       });
    assert_eq!(entity_data[4], 
       EntityData{
            table: "users".to_string(), 
            eid: 1,
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
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        let story_id = helpers::mysql_val_to_u64(&vals[2]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[3]).unwrap();
        assert!((user_id >= GHOST_ID_START && mod_id == 2)
            || (user_id == 2 && mod_id >= GHOST_ID_START));
        assert!(id >= GHOST_ID_START);
        assert!((story_id > 2 && story_id < GHOST_ID_START) || story_id >= GHOST_ID_START);
        results.insert((id, mod_id, story_id, user_id));
    }
    warn!("Moderations returned {:?}", results);
    assert_eq!(results.len(), 2);
   
    // check stories
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM stories ORDER BY stories.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(id >= GHOST_ID_START || (id > 2 && id < GHOST_ID_START));
        results.push(id)
    }
    assert_eq!(results.len(), 4);

    // users modified appropriately: ghosts added to users 
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(uid >= GHOST_ID_START || uid == 2);
        results.push(uid);
    }
    warn!("Users returned {:?}", results);
    assert_eq!(results.len(), 5); // two stories, two moderations, one real
  
    /*
     * Test 1.5: Unsubscribe User 2
     */
    let mut uid = 0; 
    let res = db.query_iter(r"SELECT id FROM users WHERE username = 'hello_2';").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", uid)).unwrap();
    user_counts = 0;
    story_counts = 0;
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        gidshardstr2 = s1.trim_end_matches('\'').trim_start_matches('\'').to_string();
        entitydatastr2 = s2.trim_end_matches('\'').trim_start_matches('\'').to_string();
        unsubscribed_gids = serde_json::from_str(&gidshardstr2).unwrap();
        entity_data = serde_json::from_str(&entitydatastr2).unwrap();
    }
    for ugid in &unsubscribed_gids{
        println!("User2 {:?}", ugid);
    }
    for entity in &entity_data {
        println!("User2 {:?}", entity);
    }
    for mapping in &unsubscribed_gids {
        if mapping.table == "users" {
            // four ghosts for two stories, two moderations
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 4);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
            user_counts += 1;
        } else if mapping.table == "stories" {
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "stories").count(), 1);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "users").count(), 0);
            assert_eq!(mapping.ghosts.iter().filter(|(tab, _gid)| tab == "moderations").count(), 0);
            story_counts += 1;
        } else if mapping.table == "moderations" {
            assert!(false, "moderations should all be ghosts!");
        } else {
            assert!(false, "bad table! {}", mapping.table);
        }
    }
    assert_eq!(unsubscribed_gids.len(), 3);
    assert_eq!(user_counts, 1); 
    assert_eq!(story_counts, 2); 
    assert_eq!(entity_data.len(), 3); // one user, two stories
    assert_eq!(entity_data[0], 
       EntityData{
            table: "stories".to_string(),
            eid: 3,
            row_strs: vec!["3".to_string(), "1".to_string(), "'reddit.com'".to_string(), "0".to_string()],
       });
    assert_eq!(entity_data[1], 
       EntityData{
            table: "stories".to_string(),
            eid: 4,
            row_strs: vec!["4".to_string(), "1".to_string(), "'fb.com'".to_string(), "0".to_string()],
       });
    assert_eq!(entity_data[2], 
       EntityData{
            table: "users".to_string(), 
            eid: 2,
            row_strs: vec!["2".to_string(), "'hello_2'".to_string(), "0".to_string()],
        });
   
    /*
     * Check that the moderations have one both ghost parents
     */
    let mut results = HashSet::new();
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let mod_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        let story_id = helpers::mysql_val_to_u64(&vals[2]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[3]).unwrap();
        assert!(user_id >= GHOST_ID_START && mod_id >= GHOST_ID_START);
        assert!(id >= GHOST_ID_START);
        assert!(story_id >= GHOST_ID_START);
        results.insert((id, mod_id, story_id, user_id));
    }
    warn!("Moderations returned {:?}", results);
    assert_eq!(results.len(), 2);

    // check stories
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM stories ORDER BY stories.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(id >= GHOST_ID_START);
        results.push(id)
    }
    assert_eq!(results.len(), 4);
    
    // users modified appropriately: ghosts added to users 
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(uid >= GHOST_ID_START);
        results.push(uid);
    }
    warn!("Users returned {:?}", results);
    assert_eq!(results.len(), 8); // for each user, we have two stories + two moderations ghost parents

    /* 
     *  Test 2: Resubscribe of user 1 and 2 adds uid to user table, removes gids from user table, 
     *  unanonymizes stories and moderation entries (back to initial state)
     */
    warn!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", 1, 
          helpers::escape_quotes_mysql(&gidshardstr1), 
          helpers::escape_quotes_mysql(&entitydatastr1));
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", 1, 
                            helpers::escape_quotes_mysql(&gidshardstr1),
                            helpers::escape_quotes_mysql(&entitydatastr1))).unwrap();
    warn!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", 2, 
          helpers::escape_quotes_mysql(&gidshardstr2), 
          helpers::escape_quotes_mysql(&entitydatastr2));
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", 2, 
                            helpers::escape_quotes_mysql(&gidshardstr2),
                            helpers::escape_quotes_mysql(&entitydatastr2))).unwrap();

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

    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM stories ORDER BY stories.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        results.push(id)
    }
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], 1);
    assert_eq!(results[1], 2);
    assert_eq!(results[2], 3);
    assert_eq!(results[3], 4);

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
        let uid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        results.push(uid);
    }
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], 2); 
    assert_eq!(results[1], 10); 
    assert_eq!(results[2], 11); 
    drop(db);
    //jh.join().unwrap();
}
