// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use std::collections::HashMap;
use log::warn;
use decor_mem::policy::{KeyRelationship, GhostColumnPolicy, GeneratePolicy, DecorrelationPolicy::Decor, ApplicationPolicy};

const SCHEMA : &'static str = include_str!("./schema.sql");
const GHOST_ID_START : u64 = 1<<20;

fn mysql_val_to_parser_val(val: &mysql::Value) -> sql_parser::ast::Value {
    match val {
        mysql::Value::NULL => sql_parser::ast::Value::Null,
        mysql::Value::Bytes(bs) => {
            let res = str::from_utf8(&bs);
            match res {
                Err(_) => sql_parser::ast::Value::String(String::new()),
                Ok(s) => sql_parser::ast::Value::String(s.to_string()),
            }
        }
        mysql::Value::Int(i) => sql_parser::ast::Value::Number(format!("{}", i)),
        mysql::Value::UInt(i) => sql_parser::ast::Value::Number(format!("{}", i)),
        mysql::Value::Float(f) => sql_parser::ast::Value::Number(format!("{}", f)),
        _ => unimplemented!("No sqlparser support for dates yet?")
        /*mysql::Date(u16, u8, u8, u8, u8, u8, u32),
        mysql::Time(bool, u32, u8, u8, u8, u32),8*/
    }
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn init_policy() -> ApplicationPolicy<'static> {
    let mut ghost_policies = HashMap::new();
    let mut users_map = HashMap::new();
    users_map.insert("id", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma", GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users", users_map);
    
    ApplicationPolicy{
        entity_type_to_decorrelate: "users",
        ghost_policies: ghost_policies, 
        edge_policies: vec![
            KeyRelationship{
                child: "moderations",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "moderations",
                parent: "users",
                column_name: "moderator_user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "stories",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            }
        ]
    }
}

#[test]
fn test_normal_execution() {
    init_logger();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            decor_mem::Shim::run_on_tcp(
                    "gdpr_normal", SCHEMA, init_policy(),
                    decor_mem::TestParams{
                        testname: "test_normal".to_string(), 
                        translate:true, parse:true, in_memory: true}, s).unwrap();
        }
    });

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db("gdpr_normal"), true);

    let mut db_actual = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
    assert_eq!(db_actual.ping(), true);
    assert_eq!(db_actual.select_db("gdpr_normal"), true);

    /*
     * NOTE: the column types are all right, but the mysql value returned is always Bytes,
     * so it always parses as a String
     */

    /* 
     * TEST 1: all tables successfully created 
     */
    let mut results = vec![];
    let res = db_actual.query_iter("SHOW tables;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        let name = format!("{}", mysql_val_to_parser_val(&vals[0]));
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
    ];
    assert_eq!(results.len(), tables.len());
    for tab in results {
        assert!(tables.iter().any(|tt| &tab == *tt));
    }

    /*
     * TEST 2: insert users works properly
     */
    let mut results = vec![];
    db.query_drop(r"INSERT INTO users (username) VALUES ('hello_1'), ('hello_2');").unwrap();
    let res = db.query_iter(r"SELECT * FROM users WHERE users.username='hello_1' OR users.username='hello_2' ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 3);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let username = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let karma = format!("{}", mysql_val_to_parser_val(&vals[2]));
        results.push((id, username, karma));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("'1'".to_string(), "'hello_1'".to_string(), "'0'".to_string()));
    assert_eq!(results[1], ("'2'".to_string(), "'hello_2'".to_string(), "'0'".to_string()));

    //  No ghost entries added
    let mut results = vec![];
    let res = db_actual.query_iter(r"SELECT COUNT(*) FROM ghostusers;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let count = format!("{}", mysql_val_to_parser_val(&vals[0]));
        results.push(count);
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "'0'".to_string());

    /*
     * TEST 3: insert into datatables works properly
     */
    let mut results = vec![];
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (1, 0, 2, 'bad story!');").unwrap();
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 
               ("'1'".to_string(), 
                "'1'".to_string(), 
                "'0'".to_string(), 
                "'2'".to_string(), 
                "'bad story!'".to_string()));

    // two ghost entries added, beginning from GHOST_ID_START
    let mut results = vec![];
    let res = db_actual.query_iter(r"SELECT * FROM ghostusers ORDER BY ghostusers.ghost_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let uid = format!("{}", mysql_val_to_parser_val(&vals[1]));
        results.push((gid, uid));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (format!("'{}'", GHOST_ID_START), "'1'".to_string()));
    assert_eq!(results[1], (format!("'{}'", GHOST_ID_START+1), "'2'".to_string()));

    /*
     * TEST 4: complex insert into datatables works properly
     */
    let mut results = vec![];
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (2, 0, 1, 'worst story!');").unwrap();
    //((SELECT id FROM users WHERE username='hello_2'), '0', '1', 'worst story!');").unwrap();
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("'1'".to_string(), "'1'".to_string(), "'0'".to_string(), "'2'".to_string(), "'bad story!'".to_string()));
    assert_eq!(results[1], ("'2'".to_string(), "'2'".to_string(), "'0'".to_string(), "'1'".to_string(), "'worst story!'".to_string()));

    // two ghost entries added, beginning from GHOST_ID_START
    let mut results = vec![];
    let res = db_actual.query_iter(r"SELECT * FROM ghostusers;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let uid = format!("{}", mysql_val_to_parser_val(&vals[1]));
        results.push((gid, uid));
    }
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], (format!("'{}'", GHOST_ID_START), "'1'".to_string()));
    assert_eq!(results[1], (format!("'{}'", GHOST_ID_START+1), "'2'".to_string()));
    assert_eq!(results[2], (format!("'{}'", GHOST_ID_START+2), "'2'".to_string()));
    assert_eq!(results[3], (format!("'{}'", GHOST_ID_START+3), "'1'".to_string()));

    /* 
     * TEST 5: complex joins
     */
    let mut results = vec![];
    let res = db.query_iter(r"SELECT moderations.moderator_user_id, users.username 
                            FROM users JOIN moderations ON users.id = moderations.user_id 
                            ORDER BY moderations.user_id ASC 
                            LIMIT 2;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let username = format!("{}", mysql_val_to_parser_val(&vals[1]));
        results.push((mod_id, username));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("'2'".to_string(), "'hello_1'".to_string()));
    assert_eq!(results[1], ("'1'".to_string(), "'hello_2'".to_string()));

    /* 
     * TEST 6: update correctly changes ghost values to point to new UIDs (correctly handling
     * deletion upon updates to NULL)
     */
    let mut results = vec![];
    db.query_drop(r"UPDATE moderations SET user_id = NULL, story_id = 1, moderator_user_id = 3 WHERE moderations.user_id=1;").unwrap();
    let res = db.query_iter(r"SELECT * FROM moderations WHERE moderations.moderator_user_id =3;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], ("'2'".to_string(), "'3'".to_string(), "'1'".to_string(), "NULL".to_string(), "'worst story!'".to_string()));

    // latest ghost entry removed (user was set to NULL)
    let mut results = vec![];
    let res = db_actual.query_iter(r"SELECT * FROM ghostusers;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let uid = format!("{}", mysql_val_to_parser_val(&vals[1]));
        results.push((gid, uid));
    }
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (format!("'{}'", GHOST_ID_START), "'1'".to_string()));
    assert_eq!(results[1], (format!("'{}'", GHOST_ID_START+1), "'2'".to_string()));
    assert_eq!(results[2], (format!("'{}'", GHOST_ID_START+2), "'3'".to_string()));

    /* 
     * TEST 7: deletions correctly remove ghost IDs
     */
    let mut results = vec![];
    db.query_drop(r"DELETE FROM moderations WHERE moderator_user_id = 1").unwrap(); 
    //(SELECT id FROM users WHERE username='hello_1');").unwrap();
    let res = db.query_iter(r"SELECT * FROM moderations;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], ("'2'".to_string(), "'3'".to_string(), "'1'".to_string(), "NULL".to_string(), "'worst story!'".to_string()));

    // first two ghost entries removed 
    let mut results = vec![];
    let res = db_actual.query_iter(r"SELECT * FROM ghostusers;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let uid = format!("{}", mysql_val_to_parser_val(&vals[1]));
        results.push((gid, uid));
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (format!("'{}'", GHOST_ID_START+2), "'3'".to_string()));
   
    drop(db);
    drop(db_actual);
    jh.join().unwrap();
}

#[test]
fn test_users() {
    init_logger();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let jh = thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        decor_mem::Shim::run_on_tcp(
                    "gdpr_users_test", SCHEMA, init_policy(),
                    decor_mem::TestParams{
                        testname: "test_users".to_string(), 
                        translate:true, parse:true, in_memory: true}, s).unwrap();
    });
    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db("gdpr_users_test"), true);

    /* 
     * Update with 2 users and 2 moderation entries (same as other tests)
     */
    db.query_drop(r"INSERT INTO users (username) VALUES ('hello_1'), ('hello_2');").unwrap();
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (1, 0, 2, 'bad story!');").unwrap();
    db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (2, 0, 1, 'worst story!');").unwrap();

    /* 
     *  Test 1: Unsubscribe of user 1 adds two ghost entries to user table, anonymizes both
     *  moderation entries
     */
    let mut results = vec![];
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", 1)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        results.push(gid);
    }
    assert_eq!(results.len(), 2);
    assert_eq!((results[0].clone(), results[1].clone()), (format!("'{}'", GHOST_ID_START), format!("'{}'", GHOST_ID_START+3)));

    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.user_id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    warn!("results: {:?}", results);
    assert!(((results[1] == ("'2'".to_string(), 
                            "'2'".to_string(), 
                            "'0'".to_string(), 
                            format!("'{}'", GHOST_ID_START+3), 
                            "'worst story!'".to_string()))
            && (results[0] == ("'1'".to_string(), 
                            format!("'{}'", GHOST_ID_START), 
                            "'0'".to_string(), 
                            "'2'".to_string(), 
                            "'bad story!'".to_string())))
        || ((results[1] == ("'2'".to_string(), 
                            "'2'".to_string(), 
                            "'0'".to_string(), 
                            format!("'{}'", GHOST_ID_START), 
                            "'worst story!'".to_string()))
            && (results[0] == ("'1'".to_string(), 
                            format!("'{}'", GHOST_ID_START+3), 
                            "'0'".to_string(), 
                            "'2'".to_string(), 
                            "'bad story!'".to_string()))));


    // users modified appropriately: ghosts added to users 
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY users.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        results.push(uid);
    }
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], format!("'{}'", 2));
    assert_eq!(results[1], format!("'{}'", GHOST_ID_START));
    assert_eq!(results[2], format!("'{}'", GHOST_ID_START+3));
  
    /* 
     *  Test 2: Resubscribe of user 1 adds uid to user table, removes gids from user table, 
     *  unanonymizes both moderation entries
     */
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS ({}, {});", 1, GHOST_ID_START, GHOST_ID_START+3)).unwrap();
    let mut results = vec![];
    let res = db.query_iter(r"SELECT * FROM moderations ORDER BY moderations.id;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 5);
        let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
        let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
        let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
        let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
        let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
        results.push((id, mod_id, story_id, user_id, action));
    }
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("'1'".to_string(), 
                            format!("'{}'", 1), 
                            "'0'".to_string(), 
                            "'2'".to_string(), 
                            "'bad story!'".to_string()));
    assert_eq!(results[1], ("'2'".to_string(), 
                            "'2'".to_string(), 
                            "'0'".to_string(), 
                            format!("'{}'", 1), 
                            "'worst story!'".to_string()));

    // users are restored in users
    let mut results = vec![];
    let res = db.query_iter(r"SELECT id FROM users ORDER BY id ASC;").unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let uid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        results.push(uid);
    }
    warn!("Got results {:?}", results);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], format!("'{}'", 1));
    assert_eq!(results[1], format!("'{}'", 2));

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
        let uid = format!("{}", mysql_val_to_parser_val(&vals[0]));
        results.push(uid);
    }
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], format!("'{}'", 2));
    assert_eq!(results[1], format!("'{}'", 10));
    assert_eq!(results[2], format!("'{}'", 11));
 
    drop(db);
    jh.join().unwrap();
}
