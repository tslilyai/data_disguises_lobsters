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
use decor;

const SCHEMA : &'static str = include_str!("./schema_lobsters.sql");
const CONFIG : &'static str = include_str!("./config.json");
const NUM_USERS: usize = 10;
const NUM_STORIES : usize = 10;
const NUM_COMMENTS: usize = 10;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Debug)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn init_database(db: &mut mysql::Conn) {
    let mut user_ids = String::new();
    for user in 0..NUM_USERS {
        if user != 0 {
            user_ids.push_str(",");
        }
        user_ids.push_str(&format!("('user{}')", user));
    }
    db.query_drop(&format!("INSERT INTO users (username) VALUES {};", user_ids)).unwrap();
    
    let mut story_vals = String::new();
    for i in 0..NUM_STORIES {
        if i != 0 {
            story_vals.push_str(",");
        }
        story_vals.push_str(&format!("({}, {}, 'story{}')", i, i, i));
    }
    db.query_drop(&format!("INSERT INTO stories (user_id, short_id, title) VALUES {};", story_vals)).unwrap();

    let mut comment_vals = String::new();
    for i in 0..NUM_COMMENTS {
        if i != 0 {
            comment_vals.push_str(",");
        }
        comment_vals.push_str(&format!("({}, {}, '{}:{}', 'comment{}', {})", i, i % NUM_STORIES, "2004-05-23T14:25:", i, i, i));
    }
    db.query_drop(&format!("INSERT INTO comments (user_id, story_id, created_at, comment, short_id) VALUES {};", comment_vals)).unwrap();
}

fn main() {
    init_logger();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
            db.query_drop("DROP DATABASE IF EXISTS decor_lobsters;").unwrap();
            db.query_drop("CREATE DATABASE decor_lobsters;").unwrap();
            assert_eq!(db.ping(), true);
            decor::Shim::run_on_tcp(db, CONFIG, SCHEMA, s).unwrap();
        }
    });

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db("decor_lobsters"), true);
    init_database(&mut db);

    drop(db);
    jh.join().unwrap();
}
