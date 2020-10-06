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
extern crate rand;

use mysql::prelude::*;
use rand::prelude::*;
use std::sync::{Arc, Barrier};
use std::*;
use structopt::StructOpt;
//use log::{warn, debug};
use decor;

const SCHEMA : &'static str = include_str!("./schema_lobsters.sql");
const CONFIG : &'static str = include_str!("./config.json");

#[derive(Debug, Clone, PartialEq)]
enum TestType {
    TestDecor, 
    TestShimParse, 
    TestShim, 
    TestNoShim, 
}
impl std::str::FromStr for TestType {
    type Err = std::io::Error;
    fn from_str(test: &str) -> Result<Self, Self::Err> {
        match test{
            "decor" => Ok(TestType::TestDecor),
            "shim_parse" => Ok(TestType::TestShimParse),
            "shim_only" => Ok(TestType::TestShim),
            "no_shim" => Ok(TestType::TestNoShim),
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, test)),
        }
    }
}

#[derive(StructOpt)]
struct Cli {
    #[structopt(long="test", default_value="no_shim")]
    test: TestType,
    #[structopt(long="num_users", default_value="10")]
    num_users: usize,
    #[structopt(long="num_stories", default_value="10")]
    num_stories: usize,
    #[structopt(long="num_comments", default_value="100")]
    num_comments: usize,
    #[structopt(long="num_threads", default_value = "1")]
    num_threads: usize,
    #[structopt(long="num_queries", default_value = "100")]
    num_queries: usize,
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

fn init_database(db: &mut mysql::Conn, nusers: usize, nstories: usize, ncomments: usize) {
    // users
    let mut user_ids = String::new();
    for user in 0..nusers {
        if user != 0 {
            user_ids.push_str(",");
        }
        user_ids.push_str(&format!("({}, 'user{}')", user+1, user));
    }
    db.query_drop(&format!(
            "INSERT INTO users (id, username) VALUES {};", 
            user_ids)).unwrap();
    
    // stories
    let mut story_vals = String::new();
    for i in 0..nstories {
        if i != 0 {
            story_vals.push_str(",");
        }
        story_vals.push_str(&format!("({}, {}, 'story{}')", i % nusers , i, i));
    }
    db.query_drop(&format!(
            "INSERT INTO stories (user_id, short_id, title) VALUES {};", 
            story_vals)).unwrap();

    // comments
    let mut comment_vals = String::new();
    for i in 0..ncomments {
        if i != 0 {
            comment_vals.push_str(",");
        }
        comment_vals.push_str(&format!(
                "({}, {}, '{}', 'comment{}', {})", 
                i % nusers, i % nstories, "2004-05-23T14:25:00", i, i));
    }
    db.query_drop(&format!(
            "INSERT INTO comments (user_id, story_id, created_at, comment, short_id) VALUES {};",
            comment_vals)).unwrap();
}

fn create_schema(db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let mut txn = db.start_transaction(mysql::TxOpts::default())?;
    
    /* issue schema statements */
    let mut sql = String::new();
    let mut stmt = String::new();
    for line in SCHEMA.lines() {
        if line.starts_with("--") || line.is_empty() {
            continue;
        }
        if !sql.is_empty() {
            sql.push_str(" ");
            stmt.push_str(" ");
        }
        stmt.push_str(line);
        if stmt.ends_with(';') {
            txn.query_drop(stmt.to_string())?;
            stmt = String::new();
        }
    }
    txn.commit()?;
    Ok(())
}

fn test_reads(db: &mut mysql::Conn, nqueries: usize, nstories: usize) {
    // select comments and stories at random to read
    for _ in 0..nqueries {
        let story = thread_rng().gen_range(0, nstories);
        db.query_iter(&format!("SELECT story_id from stories \
            LEFT JOIN comments ON comments.story_id = stories.id \
            where stories.id = {}", story)).unwrap();
    }
}

fn main() {
    init_logger();
    let args = Cli::from_args();
    let test = args.test;
    let ncomments = args.num_comments;
    let nqueries = args.num_queries;
    let nstories = args.num_stories;
    let nusers = args.num_users;
    let nthreads = args.num_threads;

    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut jh = None;
    let mut db : mysql::Conn;
       
    match test {
        TestType::TestDecor => {
            jh = Some(thread::spawn(move || {
                if let Ok((s, _)) = listener.accept() {
                    let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
                    db.query_drop("DROP DATABASE IF EXISTS decor_lobsters;").unwrap();
                    db.query_drop("CREATE DATABASE decor_lobsters;").unwrap();
                    assert_eq!(db.ping(), true);
                    decor::Shim::run_on_tcp(
                        db, CONFIG, SCHEMA, 
                        decor::TestParams{translate:true, parse:true}, s).unwrap();
                }
            }));
            db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
            assert_eq!(db.ping(), true);
            assert_eq!(db.select_db("decor_lobsters"), true);
        }
        TestType::TestShimParse => {
            jh = Some(thread::spawn(move || {
                if let Ok((s, _)) = listener.accept() {
                    let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
                    db.query_drop("DROP DATABASE IF EXISTS decor_lobsters;").unwrap();
                    db.query_drop("CREATE DATABASE decor_lobsters;").unwrap();
                    assert_eq!(db.ping(), true);
                    decor::Shim::run_on_tcp(
                        db, CONFIG, SCHEMA, 
                        decor::TestParams{translate:false, parse:true}, s).unwrap();
                }
            }));
            db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
            assert_eq!(db.ping(), true);
            assert_eq!(db.select_db("decor_lobsters"), true);
        }
        TestType::TestShim => {
            jh = Some(thread::spawn(move || {
                if let Ok((s, _)) = listener.accept() {
                    let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
                    db.query_drop("DROP DATABASE IF EXISTS decor_lobsters;").unwrap();
                    db.query_drop("CREATE DATABASE decor_lobsters;").unwrap();
                    assert_eq!(db.ping(), true);
                    decor::Shim::run_on_tcp(
                        db, CONFIG, SCHEMA, 
                        decor::TestParams{translate:false, parse:false}, s).unwrap();
                }
            }));
            db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
            assert_eq!(db.ping(), true);
            assert_eq!(db.select_db("decor_lobsters"), true);
        }
        TestType::TestNoShim => {
            db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
            db.query_drop("DROP DATABASE IF EXISTS decor_lobsters;").unwrap();
            db.query_drop("CREATE DATABASE decor_lobsters;").unwrap();
            assert_eq!(db.ping(), true);
            assert_eq!(db.select_db("decor_lobsters"), true);
            create_schema(&mut db).unwrap();
        }
    }
    init_database(&mut db, nusers, nstories, ncomments);

    let mut test_threads = vec![];
    let barrier = Arc::new(Barrier::new(2));//NUM_THREADS + 1));
    //for _ in 0..NUM_THREADS {
    let c = barrier.clone();
    test_threads.push(thread::spawn(move || {
        c.wait();
        test_reads(&mut db, nqueries/ nthreads, nstories);
        drop(db);
    }));
    //}

    // main thread starts timing at same time as other threads
    let c = barrier.clone();
    c.wait();
    let start = std::time::SystemTime::now();
    for t in test_threads {
        t.join().unwrap();
    }
    let duration = start.elapsed().unwrap();
    println!("{:?}: {:.2}RO/ms", test, nqueries as f64/duration.as_millis() as f64 * 1000f64);

    if test != TestType::TestNoShim {
        if let Some(t) = jh {
            t.join().unwrap();
        }
    }
}
