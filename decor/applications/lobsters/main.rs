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
use std::*;
use std::thread;
use structopt::StructOpt;
//use std::sync::{Arc, Barrier};
//use log::{warn, debug};

use decor::*;

const SCHEMA : &'static str = include_str!("./schema_lobsters.sql");
const CONFIG : &'static str = include_str!("./config.json");
const DBNAME : &'static str = &"decor_lobsters";

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
    #[structopt(long="nusers", default_value="10")]
    nusers: usize,
    #[structopt(long="nstories", default_value="10")]
    nstories: usize,
    #[structopt(long="ncomments", default_value="100")]
    ncomments: usize,
    #[structopt(long="nthreads", default_value = "1")]
    nthreads: usize,
    #[structopt(long="nqueries", default_value = "100")]
    nqueries: usize,
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Error)
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
            let new_stmt = helpers::process_schema_stmt(&stmt, true); 
            txn.query_drop(new_stmt.to_string())?;
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
        /*let mut rows = vec![];
        for row in res {
            let row = row.unwrap();
            let mut row_vals = vec![];
            for i in 0..row.len() {
                row_vals.push(row[i].clone());
            }
            rows.push(row_vals);
        }*/
        //println!("story {}: {:?}", story, rows);
    }
}

fn test_insert(db: &mut mysql::Conn, nqueries: usize, nstories: usize, nusers: usize, ncomments: usize) {
    // select comments and stories at random to read
    for i in 0..nqueries {
        let user = (ncomments+i) % nusers;
        let story = (ncomments+i) % nstories;
        let time = "2004-05-23T14:25:00";
        db.query_drop(&format!("INSERT INTO comments \
            (user_id, story_id, created_at, comment, short_id) \
            VALUES ({}, {}, '{}', 'newcomment', {});",
            user, story, time, ncomments+i)).unwrap();
    }
}

fn test_update(db: &mut mysql::Conn, nqueries: usize, nusers: usize, nstories: usize) {
    // select comments and stories at random to read
    for i in 0..nqueries {
        let user = i % nusers;
        let story = i % nstories;
        db.query_drop(&format!("UPDATE comments \
            SET comment = 'newercomment' \
            WHERE story_id = {} AND user_id = {};",
            story, user)).unwrap();
    }
}

fn init_db(test : TestType, nusers: usize, nstories: usize, ncomments: usize) 
    -> (mysql::Conn, Option<thread::JoinHandle<()>>) 
{
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut jh = None;
    let url : String;
    let mut db : mysql::Conn;
      
    let mut translate = false;
    let mut parse = false;
    match test {
        TestType::TestDecor => {
            translate = true;
            parse = true;
        }
        TestType::TestShimParse => {
            translate = false;
            parse = true;
        }
        TestType::TestShim => {
            translate = false;
            parse = false;
        }
        _ => (),
    }
    
    if test == TestType::TestNoShim {
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", DBNAME)).unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", DBNAME)).unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", DBNAME)), true);
        // TODO this is done automatically in all the other tests
        // when select_db is called (probably not the right interface for DeCor)
        create_schema(&mut db).unwrap();
        init_database(&mut db, nusers, nstories, ncomments);
    } else {
        jh = Some(thread::spawn(move || {
            if let Ok((s, _)) = listener.accept() {
                let mut db = mysql::Conn::new("mysql://tslilyai:pass@127.0.0.1").unwrap();
                db.query_drop(&format!("DROP DATABASE IF EXISTS {};", DBNAME)).unwrap();
                db.query_drop(&format!("CREATE DATABASE {};", DBNAME)).unwrap();
                assert_eq!(db.ping(), true);
                decor::Shim::run_on_tcp(
                    db, CONFIG, SCHEMA, 
                    decor::TestParams{translate:translate, parse:parse, in_memory: true}, s).unwrap();
            }
        }));
        url = format!("mysql://127.0.0.1:{}", port);
        db = mysql::Conn::new(&url).unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", DBNAME)), true);
        init_database(&mut db, nusers, nstories, ncomments);
    }
    (db, jh)
}

fn main() {
    init_logger();
    let args = Cli::from_args();
    let test = args.test;
    let ncomments = args.ncomments;
    let nqueries = args.nqueries;
    let nstories = args.nstories;
    let nusers = args.nusers;
    let nthreads = args.nthreads;

    // TEST RO Queries
    let (mut db, jh) = init_db(test.clone(), nusers, nstories, ncomments);
    let start = std::time::SystemTime::now();
    test_reads(&mut db, nqueries/ nthreads, nstories);
    let roduration = start.elapsed().unwrap();
    drop(db);
    if let Some(t) = jh {
        t.join().unwrap();
    }
    
    // TEST Insert Queries (should just double queries to insert)
    let (mut db, jh) = init_db(test.clone(), nusers, nstories, ncomments);
    let start = std::time::SystemTime::now();
    test_insert(&mut db, nqueries/ nthreads, nstories, nusers, ncomments);
    let insduration = start.elapsed().unwrap();
    drop(db);
    if let Some(t) = jh {
        t.join().unwrap();
    }

    // TEST Update Queries (should read from ghosts)
    let (mut db, jh) = init_db(test.clone(), nusers, nstories, ncomments);
    let start = std::time::SystemTime::now();
    test_update(&mut db, nqueries/ nthreads, nusers, nstories);
    let upduration = start.elapsed().unwrap();
    drop(db);
    if let Some(t) = jh {
        t.join().unwrap();
    } 
    println!("{:?}\t{:.2}\t{:.2}\t{:.2}",
             test, 
             nqueries as f64/roduration.as_millis() as f64 * 1000f64,
             nqueries as f64/insduration.as_millis() as f64 * 1000f64,
             nqueries as f64/upduration.as_millis() as f64 * 1000f64);
}
