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
extern crate hwloc;
extern crate libc;

use mysql::prelude::*;
use rand::prelude::*;
use std::*;
use std::collections::HashMap;
use std::thread;
use structopt::StructOpt;
use hwloc::{Topology, ObjectType, CPUBIND_THREAD, CPUBIND_PROCESS, CpuSet};
use std::sync::{Arc, Mutex};
use log::warn;

mod queriers;
mod policy;
use decor::*;

const SCHEMA : &'static str = include_str!("../schema.sql");
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
    nusers: u64,
    #[structopt(long="nstories", default_value="10")]
    nstories: u64,
    #[structopt(long="ncomments", default_value="100")]
    ncomments: u64,
    #[structopt(long="nthreads", default_value = "1")]
    nthreads: u64,
    #[structopt(long="nqueries", default_value = "100")]
    nqueries: u64,
    #[structopt(long="testname", default_value = "decor_1")]
    testname: String,
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        //.filter_level(log::LevelFilter::Warn)
        .filter_level(log::LevelFilter::Error)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn init_database(db: &mut mysql::Conn, nusers: u64, nstories: u64, ncomments: u64) {
    // users
    let mut user_ids = String::new();
    for user in 0..nusers {
        if user != 0 {
            user_ids.push_str(",");
        }
        user_ids.push_str(&format!("('user{}')", user));
    }
    db.query_drop(&format!(
            //"INSERT INTO users (id, username) VALUES {};", 
            "INSERT INTO users (username) VALUES {};", 
            user_ids)).unwrap();
    
    // stories
    let mut story_vals = String::new();
    for i in 0..nstories {
        if i != 0 {
            story_vals.push_str(",");
        }
        story_vals.push_str(&format!("({}, {}, 'story{}')", i % nusers +1, i, i));
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
                i % nusers + 1, i % nstories + 1, "2004-05-23T14:25:00", i, i));
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

fn init_db(topo: Arc<Mutex<Topology>>, test : TestType, testname: String, nusers: u64, nstories: u64, ncomments: u64) 
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
            // bind thread to core 1
            let tid = unsafe { libc::pthread_self() };
            let mut locked_topo = topo.lock().unwrap();
            let mut cpuset = cpuset_for_core(&mut *locked_topo, 2);
            cpuset.singlify();
            locked_topo.set_cpubind_for_thread(tid, cpuset, CPUBIND_THREAD).unwrap();
            drop(locked_topo);

            let policy = policy::get_lobsters_policy();
            if let Ok((s, _)) = listener.accept() {
                decor::Shim::run_on_tcp(
                    DBNAME, SCHEMA, policy,
                    decor::TestParams{testname: testname, translate:translate, parse:parse, in_memory: true}, s).unwrap();
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

fn cpuset_for_core(topology: &mut Topology, idx: usize) -> CpuSet {
    let cores = (*topology).objects_with_type(&ObjectType::Core).unwrap();
    match cores.get(idx) {
        Some(val) => val.allowed_cpuset().unwrap(),
        None => panic!("No Core found with id {}", idx)
    }
}

fn main() {
    init_logger();
    let args = Cli::from_args();
    let test = args.test;
    let ncomments = args.ncomments;
    let nqueries = args.nqueries;
    let nstories = args.nstories;
    let nusers = args.nusers;
    //let nthreads = args.nthreads;
    let testname = args.testname;

    // bind each thread to a particular cpu to avoid non-local memory accesses
    let topo = Arc::new(Mutex::new(Topology::new()));
    let mut locked_topo = topo.lock().unwrap();
    assert!(locked_topo.support().cpu().set_current_process() && 
        locked_topo.support().cpu().set_current_thread());
    let pid = unsafe { libc::getpid() };
    let mut cpuset = cpuset_for_core(&mut *locked_topo, 1);
    cpuset.singlify();
    locked_topo.set_cpubind_for_process(pid, cpuset, CPUBIND_PROCESS).unwrap();
    drop(locked_topo);
            
    let (mut db, jh) = init_db(topo.clone(), test.clone(), testname, nusers, nstories, ncomments);

    let mut rng = rand::thread_rng();
    let mut total_stories = nstories;
    let mut total_comments = ncomments;
    let mut unsubbed_users = HashMap::new(); 
    let mut nunsub = 0;
    let mut nresub = 0;
    let start = time::Instant::now();
    for _ in 0..nqueries {
        // all autoinc ids start at 1..
        let user = rng.gen_range(1, nusers+1);
        let story= rng.gen_range(0, nstories);
        if let Some(gids) = unsubbed_users.remove(&user) {
            nresub += 1;
            if test == TestType::TestDecor {
                queriers::user::resubscribe_user(user, gids, &mut db);
            } else {
                db.query_drop(&format!("INSERT INTO `users` (id, username) VALUES ({}, 'user{}')", user, user-1)).unwrap();
            }
        }
        //match rng.gen_range(0, 20) {
        match rng.gen_range(0, 24) {
            0..=8=> queriers::frontpage::query_frontpage(&mut db, Some(user)).unwrap(),
            9..=11 => {
                queriers::post_story::post_story(&mut db, Some(user), total_stories + 1, "Dummy title".to_string()).unwrap();
                total_stories += 1;
            }
            12..=14 => queriers::vote::vote_on_story(&mut db, Some(user), story, true).unwrap(),
            15..=17 => queriers::user::get_profile(&mut db, user).unwrap(),
            18..=20 => {
                queriers::comment::post_comment(&mut db, Some(user), total_comments + 1, story, None).unwrap();
                total_comments += 1;
            }
            _ => {
                nunsub += 1;
                if test == TestType::TestDecor {
                    let gids = queriers::user::unsubscribe_user(user, &mut db);
                    unsubbed_users.insert(user, gids);
                } else {
                    db.query_drop(&format!("DELETE FROM `users` WHERE `users`.`id` = {}", user)).unwrap();
                    unsubbed_users.insert(user, vec![]);
                }
            }
        }
    }
    let dur = start.elapsed();
    println!("Time to do {} queries ({}/{} un/resubs): {}s", nqueries, nunsub, nresub, dur.as_secs());
    //println!("{:.2}", nqueries as f64/duration.as_millis() as f64 * 1000f64);
    
    drop(db);
    if let Some(t) = jh {
        t.join().unwrap();
    } 
}
