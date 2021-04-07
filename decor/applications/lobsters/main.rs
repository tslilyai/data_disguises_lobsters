extern crate mysql;
extern crate log;
extern crate rand;
extern crate hwloc;
extern crate libc;

use mysql::prelude::*;
use rand::prelude::*;
use std::*;
use std::collections::HashMap;
use std::io::Write;
use std::fs::File;
use std::thread;
use structopt::StructOpt;
use hwloc::{Topology, ObjectType, CPUBIND_THREAD, CpuSet};
use std::sync::{Arc, Mutex};
use log::warn;

mod queriers;
mod datagen;
include!("statistics.rs");
use decor::*;

const SCHEMA : &'static str = include_str!("schema.sql");
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
    #[structopt(long="scale", default_value = "1")]
    scale: f64,
    #[structopt(long="nqueries", default_value = "100")]
    nqueries: u64,
    #[structopt(long="testname", default_value = "no_shim")]
    testname: String,
    #[structopt(long="prime")]
    prime: bool,
    #[structopt(long="prop_unsub", default_value = "0.0")]
    prop_unsub: f64,
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

fn create_schema(db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let mut txn = db.start_transaction(mysql::TxOpts::default())?;
    txn.query_drop("SET max_heap_table_size = 4294967295;")?;
    
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

fn init_db(topo: Arc<Mutex<Topology>>, cpu: usize, test : TestType, testname: &'static str, prime: bool) 
    -> (mysql::Conn, Option<thread::JoinHandle<()>>) 
{
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut jh = None;
    let url : String;
    let mut db : mysql::Conn;
      
    let mut use_decor = false;
    let mut parse = false;
    match test {
        TestType::TestDecor => {
            use_decor = true;
            parse = true;
        }
        TestType::TestShimParse => {
            use_decor = false;
            parse = true;
        }
        TestType::TestShim => {
            use_decor = false;
            parse = false;
        }
        _ => (),
    }
   
    let test_dbname = format!("{}_{}", DBNAME, testname);
    if test == TestType::TestNoShim {
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        // TODO this is done automatically in all the other tests
        // when select_db is called (probably not the right interface for DeCor)
        if prime {
            db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname)).unwrap();
            db.query_drop(&format!("CREATE DATABASE {};", &test_dbname)).unwrap();
            assert_eq!(db.ping(), true);
            assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
            create_schema(&mut db).unwrap();
        } else {
            assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        }
    } else {
        let dbname = test_dbname.clone();
        jh = Some(thread::spawn(move || {
            let tid = unsafe { libc::pthread_self() };
            let mut locked_topo = topo.lock().unwrap();
            let mut cpuset = cpuset_for_core(&mut *locked_topo, cpu);
            cpuset.singlify();
            locked_topo.set_cpubind_for_thread(tid, cpuset, CPUBIND_THREAD).unwrap();
            drop(locked_topo);

            /*if let Ok((s, _)) = listener.accept() {
                decor::Shim::run_on_tcp(
                    &dbname, SCHEMA, app,
                    decor::TestParams{
                        testname: testname.to_string(), 
                        use_decor: use_decor, 
                        parse:parse, 
                        in_memory: true, 
                        prime: prime
                    }, s).unwrap();
            }*/
        }));
        url = format!("mysql://127.0.0.1:{}", port);
        db = mysql::Conn::new(&url).unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
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

fn run_unsub_test(db: &mut mysql::Conn, scale: f64, prime: bool, testname: &'static str) {
    let sampler = datagen::Sampler::new(scale);
    if prime {
        datagen::gen_data(&sampler, db);
    }
    
    let mut file = File::create(format!("{}.out", testname)).unwrap();
    for i in 0..sampler.nusers() {
        let user_id = i as u64 + 1;
        let mut user_stories = 0;
        let mut user_comments = 0;
        let res = db.query_iter(format!(r"SELECT COUNT(*) FROM stories WHERE user_id={};", user_id)).unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_stories =helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        let res = db.query_iter(format!(r"SELECT COUNT(*) FROM comments WHERE user_id={};", user_id)).unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_comments =helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }

        let start = time::Instant::now();
        let gids = queriers::user::unsubscribe_user(user_id, db);
        let dur = start.elapsed();
        file.write(format!("{}, {}, {}, ", user_id, user_stories+user_comments, dur.as_micros()).as_bytes()).unwrap();
        
        let start = time::Instant::now();
        queriers::user::resubscribe_user(user_id, &gids, db);
        let dur = start.elapsed();
        file.write(format!("{}\n", dur.as_micros()).as_bytes()).unwrap();
    }
        
    file.flush().unwrap();
}

fn run_test(db: &mut mysql::Conn, test: TestType, nqueries: u64, scale: f64, prime: bool, testname: &'static str, prop_unsub: f64) {
    //let (mut db, jh) = init_db(topo.clone(), test.clone(), testname, prime);
    let sampler = datagen::Sampler::new(scale);
    let mut nstories = sampler.nstories();
    let mut ncomments = sampler.ncomments();
    if prime {
        let (ns, nc) = datagen::gen_data(&sampler, db);
        nstories = ns;
        ncomments = nc;
    }
    
    let mut rng = rand::thread_rng();
    let mut unsubbed_users : HashMap<u64, (String, String)> = HashMap::new(); 
    let mut nunsub = 0;
    let mut nresub = 0;
    let mut file = File::create(format!("{}.out", testname)).unwrap();
    let max_id = 0;//decor::guises::GHOST_ID_START as u32;
    let start = time::Instant::now();
    for i in 0..nqueries {
        // XXX: we're assuming that basically all page views happen as a user, and that the users
        // who are most active voters are also the ones that interact most with the site.
        // XXX: we're assuming that users who vote a lot also comment a lot
        // XXX: we're assuming that users who vote a lot also submit many stories
        let user_id = sampler.user(&mut rng) as u64;
        let username_id = user_id - 1;
        let user = Some(user_id);

        if let Some(gids) = &unsubbed_users.remove(&user_id) {
            nresub += 1;
            if test == TestType::TestDecor {
                queriers::user::resubscribe_user(user_id, gids, db);
            } else {
                // user id is always one more than the username...
                db.query_drop(&format!("INSERT INTO `users` (id, username) VALUES ({}, 'user{}')", user_id, username_id)).unwrap();
            }
        }

        // with probability prop_unsub, unsubscribe the user
        if rng.gen_bool(prop_unsub) {
            nunsub += 1;
            if test == TestType::TestDecor {
                let gids = queriers::user::unsubscribe_user(user_id, db);
                unsubbed_users.insert(user_id, gids);
            } else {
                db.query_drop(&format!("DELETE FROM `users` WHERE `users`.`username` = 'user{}'", username_id)).unwrap();
                unsubbed_users.insert(user_id, (String::new(), String::new()));
            }
        } else {
            // randomly pick next request type based on relative frequency
            let mut seed: isize = rng.gen_range(0, 100000);
            let seed = &mut seed;
            let mut pick = |f| {
                let applies = *seed <= f;
                *seed -= f;
                applies
            };

            let mut res = vec![];
            if pick(55842) {
                // XXX: we're assuming here that stories with more votes are viewed more
                let story = sampler.story_for_vote(&mut rng) as u64;
                res = queriers::stories::read_story(db, user, story).unwrap();
            } else if pick(30105) {
                res = queriers::frontpage::query_frontpage(db, user).unwrap();
            } else if pick(6702) {
                // XXX: we're assuming that users who vote a lot are also "popular"
                queriers::user::get_profile(db, user_id).unwrap();
            } else if pick(4674) {
                queriers::comment::get_comments(db, user).unwrap();
            } else if pick(967) {
                queriers::recent::recent(db, user).unwrap();
            } else if pick(630) {
                let comment = sampler.comment_for_vote(&mut rng);
                queriers::vote::vote_on_comment(db, user, comment as u64, true).unwrap();
            } else if pick(475) {
                let story = sampler.story_for_vote(&mut rng);
                queriers::vote::vote_on_story(db, user, story as u64, true).unwrap();
            } else if pick(316) {
                // comments without a parent
                let id = rng.gen_range(ncomments, max_id);
                let story = sampler.story_for_comment(&mut rng);
                queriers::comment::post_comment(db, user, id as u64, story as u64, None).unwrap();
            } else if pick(87) {
                queriers::user::login(db, user_id).unwrap();
            } else if pick(71) {
                // comments with a parent
                let id = rng.gen_range(ncomments, max_id);
                let story = sampler.story_for_comment(&mut rng);
                // we need to pick a comment that's on the chosen story
                // we know that every nth comment from prepopulation is to the same story
                let comments_per_story = ncomments / nstories;
                let parent = story + nstories * rng.gen_range(0, comments_per_story);
                queriers::comment::post_comment(db, user, id.into(), story as u64, Some(parent as u64)).unwrap();
            } else if pick(54) {
                let comment = sampler.comment_for_vote(&mut rng);
                queriers::vote::vote_on_comment(db, user, comment as u64, false).unwrap();
            } else if pick(53) {
                let id = rng.gen_range(nstories, max_id);
                queriers::stories::post_story(db, user, id as u64, format!("benchmark {}", id)).unwrap();
            } else {
                let story = sampler.story_for_vote(&mut rng);
                queriers::vote::vote_on_story(db, user, story as u64, false).unwrap();
            }
            res.sort();
            warn!("Query {}, user{}, {}\n", i, user_id, res.join(" "));
            file.write(format!("Query {}, user{}, {}\n", i, user_id, res.join(" ")).as_bytes()).unwrap();
        }
    }
    let dur = start.elapsed();
    println!("{} Time to do {} queries ({}/{} un/resubs): {}s", testname, nqueries, nunsub, nresub, dur.as_secs());
        
    file.flush().unwrap();
}

fn main() {
    init_logger();
    let args = Cli::from_args();
    //let test = args.test;
    let nqueries = args.nqueries;
    let scale = args.scale;
    let prime = args.prime;
    //let testname = args.testname;
    let prop_unsub = args.prop_unsub;

    let tid_core = 2;
    let topo = Arc::new(Mutex::new(Topology::new()));
    let tid = unsafe { libc::pthread_self() };
    
    let mut locked_topo = topo.lock().unwrap();
    let mut cpuset = cpuset_for_core(&mut *locked_topo, tid_core);
    cpuset.singlify();
    locked_topo.set_cpubind_for_thread(tid, cpuset, CPUBIND_THREAD).unwrap();
    drop(locked_topo);

    let (mut db, _) = init_db(topo, tid_core, TestType::TestDecor, "decor_unsub", prime);
    run_unsub_test(&mut db, scale, prime, "decor_unsub");

    /*use TestType::*;
    let tests = &[TestDecor];
    let testnames = vec!["decor"];
    //let tests = vec![TestNoShim, TestShim, TestDecor, TestShimParse];
    //let testnames = vec!["no_shim", "shim_only", "decor", "shim_parse"];

    //let mut threads = vec![];
    let mut core = 2;
    for i in 0..tests.len() {
        let testclone = tests[i].clone();
        let testname = testnames[i].clone();
        let mut tid_core = core;
        //threads.push(thread::spawn(move || {
            // bind thread to core 1
            let topo = Arc::new(Mutex::new(Topology::new()));
            let tid = unsafe { libc::pthread_self() };
            let mut locked_topo = topo.lock().unwrap();
            let mut cpuset = cpuset_for_core(&mut *locked_topo, tid_core);
            tid_core+=1;
            cpuset.singlify();
            locked_topo.set_cpubind_for_thread(tid, cpuset, CPUBIND_THREAD).unwrap();
            drop(locked_topo);
            
            let (mut db, jh) = init_db(topo, tid_core, testclone.clone(), testname, prime);
            run_test(&mut db, testclone, nqueries, scale, prime, testname, prop_unsub);
            
            drop(db);
            if let Some(t) = jh {
                t.join().unwrap();
            }
        //}));
        core += 2;
    }*/
    /*for thread in threads {
        thread.join().unwrap();
    }*/
}
