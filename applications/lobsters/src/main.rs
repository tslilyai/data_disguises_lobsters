extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use chrono::Local;
use edna::{helpers, EdnaClient};
use log::{error};
use mysql::prelude::*;
use rand::Rng;
use rsa::pkcs1::ToRsaPrivateKey;
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
//use std::io::BufReader;
use std::io::Write;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time;
use std::time::Duration;
use std::*;
use structopt::StructOpt;

mod datagen;
mod disguises;
mod queriers;
include!("statistics.rs");

const TOTAL_TIME: u128 = 100000;
const SCHEMA: &'static str = include_str!("../schema.sql");

#[derive(StructOpt)]
struct Cli {
    #[structopt(long = "scale", default_value = "1")]
    scale: f64,
    #[structopt(long = "nsleep", default_value = "10000")]
    nsleep: u64,
    #[structopt(long = "filename", default_value = "")]
    filename: String,
    #[structopt(long = "nconcurrent", default_value = "1")]
    nconcurrent: usize,
    #[structopt(long = "disguiser", default_value = "cheap")]
    disguiser: String,
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "stats")]
    stats: bool,
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

fn main() {
    init_logger();
    let args = Cli::from_args();
    let scale = args.scale;
    let prime = args.prime;
    let sampler = datagen::Sampler::new(scale);
    let nusers = sampler.nusers();

    let dbname: String = "lobsters_edna".to_string();
    let mut edna = EdnaClient::new(
        args.prime,
        true, // batch
        "127.0.0.1",
        &dbname,
        SCHEMA,
        true,
        nusers as usize * 200,      // assume each user has 200 pieces of data
        disguises::get_guise_gen(), /*in-mem*/
    );

    let mut user2decryptcap = HashMap::new();
    if prime {
        datagen::gen_data(&sampler, &mut edna.get_conn().unwrap());
    }
    // create private keys, save in file jic if we've primed already
    //if prime {
    let mut db = edna.get_conn().unwrap();
    db.query_drop("DELETE FROM EdnaPrincipals WHERE uid >= 0")
        .unwrap();
    // clear extra pseudoprincipals
    db.query_drop("DELETE FROM users WHERE id >= 10000")
        .unwrap();
    for u in 0..nusers {
        let user_id = u as u64 + 1;
        let private_key = edna.register_principal(&user_id.to_string());
        let privkey_str = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        user2decryptcap.insert(user_id, privkey_str);
    }
    serde_json::to_writer(
        &File::create("decrypt_caps.json").unwrap(),
        &user2decryptcap,
    )
    .unwrap();
    error!("Got {} decrypt caps", user2decryptcap.len());
    /*} else {
        let file = File::open("decrypt_caps.json").unwrap();
        let reader = BufReader::new(file);
        user2decryptcap = serde_json::from_reader(reader).unwrap();
        error!("Got {} decrypt caps", user2decryptcap.len());
    }*/

    if args.stats {
        run_stats_test(&mut edna, &sampler, &user2decryptcap, prime);
        return;
    }

    // otherwise run the concurrent test
    let delete_durations = Arc::new(Mutex::new(vec![]));
    let restore_durations = Arc::new(Mutex::new(vec![]));
    let op_durations = Arc::new(Mutex::new(vec![]));

    let mut user_stories = 0;
    let mut user_comments = 0;
    let mut user_votes = 0;
    let user_to_disguise = if args.disguiser == "cheap" {
        1 as u64
    } else if args.disguiser == "expensive" {
        nusers as u64
    } else {
        0
    };
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM stories WHERE user_id={};",
            user_to_disguise
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        user_stories = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM votes WHERE user_id={};",
            user_to_disguise
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        user_votes = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    let res = db
        .query_iter(format!(
            r"SELECT COUNT(*) FROM comments WHERE user_id={};",
            user_to_disguise
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        user_comments = helpers::mysql_val_to_u64(&vals[0]).unwrap();
    }
    error!(
        "Going to disguise user with {} stories, {} comments, {} votes ({} total)",
        user_stories,
        user_comments,
        user_votes,
        user_stories + user_votes + user_comments
    );

    let mut threads = vec![];
    let arc_sampler = Arc::new(sampler);
    let barrier = Arc::new(Barrier::new(args.nconcurrent + 1));
    for _ in 0..args.nconcurrent {
        let c = barrier.clone();
        let my_op_durations = op_durations.clone();
        let mut db = edna.get_conn().unwrap();
        let my_arc_sampler = arc_sampler.clone();
        threads.push(thread::spawn(move || {
            run_normal_thread(&mut db, my_arc_sampler, my_op_durations, c)
        }));
    }
    error!("Waiting for barrier!");
    barrier.wait();

    let arc_edna = Arc::new(Mutex::new(edna));
    let ndisguises = if user_to_disguise > 0 {
        run_disguising_sleeps(
            &args,
            arc_edna,
            user_to_disguise,
            &user2decryptcap,
            delete_durations.clone(),
            restore_durations.clone(),
        )
        .unwrap()
    } else {
        0
    };

    for j in threads {
        j.join().expect("Could not join?");
    }

    print_stats(
        &args,
        ndisguises,
        &op_durations.lock().unwrap(),
        &delete_durations.lock().unwrap(),
        &restore_durations.lock().unwrap(),
    );
}

fn run_normal_thread(
    db: &mut mysql::PooledConn,
    sampler: Arc<datagen::Sampler>,
    op_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    barrier: Arc<Barrier>,
) {
    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();

    let mut rng = rand::thread_rng();
    let max_id = ncomments * 10000;
    let mut my_op_durations = vec![];

    barrier.wait();

    let overall_start = time::Instant::now();
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        let start = time::Instant::now();
        // XXX: we're assuming that basically all page views happen as a user, and that the users
        // who are most active voters are also the ones that interact most with the site.
        // XXX: we're assuming that users who vote a lot also comment a lot
        // XXX: we're assuming that users who vote a lot also submit many stories
        let user_id = sampler.user(&mut rng) as u64;
        let user = Some(user_id);

        // randomly pick next request type based on relative frequency
        let mut seed: isize = rng.gen_range(0, 100000);
        let seed = &mut seed;
        let mut pick = |f| {
            let applies = *seed <= f;
            *seed -= f;
            applies
        };

        let mut res = vec![];
        let mut op = "read_story";
        if pick(55842) {
            // XXX: we're assuming here that stories with more votes are viewed more
            let story = sampler.story_for_vote(&mut rng) as u64;
            res = queriers::stories::read_story(db, user, story).unwrap();
        } else if pick(30105) {
            op = "frontpage";
            res = queriers::frontpage::query_frontpage(db, user).unwrap();
        } else if pick(6702) {
            // XXX: we're assuming that users who vote a lot are also "popular"
            op = "prof";
            queriers::user::get_profile(db, user_id).unwrap();
        } else if pick(4674) {
            op = "getcomments";
            queriers::comment::get_comments(db, user).unwrap();
        } else if pick(967) {
            op = "getrecent";
            queriers::recent::recent(db, user).unwrap();
        } else if pick(630) {
            op = "votecomment";
            let comment = sampler.comment_for_vote(&mut rng);
            queriers::vote::vote_on_comment(db, user, comment as u64, true).unwrap();
        } else if pick(475) {
            op = "votestory";
            let story = sampler.story_for_vote(&mut rng);
            queriers::vote::vote_on_story(db, user, story as u64, true).unwrap();
        } else if pick(316) {
            // comments without a parent
            op = "postcomment";
            let id = rng.gen_range(ncomments, max_id);
            let story = sampler.story_for_comment(&mut rng);
            queriers::comment::post_comment(db, user, id as u64, story as u64, None).unwrap();
        } else if pick(87) {
            queriers::user::login(db, user_id).unwrap();
        } else if pick(71) {
            // comments with a parent
            op = "postcommentparent";
            let id = rng.gen_range(ncomments, max_id);
            let story = sampler.story_for_comment(&mut rng);
            // we need to pick a comment that's on the chosen story
            // we know that every nth comment from prepopulation is to the same story
            let comments_per_story = ncomments / nstories;
            let parent = story + nstories * rng.gen_range(0, comments_per_story);
            queriers::comment::post_comment(db, user, id.into(), story as u64, Some(parent as u64))
                .unwrap();
        } else if pick(54) {
            op = "votecomment";
            let comment = sampler.comment_for_vote(&mut rng);
            queriers::vote::vote_on_comment(db, user, comment as u64, false).unwrap();
        } else if pick(53) {
            op = "poststory";
            let id = rng.gen_range(nstories, max_id);
            queriers::stories::post_story(db, user, id as u64, format!("benchmark {}", id))
                .unwrap();
        } else {
            op = "votestory";
            let story = sampler.story_for_vote(&mut rng);
            queriers::vote::vote_on_story(db, user, story as u64, false).unwrap();
        }
        res.sort();
        my_op_durations.push((overall_start.elapsed(), start.elapsed()));
        //let dur = start.elapsed().as_micros();
        //error!("user{} {}: {}", user_id, op, dur);
    }
    op_durations.lock().unwrap().append(&mut my_op_durations);
}

fn run_disguising_thread(
    edna: Arc<Mutex<EdnaClient>>,
    uid: u64,
    decryption_cap: &Vec<u8>,
    delete_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    restore_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    overall_start: time::Instant,
    barrier: Arc<Barrier>,
    nsleep: u64,
) {
    let mut my_delete_durations = vec![];
    let mut my_restore_durations = vec![];

    // UNSUB
    let start = time::Instant::now();
    let mut edna_locked = edna.lock().unwrap();
    let lcs =
        disguises::gdpr_disguise::apply(&mut edna_locked, uid, decryption_cap.clone(), vec![])
            .unwrap();
    my_delete_durations.push((overall_start.elapsed(), start.elapsed()));
    drop(edna_locked);

    // wait for any concurrent disguisers to finish
    barrier.wait();
    // sleep for some seconds, then restore
    thread::sleep(time::Duration::from_millis(nsleep));

    // RESUB
    let start = time::Instant::now();
    let mut edna_locked = edna.lock().unwrap();
    let ls = match lcs.get(&(uid.to_string(), disguises::gdpr_disguise::get_disguise_id())) {
        Some(dl) => vec![*dl],
        None => vec![],
    };
    disguises::gdpr_disguise::reveal(&mut edna_locked, decryption_cap.clone(), ls).unwrap();
    drop(edna_locked);
    my_restore_durations.push((overall_start.elapsed(), start.elapsed()));

    delete_durations
        .lock()
        .unwrap()
        .append(&mut my_delete_durations);
    restore_durations
        .lock()
        .unwrap()
        .append(&mut my_restore_durations);
}

fn run_disguising_sleeps(
    args: &Cli,
    edna: Arc<Mutex<EdnaClient>>,
    uid: u64,
    user2decryptcap: &HashMap<u64, Vec<u8>>,
    delete_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    restore_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
) -> Result<u64, mysql::Error> {
    let overall_start = time::Instant::now();
    let mut nexec = 0;
    //let mut rng = rand::thread_rng();
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        // wait between each round
        thread::sleep(time::Duration::from_millis(args.nsleep));
        let barrier = Arc::new(Barrier::new(0));
        let my_edna = edna.clone();
        let decryptcap = user2decryptcap.get(&uid).unwrap().clone();
        let my_delete_durations = delete_durations.clone();
        let my_restore_durations = restore_durations.clone();
        let c = barrier.clone();
        run_disguising_thread(
            my_edna,
            uid,
            &decryptcap,
            my_delete_durations,
            my_restore_durations,
            overall_start,
            c,
            args.nsleep,
        );
        nexec += 1;
    }
    Ok(nexec)
}

/*fn run_disguising(
    edna: Arc<Mutex<EdnaClient>>,
    ndisguising: u64,
    user2decryptcap: &HashMap<u64, Vec<u8>>,
    delete_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    restore_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
) -> Result<(), mysql::Error> {
    let us: Vec<usize> = (101..101 + ndisguising as usize).collect();
    let overall_start = time::Instant::now();
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        // wait between each round
        thread::sleep(time::Duration::from_millis(10000));
        let mut disguising_threads = vec![];
        let barrier = Arc::new(Barrier::new(us.len()));
        for u in &us {
            let decryptcap = user2decryptcap.get(&(*u as u64)).unwrap().clone();
            let my_delete_durations = delete_durations.clone();
            let my_restore_durations = restore_durations.clone();
            let my_edna = edna.clone();
            let c = barrier.clone();
            let u = *u as u64;
            disguising_threads.push(thread::spawn(move || {
                run_disguising_thread(
                    my_edna,
                    u,
                    &decryptcap,
                    my_delete_durations,
                    my_restore_durations,
                    overall_start,
                    c,
                    10000,
                )
            }));
        }
        for j in disguising_threads {
            j.join().expect("Could not join?");
        }
    }
    Ok(())
}*/

fn run_stats_test(
    edna: &mut EdnaClient,
    sampler: &datagen::Sampler,
    user2decryptcaps: &HashMap<u64, Vec<u8>>,
    prime: bool,
) {
    let mut db = edna.get_conn().unwrap();
    let filename = format!("lobsters_disguise_stats.csv");
    let mut file = File::create(filename).unwrap();
    file.write(
        "uid, ndata, create_baseline, create_edna, decay, undecay, delete, restore, baseline\n"
            .as_bytes(),
    )
    .unwrap();
    let mut rng = rand::thread_rng();

    for u in 0..sampler.nusers() {

        // sample every 50 users
        if u % 50 != 0 {
            continue;
        }
        let user_id = u as u64 + 1;
        let decryption_cap = user2decryptcaps.get(&user_id).unwrap();
        let mut user_stories = 0;
        let mut user_comments = 0;
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_stories = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            user_comments = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        }
        file.write(format!("{}, {}, ", user_id, user_stories + user_comments).as_bytes())
            .unwrap();

        let start = time::Instant::now();
        let some_user_id: u32 = rng.gen();
        db.query_drop(&format!(
            "INSERT INTO `users` (`username`, `last_login`) VALUES ({}, '{}')",
            some_user_id,
            Local::now().naive_local().to_string()
        ))
        .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        edna.register_principal(&some_user_id.to_string());
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();

        // DECAY
        let start = time::Instant::now();
        let lcs =
            disguises::data_decay::apply(edna, user_id, decryption_cap.clone(), vec![]).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();

        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }

        // UNDECAY
        let start = time::Instant::now();
        let ls = match lcs.get(&(
            user_id.to_string(),
            disguises::data_decay::get_disguise_id(),
        )) {
            Some(ol) => vec![*ol],
            None => vec![],
        };
        disguises::data_decay::reveal(edna, decryption_cap.clone(), ls).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();

        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();

        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_stories);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_comments);
        }

        // UNSUB
        let start = time::Instant::now();
        let lcs =
            disguises::gdpr_disguise::apply(edna, user_id, decryption_cap.clone(), vec![]).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), 0);
        }

        // RESUB
        let start = time::Instant::now();
        let ls = match lcs.get(&(
            user_id.to_string(),
            disguises::gdpr_disguise::get_disguise_id(),
        )) {
            Some(ol) => vec![*ol],
            None => vec![],
        };
        disguises::gdpr_disguise::reveal(edna, decryption_cap.clone(), ls).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes())
            .unwrap();
        // checks
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM stories WHERE user_id={};",
                user_id
            ))
            .unwrap();

        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_stories);
        }
        let res = db
            .query_iter(format!(
                r"SELECT COUNT(*) FROM comments WHERE user_id={};",
                user_id
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            assert_eq!(helpers::mysql_val_to_u64(&vals[0]).unwrap(), user_comments);
        }

        // baseline delete
        // only measure this if we're not priming, so we don't mess up the DB again...
        let start = time::Instant::now();
        disguises::baseline::apply_delete(u as u64 + 1, edna).unwrap();
        //disguises::baseline::apply_decay(user_id, edna).unwrap();
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes())
            .unwrap();
    }

    file.flush().unwrap();
}

fn print_stats(
    args: &Cli,
    ndisguises: u64,
    edit_durations: &Vec<(Duration, Duration)>,
    delete_durations: &Vec<(Duration, Duration)>,
    restore_durations: &Vec<(Duration, Duration)>,
) {
    error!("Finished {} disguises", ndisguises);

    let filename = format!("concurrent_disguise_stats_{}.csv", args.filename);

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();

    writeln!(f, "{}", ndisguises).unwrap();

    writeln!(
        f,
        "{}",
        edit_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!(
                "{}:{}",
                d.0.as_millis().to_string(),
                d.1.as_micros().to_string()
            ))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
