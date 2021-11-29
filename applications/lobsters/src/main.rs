extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use edna::{helpers, EdnaClient};
use hwloc::{CpuSet, ObjectType, Topology, CPUBIND_THREAD};
use log::warn;
use mysql::prelude::*;
use rand::Rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread;
use std::*;
use structopt::StructOpt;

mod datagen;
mod disguises;
mod queriers;
include!("statistics.rs");

const SCHEMA: &'static str = include_str!("../schema.sql");
const DBNAME: &'static str = &"edna_lobsters";

#[derive(StructOpt)]
struct Cli {
    #[structopt(long = "scale", default_value = "1")]
    scale: f64,
    #[structopt(long = "nqueries", default_value = "100")]
    nqueries: u64,
    #[structopt(long = "testname", default_value = "no_shim")]
    testname: String,
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "prop_unsub", default_value = "0.0")]
    prop_unsub: f64,
    #[structopt(long = "is_baseline")]
    is_baseline: bool,
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

fn run_unsub_test(
    edna: &mut EdnaClient,
    scale: f64,
    prime: bool,
    testname: &'static str,
    is_baseline: bool,
    sampler: &datagen::Sampler,
) {
    let mut db = edna.get_conn().unwrap();
    let mut file = File::create(format!("{}.out", testname)).unwrap();
    for i in 0..sampler.nusers() {
        let user_id = i as u64 + 1;
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

        let start = time::Instant::now();
        // UNSUB
        let dur = start.elapsed();
        file.write(
            format!(
                "{}, {}, {}, ",
                user_id,
                user_stories + user_comments,
                dur.as_micros()
            )
            .as_bytes(),
        )
        .unwrap();

        let start = time::Instant::now();
        // RESUB
        let dur = start.elapsed();
        file.write(format!("{}\n", dur.as_micros()).as_bytes())
            .unwrap();
    }

    file.flush().unwrap();
}

fn run_test(
    edna: &mut EdnaClient,
    nqueries: u64,
    scale: f64,
    prime: bool,
    testname: &'static str,
    prop_unsub: f64,
    is_baseline: bool,
    user2decryptcap: &HashMap<u64, Vec<u8>>,
    sampler: &datagen::Sampler,
) {
    let mut nusers = sampler.nusers();
    let mut nstories = sampler.nstories();
    let mut ncomments = sampler.ncomments();
    let mut db = edna.get_conn().unwrap();

    let mut rng = rand::thread_rng();
    let mut unsubbed_users: HashMap<u64, (String, String)> = HashMap::new();
    let mut nunsub = 0;
    let mut nresub = 0;
    let mut file = File::create(format!("{}.out", testname)).unwrap();
    let max_id = nusers;

    let start = time::Instant::now();
    for i in 0..nqueries {
        // XXX: we're assuming that basically all page views happen as a user, and that the users
        // who are most active voters are also the ones that interact most with the site.
        // XXX: we're assuming that users who vote a lot also comment a lot
        // XXX: we're assuming that users who vote a lot also submit many stories
        let user_id = sampler.user(&mut rng) as u64;
        let username_id = user_id - 1;
        let user = Some(user_id);

        if let Some((dlcs, olcs)) = &unsubbed_users.remove(&user_id) {
            nresub += 1;
            if is_baseline {
                // RESUB
            } else {
                // user id is always one more than the username...
                db.query_drop(&format!(
                    "INSERT INTO `users` (id, username) VALUES ({}, 'user{}')",
                    user_id, username_id
                ))
                .unwrap();
            }
        }

        // with probability prop_unsub, unsubscribe the user
        if rng.gen_bool(prop_unsub) {
            nunsub += 1;
            if is_baseline {
                // UNSUB
                let (dlcs, olcs) = disguises::gdpr_disguise::apply(
                    &*edna,
                    user_id,
                    decryption_cap,
                    own_loc_caps,
                    is_baseline,
                )
                .unwrap();
                unsubbed_users.insert(user_id, (dlcs, olcs));
            } else {
                db.query_drop(&format!(
                    "DELETE FROM `users` WHERE `users`.`username` = 'user{}'",
                    username_id
                ))
                .unwrap();
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
                res = queriers::stories::read_story(&mut db, user, story).unwrap();
            } else if pick(30105) {
                res = queriers::frontpage::query_frontpage(&mut db, user).unwrap();
            } else if pick(6702) {
                // XXX: we're assuming that users who vote a lot are also "popular"
                queriers::user::get_profile(&mut db, user_id).unwrap();
            } else if pick(4674) {
                queriers::comment::get_comments(&mut db, user).unwrap();
            } else if pick(967) {
                queriers::recent::recent(&mut db, user).unwrap();
            } else if pick(630) {
                let comment = sampler.comment_for_vote(&mut rng);
                queriers::vote::vote_on_comment(&mut db, user, comment as u64, true).unwrap();
            } else if pick(475) {
                let story = sampler.story_for_vote(&mut rng);
                queriers::vote::vote_on_story(&mut db, user, story as u64, true).unwrap();
            } else if pick(316) {
                // comments without a parent
                let id = rng.gen_range(ncomments, max_id);
                let story = sampler.story_for_comment(&mut rng);
                queriers::comment::post_comment(&mut db, user, id as u64, story as u64, None)
                    .unwrap();
            } else if pick(87) {
                queriers::user::login(&mut db, user_id).unwrap();
            } else if pick(71) {
                // comments with a parent
                let id = rng.gen_range(ncomments, max_id);
                let story = sampler.story_for_comment(&mut rng);
                // we need to pick a comment that's on the chosen story
                // we know that every nth comment from prepopulation is to the same story
                let comments_per_story = ncomments / nstories;
                let parent = story + nstories * rng.gen_range(0, comments_per_story);
                queriers::comment::post_comment(
                    &mut db,
                    user,
                    id.into(),
                    story as u64,
                    Some(parent as u64),
                )
                .unwrap();
            } else if pick(54) {
                let comment = sampler.comment_for_vote(&mut rng);
                queriers::vote::vote_on_comment(&mut db, user, comment as u64, false).unwrap();
            } else if pick(53) {
                let id = rng.gen_range(nstories, max_id);
                queriers::stories::post_story(
                    &mut db,
                    user,
                    id as u64,
                    format!("benchmark {}", id),
                )
                .unwrap();
            } else {
                let story = sampler.story_for_vote(&mut rng);
                queriers::vote::vote_on_story(&mut db, user, story as u64, false).unwrap();
            }
            res.sort();
            warn!("Query {}, user{}, {}\n", i, user_id, res.join(" "));
            file.write(format!("Query {}, user{}, {}\n", i, user_id, res.join(" ")).as_bytes())
                .unwrap();
        }
    }
    let dur = start.elapsed();
    println!(
        "{} Time to do {} queries ({}/{} un/resubs): {}s",
        testname,
        nqueries,
        nunsub,
        nresub,
        dur.as_secs()
    );

    file.flush().unwrap();
}

fn main() {
    init_logger();
    let args = Cli::from_args();
    let nqueries = args.nqueries;
    let scale = args.scale;
    let prime = args.prime;
    let prop_unsub = args.prop_unsub;
    let is_baseline = args.is_baseline;
    let sampler = datagen::Sampler::new(scale);

    let dbname: String;
    if is_baseline {
        dbname = format!("{}_baseline", DBNAME);
    } else {
        dbname = format!("{}_edna", DBNAME);
    }
    let mut edna = EdnaClient::new(
        args.prime,
        DBNAME,
        SCHEMA,
        true,
        sampler.nusers() as usize * 200, // assume each user has 200 pieces of data
        disguises::get_guise_gen(),      /*in-mem*/
    );
    if prime {
        datagen::gen_data(&sampler, &mut edna.get_conn().unwrap());
    }
    // always register users with edna?
    let mut user2decryptcap = HashMap::new();
    for u in 0..sampler.nusers() {
        let user_id = u as u64 + 1;
        let private_key = edna.register_principal(user_id.as_str().into());
        user2decryptcap.insert(user_id, private_key);
    }
    run_unsub_test(&mut edna, scale, prime, "unsub", is_baseline, &sampler);
    run_test(
        &mut edna,
        nqueries,
        scale,
        prime,
        "normal_unsub",
        prop_unsub,
        is_baseline,
        &user2decryptcap,
        &sampler,
    );
}
