extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use edna::{helpers, EdnaClient};
use log::warn;
use mysql::prelude::*;
use rand::Rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::thread;
use std::*;
use structopt::StructOpt;
use rsa::pkcs1::ToRsaPrivateKey;

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
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "batch")]
    batch: bool,
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

fn run_stats_test(
    edna: &mut EdnaClient,
    sampler: &datagen::Sampler,
    user2decryptcaps: &HashMap<u64, Vec<u8>>,
    batch: bool
) {
    let mut db = edna.get_conn().unwrap();
    let filename = if batch {
        format!("lobsters_disguise_stats.csv")
    } else {
        format!("lobsters_disguise_stats_batch.csv")
    };
    let mut file = File::create(filename).unwrap();
    file.write("uid, ndata, create_baseline, create_edna, decay, undecay, delete, restore, baseline\n".as_bytes()).unwrap();
    let mut rng = rand::thread_rng();

    for u in 0..sampler.nusers() {
        // sample every 50 users
        if u % 70 != 0 {
            continue
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
        file.write(
            format!(
                "{}, {}, ",
                user_id,
                user_stories + user_comments
            ).as_bytes()).unwrap();
 
        let start = time::Instant::now();
        let some_user_id : u32 = rng.gen();
        db.query_drop(&format!("INSERT INTO `users` (`username`) VALUES ({})", some_user_id)).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes()).unwrap();
        edna.register_principal(&some_user_id.to_string());
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes()).unwrap();

        // DECAY
        let start = time::Instant::now();
        let (dlcs, olcs) = disguises::data_decay::apply(
            edna,
            user_id,
            decryption_cap.clone(),
            vec![],
        )
        .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes()).unwrap();

        // UNDECAY
        let start = time::Instant::now();
        let dls = match dlcs.get(&(user_id.to_string(), disguises::data_decay::get_disguise_id())) {
            Some(dl) => vec![*dl],
            None => vec![],
        };
        let ols = match olcs.get(&(user_id.to_string(), disguises::data_decay::get_disguise_id())) {
            Some(ol) => vec![*ol],
            None => vec![],
        };
        disguises::data_decay::reveal(
            edna,
            decryption_cap.clone(),
            dls, ols
        ).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes()).unwrap();

        // UNSUB 
        let start = time::Instant::now();
        let (dlcs, olcs) = disguises::gdpr_disguise::apply(
            edna,
            user_id,
            decryption_cap.clone(),
            vec![],
        )
        .unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes()).unwrap();

        // RESUB
        let start = time::Instant::now();
        let dls = match dlcs.get(&(user_id.to_string(), disguises::gdpr_disguise::get_disguise_id())) {
            Some(dl) => vec![*dl],
            None => vec![],
        };
        let ols = match olcs.get(&(user_id.to_string(), disguises::gdpr_disguise::get_disguise_id())) {
            Some(ol) => vec![*ol],
            None => vec![],
        };
        disguises::gdpr_disguise::reveal(
            edna,
            decryption_cap.clone(),
            dls, ols,
        ).unwrap();
        file.write(format!("{}, ", start.elapsed().as_micros()).as_bytes()).unwrap();

        // baseline delete
        let start = time::Instant::now();
        //disguises::baseline::apply_delete(user_id, edna).unwrap();
        disguises::baseline::apply_decay(user_id, edna).unwrap();
        file.write(format!("{}\n", start.elapsed().as_micros()).as_bytes()).unwrap();
    }

    file.flush().unwrap();
}

fn run_test(
    edna: &mut EdnaClient,
    nqueries: u64,
    is_baseline: bool,
    user2decryptcap: &HashMap<u64, Vec<u8>>,
    sampler: &datagen::Sampler,
) {
    let nusers = sampler.nusers();
    let nstories = sampler.nstories();
    let ncomments = sampler.ncomments();
    let mut db = edna.get_conn().unwrap();

    let mut rng = rand::thread_rng();
    let mut file = File::create(format!("lobsters_stats.out")).unwrap();
    let max_id = nusers;

    let start = time::Instant::now();
    for i in 0..nqueries {
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
    println!(
        "Lobsters: Time to do {} queries: {}s",
        nqueries,
        start.elapsed().as_secs()
    );

    file.flush().unwrap();
}

fn main() {
    init_logger();
    let args = Cli::from_args();
    let nqueries = args.nqueries;
    let scale = args.scale;
    let prime = args.prime;
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
        args.batch,
        &dbname,
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
        let private_key = edna.register_principal(&user_id.to_string());
        let privkey_str = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        user2decryptcap.insert(user_id, privkey_str);
    }
    run_stats_test(&mut edna, &sampler, &user2decryptcap, args.batch);
    /*run_test(
        &mut edna,
        nqueries,
        is_baseline,
        &user2decryptcap,
        &sampler,
    );*/
}
