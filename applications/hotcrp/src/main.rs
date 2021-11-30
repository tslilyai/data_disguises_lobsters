extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use edna::EdnaClient;
use log::warn;
use rsa::pkcs1::ToRsaPrivateKey;
use std::collections::HashMap;
use std::fs::{OpenOptions};
use std::io::{Write};
use std::time::Duration;
use std::*;
use structopt::StructOpt;
use mysql::prelude::*;
use mysql::from_value;
use std::str::FromStr;

mod datagen;
mod disguises;

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "baseline")]
    baseline: bool,
    // Generates nusers_nonpc+nusers_pc users
    #[structopt(long = "nusers_nonpc", default_value="400")]
    nusers_nonpc: usize,
    #[structopt(long = "nusers_pc", default_value="50")]
    nusers_pc: usize,
    // Generates npapers_rej+npapers_accept papers.
    #[structopt(long = "npapers_rej", default_value="400")]
    npapers_rej: usize,
    #[structopt(long = "npapers_acc", default_value="50")]
    npapers_accept: usize,
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
    
    if args.baseline {
        run_baseline(&args);
    } else {
        run_edna(&args);
    }
}


fn run_edna(args: &Cli) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];
    let mut restore_durations = vec![];

    let mut edit_durations_preanon = vec![];
    let mut delete_durations_preanon = vec![];
    let mut restore_durations_preanon = vec![];

    let nusers = args.nusers_nonpc + args.nusers_pc;
    let mut edna = EdnaClient::new(
        args.prime,
        DBNAME,
        SCHEMA,
        true,
        nusers * 100, 
        disguises::get_guise_gen(), 
    );
    if args.prime {
        datagen::populate_database(&mut edna, &args).unwrap();
    }

    warn!("database populated!");

    let mut db = edna.get_conn().unwrap();
    let mut decrypt_caps = HashMap::new();
    for uid in 1..nusers + 1 {
        let start = time::Instant::now();
        let private_key = edna.register_principal(uid.to_string());
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        decrypt_caps.insert(uid as usize, private_key_vec);
        datagen::insert_single_user(&mut db).unwrap();
        account_durations.push(start.elapsed());
    }

    let mut user2rid = HashMap::new();
    // baseline edit/delete/restore for pc members 
    for u in args.nusers_nonpc+2..args.nusers_nonpc+2 + 10 {
        let dc = decrypt_caps.get(&u).unwrap().to_vec();

        // edit
        let start = time::Instant::now();
        let rids = datagen::reviews::get_reviews(u as u64, &mut db).unwrap();
        datagen::reviews::update_review(rids[0], &mut db).unwrap();
        edit_durations_preanon.push(start.elapsed());
        user2rid.insert(u, rids[0]);

        // delete
        let start = time::Instant::now();
        let (gdpr_diff_locs, gdpr_own_locs) =
            disguises::gdpr_disguise::apply(&mut edna, u as u64, dc.clone(), vec![]).unwrap();
        delete_durations_preanon.push(start.elapsed());

        // restore
        let start = time::Instant::now();
        let dl = vec![*gdpr_diff_locs.get(&(u.to_string(), disguises::gdpr_disguise::get_disguise_id())).unwrap()];
        let ol = vec![*gdpr_own_locs.get(&(u.to_string(), disguises::gdpr_disguise::get_disguise_id())).unwrap()];
        disguises::gdpr_disguise::reveal(&mut edna, dc, dl, ol).unwrap();
        restore_durations_preanon.push(start.elapsed());
    }

    // anonymize
    let start = time::Instant::now();
    // anonymization doesn't produce diff tokens that we'll reuse later
    let (_diff_locs, own_locs) = disguises::conf_anon_disguise::apply(&mut edna).unwrap();
    anon_durations.push(start.elapsed());

    // edit/delete/restore for pc members 
    for u in args.nusers_nonpc+2..args.nusers_nonpc+2 + 10 {
        let dc = decrypt_caps.get(&u).unwrap().to_vec();
        let ol = vec![*own_locs
            .get(&(
                u.to_string(),
                disguises::conf_anon_disguise::get_disguise_id(),
            ))
            .unwrap()];

        // edit after anonymization, for fairness only edit the one review 
        let rid = user2rid.get(&u).unwrap();
        let start = time::Instant::now();
        let mut db = edna.get_conn().unwrap();
        let pps = edna.get_pseudoprincipals(dc.clone(), ol.clone());
        for pp in pps {
            let rids = datagen::reviews::get_reviews(u64::from_str(&pp).unwrap(), &mut db).unwrap();
            if rids.len() > 0 && rids[0] == *rid {
                datagen::reviews::update_review(rids[0], &mut db).unwrap();
            }
        }
        edit_durations.push(start.elapsed());

        // delete
        let start = time::Instant::now();
        let (gdpr_diff_locs, _gdpr_own_locs) =
            disguises::gdpr_disguise::apply(&mut edna, u as u64, dc.clone(), ol.clone()).unwrap();
        delete_durations.push(start.elapsed());

        // restore
        let start = time::Instant::now();
        let dl = vec![*gdpr_diff_locs.get(&(u.to_string(), disguises::gdpr_disguise::get_disguise_id())).unwrap()];
        //let ol = gdpr_own_locs.into_iter().map(|p| p.1).collect();
        disguises::gdpr_disguise::reveal(&mut edna, dc, dl, ol).unwrap();
        restore_durations.push(start.elapsed());
    }
    print_stats(
        nusers as u64,
        account_durations,
        anon_durations,
        edit_durations,
        delete_durations,
        restore_durations,
        edit_durations_preanon,
        delete_durations_preanon,
        restore_durations_preanon,
        false,
    );
}

fn run_baseline(args: &Cli) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];

    let nusers = args.nusers_nonpc + args.nusers_pc;
    let mut edna = EdnaClient::new(
        args.prime,
        DBNAME,
        SCHEMA,
        true,
        nusers * 100,
        disguises::get_guise_gen(), 
    );
    if args.prime {
        datagen::populate_database(&mut edna, &args).unwrap();
    }

    warn!("database populated!");

    let mut db = edna.get_conn().unwrap();
    let mut db1 = edna.get_conn().unwrap();
    for _ in 0..10 {
        let start = time::Instant::now();
        datagen::insert_single_user(&mut db).unwrap();
        account_durations.push(start.elapsed());
    }

    // baseline edit/delete/restore for pc members 
    for u in args.nusers_nonpc+2..args.nusers_nonpc+2 + 10 {
        // edit
        let start = time::Instant::now();
        let rids = datagen::reviews::get_reviews(u as u64, &mut db).unwrap();
        datagen::reviews::update_review(rids[0], &mut db).unwrap();
        edit_durations.push(start.elapsed());

        // delete
        let start = time::Instant::now();
        db.query_drop(&format!("DELETE FROM ContactInfo WHERE contactId = {}", u)).unwrap();
        db.query_drop(&format!("DELETE FROM PaperWatch WHERE contactId = {}", u)).unwrap();
        db.query_drop(&format!("DELETE FROM PaperReviewPreference WHERE contactId = {}", u)).unwrap();
        db.query_drop(&format!("DELETE FROM Capability WHERE contactId = {}", u)).unwrap();
        db.query_drop(&format!("DELETE FROM PaperConflict WHERE contactId = {}", u)).unwrap();
        db.query_drop(&format!("DELETE FROM TopicInterest WHERE contactId = {}", u)).unwrap();
        // decorrelate papers
        let res = db.query_iter(&format!("SELECT paperId FROM Paper WHERE leadContactId = {} OR shepherdContactId = {} OR managerContactId = {}", u, u, u)).unwrap();
        for row in res {
            let pid : u64 = from_value(row.unwrap().unwrap()[0].clone());
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE Paper SET leadContactId = {} WHERE PaperId = {}", u, pid)).unwrap();
        }
        // decorrelate reviews
        let res = db.query_iter(&format!("SELECT reviewId FROM PaperReview WHERE contactId = {}", u)).unwrap();
        for row in res {
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperReview SET contactId = {} WHERE ReviewId = {}", u, rid)).unwrap();
        }
        // decorrelate comments
        let res = db.query_iter(&format!("SELECT commentId FROM PaperComment WHERE contactId = {}", u)).unwrap();
        for row in res {
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperComment SET contactId = {} WHERE commentId = {}", u, rid)).unwrap();
        }
        // decorrelate paper review refused
        let res = db.query_iter(&format!("SELECT * FROM PaperReviewRefused WHERE requestedBy = {}", u)).unwrap();
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperReviewRefused SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
        // decorrelate review rating
        let res = db.query_iter(&format!("SELECT * FROM ReviewRating WHERE contactId = {}", u)).unwrap();
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE ReviewRating SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
        // decorrelate action log
        let res = db.query_iter(&format!("SELECT * FROM ActionLog WHERE contactId = {} OR destContactId = {} OR trueContactId = {}", u, u, u)).unwrap();
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE ActionLog SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
        delete_durations.push(start.elapsed());
    }

    // anonymize one user at a time
    let start = time::Instant::now();
    for u in args.nusers_nonpc+12..args.nusers_nonpc+12 + 10 {
        db1.query_drop(&format!("DELETE FROM ContactInfo WHERE contactId = {}", u)).unwrap();
        
        // decorrelate paper watches
        let res = db.query_iter(&format!("SELECT * FROM PaperWatch WHERE contactId = {}", u)).unwrap(); 
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperWatch SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
        // decorrelate paper review pref
        let res = db.query_iter(&format!("SELECT * FROM PaperReviewPreference WHERE contactId = {}", u)).unwrap(); 
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperReviewPreference SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
 
        // decorrelate papers
        let res = db.query_iter(&format!("SELECT paperId FROM Paper WHERE leadContactId = {} OR shepherdContactId = {} OR managerContactId = {}", u, u, u)).unwrap();
        for row in res {
            let pid : u64 = from_value(row.unwrap().unwrap()[0].clone());
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE Paper SET leadContactId = {} WHERE PaperId = {}", u, pid)).unwrap();
        }
        // decorrelate reviews
        let res = db.query_iter(&format!("SELECT reviewId FROM PaperReview WHERE contactId = {}", u)).unwrap();
        for row in res {
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperReview SET contactId = {} WHERE ReviewId = {}", u, rid)).unwrap();
        }
        // decorrelate comments
        let res = db.query_iter(&format!("SELECT commentId FROM PaperComment WHERE contactId = {}", u)).unwrap();
        for row in res {
            let rid: u64 = from_value(row.unwrap().unwrap()[0].clone());
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperComment SET contactId = {} WHERE commentId = {}", u, rid)).unwrap();
        }
        // decorrelate paper review refused
        let res = db.query_iter(&format!("SELECT * FROM PaperReviewRefused WHERE requestedBy = {}", u)).unwrap();
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE PaperReviewRefused SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
        // decorrelate review rating
        let res = db.query_iter(&format!("SELECT * FROM ReviewRating WHERE contactId = {}", u)).unwrap();
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE ReviewRating SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
        // decorrelate action log
        let res = db.query_iter(&format!("SELECT * FROM ActionLog WHERE contactId = {} OR destContactId = {} OR trueContactId = {}", u, u, u)).unwrap();
        for _row in res {
            datagen::insert_single_user(&mut db1).unwrap();
            db1.query_drop(&format!("UPDATE ActionLog SET contactId = {} WHERE contactId = {}", u, u)).unwrap();
        }
 
    }
    anon_durations.push(start.elapsed());

    print_stats(
        nusers as u64,
        account_durations,
        anon_durations,
        edit_durations,
        delete_durations,
        vec![],
        vec![],
        vec![],
        vec![],
        true,
    );
}
fn print_stats(
    nusers: u64,
    account_durations: Vec<Duration>,
    anon_durations: Vec<Duration>,
    edit_durations: Vec<Duration>,
    delete_durations: Vec<Duration>,
    restore_durations: Vec<Duration>,
    edit_durations_preanon: Vec<Duration>,
    delete_durations_preanon: Vec<Duration>,
    restore_durations_preanon: Vec<Duration>,
    baseline: bool
) {
    let filename = if baseline {
        format!("hotcrp_disguise_stats_{}users_baseline.csv", nusers)
    } else {
        format!("hotcrp_disguise_stats_{}users.csv", nusers)
    };

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();
    writeln!(
        f,
        "{}",
        account_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        anon_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        edit_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        edit_durations_preanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations_preanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations_preanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
