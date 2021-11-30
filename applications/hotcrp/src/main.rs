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

mod datagen;
mod disguises;

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(long = "prime")]
    prime: bool,
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
        .filter_level(log::LevelFilter::Warn)
        //.filter_level(log::LevelFilter::Error)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

fn main() {
    init_logger();
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];
    let mut restore_durations = vec![];

    let args = Cli::from_args();
    let nusers = args.nusers_nonpc + args.nusers_pc;
    let mut edna = EdnaClient::new(
        args.prime,
        DBNAME,
        SCHEMA,
        true,
        nusers * 2, // assume each user has approx 5 pieces of data
        disguises::get_guise_gen(), /*in-mem*/
    );
    if args.prime {
        datagen::populate_database(&mut edna, &args).unwrap();
    }

    warn!("database populated!");

    let mut decrypt_caps = HashMap::new();
    for uid in 1..nusers + 1 {
        let start = time::Instant::now();
        let private_key = edna.register_principal(uid.to_string());
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        decrypt_caps.insert(uid as usize, private_key_vec);
        account_durations.push(start.elapsed());
    }

    // anonymize
    let start = time::Instant::now();
    // anonymization doesn't produce diff tokens that we'll reuse later
    let (_diff_locs, own_locs) = disguises::conf_anon_disguise::apply(&mut edna).unwrap();
    anon_durations.push(start.elapsed());

    // edit/delete/restore for pc members 
    for u in args.nusers_nonpc+1..nusers+1 {
        let dc = decrypt_caps.get(&u).unwrap().to_vec();
        let ol = vec![*own_locs
            .get(&(
                u.to_string(),
                disguises::conf_anon_disguise::get_disguise_id(),
            ))
            .unwrap()];

        // edit
        let start = time::Instant::now();
        let pps = edna.get_pseudoprincipals(dc.clone(), ol.clone());
        edit_durations.push(start.elapsed());

        // delete
        let start = time::Instant::now();
        let (gdpr_diff_locs, gdpr_own_locs) =
            disguises::gdpr_disguise::apply(&mut edna, u as u64, dc.clone(), ol).unwrap();
        delete_durations.push(start.elapsed());

        // restore
        let start = time::Instant::now();

        let dl = vec![*gdpr_diff_locs
            .get(&(
                u.to_string(),
                disguises::gdpr_disguise::get_disguise_id(),
            ))
            .unwrap()];
        let ol = vec![*gdpr_own_locs
            .get(&(
                u.to_string(),
                disguises::gdpr_disguise::get_disguise_id(),
            ))
            .unwrap()];
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
    );
}

fn print_stats(
    nusers: u64,
    account_durations: Vec<Duration>,
    anon_durations: Vec<Duration>,
    edit_durations: Vec<Duration>,
    delete_durations: Vec<Duration>,
    restore_durations: Vec<Duration>,
) {
    let filename = format!("disguise_stats_{}users.csv", nusers);

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
}
