extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use log::warn;
use rand::rngs::OsRng;
use std::collections::{HashMap};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::*;
use structopt::StructOpt;

mod datagen;
mod disguises;

use edna::{disguise, EdnaClient};

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";

#[derive(StructOpt)]
struct Cli {
    #[structopt(long = "prime")]
    prime: bool,
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

    let args = Cli::from_args();
    let nusers = datagen::NUSERS_NONPC + datagen::NUSERS_PC;
    let mut edna = EdnaClient::new(
        args.prime,
        DBNAME,
        SCHEMA,
        true,
        nusers * 200,               // assume each user has max 200 pieces of data
        disguises::get_guise_gen(), /*in-mem*/
    );
    if args.prime {
        datagen::populate_database(&mut edna).unwrap();
    }

    let mut user_keys = HashMap::new();
    let mut rng = OsRng;
    for uid in 1..nusers + 1 {
        let private_key = edna.register_principal(uid.to_string());
        user_keys.insert(uid, private_key);
    }
}
