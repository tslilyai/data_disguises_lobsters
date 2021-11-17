extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use log::warn;
use rand::rngs::OsRng;
use rsa::{PaddingScheme, RsaPrivateKey, RsaPublicKey};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::*;
use structopt::StructOpt;

mod disguises;
mod datagen;

use edna::{disguise, tokens};
use rand::seq::SliceRandom;

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";

const GDPR_DISGUISE_ID: u64 = 1;
const CONF_ANON_DISGUISE_ID: u64 = 2;
const RSA_BITS: usize = 2048;

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

fn run_test(disguises: Vec<Arc<disguise::Disguise>>, users: &Vec<u64>, prime: bool) {
    let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", DBNAME);
    let edna = EdnaClient::new(
            prime,
            DBNAME,
            SCHEMA,
            true,
            (nusers + 1) * nlec * 2, // generate twice as many guises as we probably need
            disguises::get_guise_gen(), /*in-mem*/
        );
    if prime {
        datagen::populate_database(&mut edna).unwrap();
    }
    let mut user_keys = HashMap::new();
    let mut rng = OsRng;
    for uid in users {
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        edna.register_principal(*uid, &pub_key);
        user_keys.insert(uid, private_key);
    }

    //file.flush().unwrap();
}

fn main() {
    init_logger();

    let args = Cli::from_args();
    let nusers = datagen::NUSERS_NONPC + datagen::NUSERS_PC;
    let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", DBNAME);
    let edna = EdnaClient::new(
            args.prime,
            DBNAME,
            SCHEMA,
            true,
            nusers * 200, // assume each user has max 200 pieces of data
            disguises::get_guise_gen(), /*in-mem*/
        );
    if args.prime {
        datagen::populate_database(&mut edna).unwrap();
    }
    let mut user_keys = HashMap::new();
    let mut rng = OsRng;
    for uid in 1..nusers+1{
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let private_key = edna.register_principal(uid.to_string());
        user_keys.insert(uid, private_key);
    }
}
