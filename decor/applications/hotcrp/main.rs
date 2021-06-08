extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use log::warn;
use mysql::prelude::*;
use mysql::Conn;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::*;
use structopt::StructOpt;

mod conf_anon_disguise;
mod datagen;
mod gdpr_disguise;

use decor::{helpers, types};
use rand::seq::SliceRandom;

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";
const SCHEMA_UID_COL: &'static str = "contactId";
const SCHEMA_UID_TABLE: &'static str = "ContactInfo";

const GDPR_DISGUISE_ID: u64 = 1;
const CONF_ANON_DISGUISE_ID: u64 = 2;

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
        match test {
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
    #[structopt(long = "prime")]
    prime: bool,
    #[structopt(long = "spec")]
    spec: bool,
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

fn init_db(prime: bool, edna: &decor::EdnaClient) {
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
    let mut db = Conn::new(&url).unwrap();
    if prime {
        warn!("Priming database");
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", DBNAME))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", DBNAME))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", DBNAME)), true);
    } else {
        assert_eq!(db.select_db(&format!("{}", DBNAME)), true);
    }
}

fn run_test(disguises: Vec<Arc<types::Disguise>>, users: &Vec<u64>, prime: bool) {
    let mut file = File::create("hotcrp.out".to_string()).unwrap();
    file.write(
        "Disguise, NQueries, NQueriesVault, RecordDur, RemoveDur, DecorDur, ModDur, Duration(ms)\n"
            .as_bytes(),
    )
    .unwrap();

    let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", DBNAME);
    let mut edna = decor::EdnaClient::new(&url, SCHEMA, true);
    if prime {
        datagen::populate_database(&mut edna).unwrap();
    }
    for (i, disguise) in disguises.into_iter().enumerate() {
        let start = time::Instant::now();

        let id = disguise.disguise_id;
        edna.apply_disguise(Some(users[i]), disguise).unwrap();
        let dur = start.elapsed();

        let stats = edna.get_stats();
        let stats = stats.lock().unwrap();
        file.write(
            format!(
                "disguise{}, {}, {}, {}, {}, {}, {}\n
                Total disguise duration: {}\n",
                id,
                stats.nqueries,
                stats.nqueries_vault,
                stats.record_dur.as_millis(),
                stats.remove_dur.as_millis(),
                stats.decor_dur.as_millis(),
                stats.mod_dur.as_millis(),
                dur.as_millis()
            )
            .as_bytes(),
        )
        .unwrap();
        edna.clear_stats();
    }

    file.flush().unwrap();
}

fn main() {
    init_logger();

    let args = Cli::from_args();
    let prime = args.prime;
    let spec = args.spec;

    let disguises = vec![
        Arc::new(conf_anon_disguise::get_disguise()),
        //gdpr_disguise::get_disguise((1) as u64),
        Arc::new(gdpr_disguise::get_disguise(
            (datagen::NUSERS_NONPC + 1) as u64,
        )),
    ];
    let uids: Vec<usize> = (1..(datagen::NUSERS_PC + datagen::NUSERS_NONPC + 1)).collect();
    /*let mut rng = &mut rand::thread_rng();
    let rand_users: Vec<usize> = uids;
        .choose_multiple(&mut rng, uids.len())
        .cloned()
        .collect();
    for user in &rand_users {
        disguises.push(Arc::new(gdpr_disguise::get_disguise(*user as u64)));
    }*/

    if spec {
    } else {
        let users = vec![0 as u64, (datagen::NUSERS_NONPC + 1) as u64];
        run_test(disguises, &users, prime);
    }
}
