extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use log::warn;
use mysql::prelude::*;
use mysql::{Pool, Opts, Conn};
use std::fs::File;
use std::io::Write;
use std::*;
use structopt::StructOpt;
use std::sync::{Mutex, Arc};

mod conf_anon_disguise;
mod datagen;
mod gdpr_disguise;

use decor::stats::QueryStat;
use decor::{vault, spec, types};
use rand::seq::SliceRandom;

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

fn init_db(prime: bool) -> Pool {
    let test_dbname = format!("{}", DBNAME);
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
    let mut db = Conn::new(&url).unwrap();
    if prime {
        warn!("Priming database");
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        datagen::populate_database(&mut db).unwrap();
    }
    else {
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        datagen::populate_database(&mut db).unwrap();
    }

    let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", test_dbname);
    let opts = Opts::from_url(&url).unwrap();
    Pool::new(opts).unwrap()
}

fn run_test(pool: &mysql::Pool, disguises: Vec<types::Disguise>, users: &Vec<u64>) {
    let mut file = File::create("hotcrp.out".to_string()).unwrap();
   file.write("Disguise, NQueries, NQueriesVault, RecordDur, RemoveDur, DecorDur, ModDur, Duration(ms)\n".as_bytes())
        .unwrap();  
   for (i, disguise) in disguises.into_iter().enumerate() {
        let stats = Arc::new(Mutex::new(QueryStat::new()));
        let start = time::Instant::now();

        let id = disguise.disguise_id;
        decor::disguise::apply(Some(users[i]), disguise, pool.clone(), stats.clone()).unwrap();
        let dur = start.elapsed();
  
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
    }
    
    file.flush().unwrap();

    vault::print_as_filters(&mut pool.get_conn().unwrap()).unwrap();
}

fn main() {
    init_logger();

    let args = Cli::from_args();
    let prime = args.prime;
    let spec = args.spec;

    let disguises = vec![
        conf_anon_disguise::get_disguise(),
        //gdpr_disguise::get_disguise((1) as u64),
        gdpr_disguise::get_disguise((datagen::NUSERS_NONPC+1) as u64),
    ];
    let uids: Vec<usize> = (1..(datagen::NUSERS_PC + datagen::NUSERS_NONPC + 1)).collect();
    /*let mut rng = &mut rand::thread_rng();
    let rand_users: Vec<usize> = uids;
        .choose_multiple(&mut rng, uids.len())
        .cloned()
        .collect();
    for user in &rand_users {
        disguises.push(gdpr_disguise::get_disguise(*user as u64));
    }*/

    if spec {
    } else {
        let pool = init_db(prime);
        let users = vec![0 as u64, (datagen::NUSERS_NONPC+1) as u64];
        run_test(&pool, disguises, &users);
        drop(pool);
    }
}
