extern crate hwloc;
extern crate libc;
extern crate log;
extern crate mysql;
extern crate rand;

use log::warn;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use rsa::{PaddingScheme, RsaPrivateKey, RsaPublicKey};
use std::*;
use std::collections::{HashSet, HashMap};
use structopt::StructOpt;
use rand::{rngs::OsRng};

mod conf_anon_disguise;
mod datagen;
mod gdpr_disguise;

use decor::{disguise, tokens};
use rand::seq::SliceRandom;

const SCHEMA: &'static str = include_str!("schema.sql");
const DBNAME: &'static str = &"test_hotcrp";
const SCHEMA_UID_COL: &'static str = "contactId";
const SCHEMA_UID_TABLE: &'static str = "ContactInfo";

const GDPR_DISGUISE_ID: u64 = 1;
const CONF_ANON_DISGUISE_ID: u64 = 2;
const RSA_BITS: usize = 2048;

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
    let mut file = File::create("hotcrp.out".to_string()).unwrap();
    file.write(
        "Disguise, NQueries, NQueriesVault, RecordDur, RemoveDur, DecorDur, ModDur, Duration(ms)\n"
            .as_bytes(),
    )
    .unwrap();

    let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", DBNAME);
    let mut edna = decor::EdnaClient::new(prime, &url, SCHEMA, true);
    decor::init_db(prime, true, DBNAME, SCHEMA);
    if prime {
        datagen::populate_database(&mut edna).unwrap();
    }
    let mut user_keys = HashMap::new();
    let mut rng = OsRng;
    for uid in users {
        let private_key =
            RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        edna.register_principal(*uid, &pub_key);
        user_keys.insert(uid, private_key);
    }
    for (i, disguise) in disguises.into_iter().enumerate() {
        let start = time::Instant::now();
        let user = users[i];
        let did = disguise.did;

        let enckeys = edna.get_encrypted_symkeys_of_disguises(user, vec![did]);
        let mut symkeys = HashSet::new();

        for ek in enckeys {
            let padding = PaddingScheme::new_pkcs1v15_encrypt();
            let symkey = user_keys[&ek.uid] 
                .decrypt(padding, &ek.enc_symkey)
                .expect("failed to decrypt");
            symkeys.insert(tokens::ListSymKey {
                uid: user,
                did: did,
                symkey: symkey,
            });
        }
        
        let tokens = edna.get_tokens_of_disguise_keys(symkeys, true);
        edna.apply_disguise(disguise, tokens).unwrap();
        let dur = start.elapsed();

        let stats = edna.get_stats();
        let stats = stats.lock().unwrap();
        file.write(
            format!(
                "disguise{}, {}, {}, {}, {}, {}, {}\n
                Total disguise duration: {}\n",
                did,
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
        drop(stats);
        edna.clear_stats();
    }

    file.flush().unwrap();
}

fn main() {
    init_logger();

    let args = Cli::from_args();
    let prime = args.prime;

    let disguises = vec![
        Arc::new(conf_anon_disguise::get_disguise()),
        Arc::new(gdpr_disguise::get_disguise(
            (datagen::NUSERS_NONPC + 1) as u64,
        )),
    ];
    /*let uids: Vec<usize> = (1..(datagen::NUSERS_PC + datagen::NUSERS_NONPC + 1)).collect();
    let mut rng = &mut rand::thread_rng();
    let rand_users: Vec<usize> = uids;
        .choose_multiple(&mut rng, uids.len())
        .cloned()
        .collect();
    for user in &rand_users {
        disguises.push(Arc::new(gdpr_disguise::get_disguise(*user as u64)));
    }*/

    let users = vec![0 as u64, (datagen::NUSERS_NONPC + 1) as u64];
    run_test(disguises, &users, prime);
}
