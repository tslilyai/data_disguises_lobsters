extern crate log;
extern crate mysql;

mod gdpr_disguise;
use mysql::prelude::*;
use rand::rngs::OsRng;
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::sync::Arc;
use std::collections::HashSet;
use std::*;
use decor::helpers;

const SCHEMA: &'static str = include_str!("./schema.sql");
const DBNAME: &'static str = "test_disg";
const RSA_BITS: usize = 2048;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[test]
fn test_app_gdpr_disguise() {
    init_logger();
    let mut edna = decor::EdnaClient::new(true, DBNAME, SCHEMA, true);
    let mut db = mysql::Conn::new(&format!("mysql://tslilyai:pass@127.0.0.1/{}", DBNAME)).unwrap();
    assert_eq!(db.ping(), true);

    /*
     * NOTE: the column types are all right, but the mysql value returned is always Bytes,
     * so it always parses as a String
     */

    /*
     * TEST 1: insert user and register user
     */
    let mut rng = OsRng;
    let mut priv_keys = vec![];
    let mut pub_keys = vec![];

    let user_iters = 2;
    let nstories = 2;
    for u in 1..user_iters {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..nstories {
            db.query_drop(format!(
                r"INSERT INTO stories (id, user_id) VALUES ({}, {});",
                u * s + s,
                u
            ))
            .unwrap();
            db.query_drop(format!(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');", u, s*u + s, u)).unwrap();
        }

        // register user in Edna
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        edna.register_principal(u, &pub_key);
        pub_keys.push(pub_key.clone());
        priv_keys.push(private_key.clone());
    }
    for u in 1..user_iters {
        let gdpr_disguise = gdpr_disguise::get_disguise(u);
        edna.apply_disguise(Arc::new(gdpr_disguise), vec![])
            .unwrap();
    }

    // users removed
    for u in 1..user_iters {
        let mut results = vec![];
        let res = db.query_iter(format!(r"SELECT * FROM users WHERE users.username='hello{}'", u)).unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let id = helpers::mysql_val_to_string(&vals[0]);
            let username = helpers::mysql_val_to_string(&vals[1]);
            let karma = helpers::mysql_val_to_string(&vals[2]);
            results.push((id, username, karma));
        }
        assert_eq!(results.len(), 0);
    }
    // no correlated moderations
    for u in 1..user_iters {
        let mut results = vec![];
        let res = db.query_iter(format!(r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}", u, u)).unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 0);
    }
    // no correlated stories
    for u in 1..user_iters {
        let mut results = vec![];
        let res = db.query_iter(format!(r"SELECT id FROM stories WHERE user_id={}", u)).unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 0);
    }

    let mut guises = HashSet::new();
    
    // stories have guises as owners
    let mut stories_results = vec![];
    let res = db.query_iter(format!(r"SELECT user_id FROM stories")).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(guises.insert(user_id));
        assert!(user_id >= user_iters);
        stories_results.push(user_id);
    }
    assert_eq!(stories_results.len() as u64, (user_iters-1)*nstories);
    
    // moderations have guises as owners
    let res = db.query_iter(format!(r"SELECT moderator_user_id, user_id FROM moderations")).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(guises.insert(user_id));
        assert!(guises.insert(moderator_user_id));
        assert!(user_id >= user_iters);
        assert!(moderator_user_id >= user_iters);
    }

    // check that all guises exist
    for u in guises {
        let res = db.query_iter(format!(r"SELECT * FROM users WHERE id={}", u)).unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let username = helpers::mysql_val_to_string(&vals[1]);
            assert_eq!(username, format!("{}", u));
        }
    }
    //edna.get_encrypted_symkeys_of_disguises();

    //pub fn register_principal(&mut self, uid: u64, pubkey: &RsaPublicKey) {
    //pub fn get_encrypted_symkeys_of_disguises(
    //pub fn get_tokens_of_disguise_keys(
    //pub fn apply_disguise(
    //pub fn reverse_disguise(

    drop(db);
}
