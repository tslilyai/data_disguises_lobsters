extern crate log;
extern crate mysql;

mod disguises;
use decor::helpers;
use decor::tokens;
use mysql::prelude::*;
use rand::rngs::OsRng;
use rsa::{PaddingScheme, RsaPrivateKey, RsaPublicKey};
use std::collections::HashSet;
use std::sync::Arc;
use std::*;

const SCHEMA: &'static str = include_str!("./schema.sql");
const RSA_BITS: usize = 2048;
const USER_ITERS: u64 = 2;
const NSTORIES: u64 = 2;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Debug)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

/*
#[test]
fn test_app_rev_anon_disguise() {
    init_logger();
    let dbname = "testRevAnon".to_string();
    let mut edna = decor::EdnaClient::new(true, &dbname, SCHEMA, true);
    let mut db = mysql::Conn::new(&format!("mysql://tslilyai:pass@127.0.0.1/{}", &dbname)).unwrap();
    assert_eq!(db.ping(), true);

    let mut rng = OsRng;
    let mut priv_keys = vec![];
    let mut pub_keys = vec![];

    // INITIALIZATION
    for u in 1..USER_ITERS {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
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

    // APPLY ANON DISGUISES
    let anon_disguise = Arc::new(disguises::universal_anon_disguise::get_disguise());
    edna.apply_disguise(anon_disguise.clone(), HashSet::new())
        .unwrap();

    // REVERSE ANON DISGUISE WITH NO USER TOKENS
    edna.reverse_disguise(anon_disguise.clone(), HashSet::new())
        .unwrap();

    // CHECK DISGUISE RESULTS: moderations have been restored
    // users exist
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT * FROM users WHERE users.username='hello{}'",
                u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let id = helpers::mysql_val_to_string(&vals[0]);
            let username = helpers::mysql_val_to_string(&vals[1]);
            let karma = helpers::mysql_val_to_string(&vals[2]);
            results.push((id, username, karma));
        }
        assert_eq!(results.len(), 1);
    }

    // no correlated stories
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(r"SELECT id FROM stories WHERE user_id={}", u))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 0);
    }

    // moderations recorrelated
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                u, u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 1);
    }

    let mut guises = HashSet::new();

    // stories have guises as owners
    let mut stories_results = vec![];
    let res = db
        .query_iter(format!(r"SELECT user_id FROM stories"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(guises.insert(user_id));
        assert!(user_id >= USER_ITERS);
        stories_results.push(user_id);
    }
    assert_eq!(stories_results.len() as u64, (USER_ITERS - 1) * NSTORIES);

    // moderations have no guises as owners
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(user_id < USER_ITERS);
        assert!(moderator_user_id < USER_ITERS);
    }

    // check that all guises exist
    for u in guises {
        let res = db
            .query_iter(format!(r"SELECT * FROM users WHERE id={}", u))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let username = helpers::mysql_val_to_string(&vals[1]);
            assert_eq!(username, format!("{}", u));
        }
    }

    // REVERSE DISGUISE WITH USER TOKENS
    let mut hs = HashSet::new();
    for u in 1..USER_ITERS {
        // get tokens
        let esymks = edna.get_encrypted_symkeys_of_disguises(u, vec![1]);
        assert_eq!(esymks.len(), 1);
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey = priv_keys[u as usize - 1]
            .decrypt(padding, &esymks[0].enc_symkey)
            .expect("failed to decrypt");
        hs.insert(tokens::ListSymKey {
            uid: u,
            did: 1,
            symkey: symkey,
        });
    }
    edna.reverse_disguise(anon_disguise.clone(), hs).unwrap();

    // CHECK DISGUISE RESULTS: stories have been restored too
    // stories recorrelated
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(r"SELECT id FROM stories WHERE user_id={}", u))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 1);
    }

    // moderations recorrelated
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                u, u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 1);
    }

    // stories have no guises as owners
    let mut stories_results = vec![];
    let res = db
        .query_iter(format!(r"SELECT user_id FROM stories"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(user_id < USER_ITERS);
        stories_results.push(user_id);
    }
    assert_eq!(stories_results.len() as u64, (USER_ITERS - 1) * NSTORIES);

    // moderations have no guises as owners
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(user_id < USER_ITERS);
        assert!(moderator_user_id < USER_ITERS);
    }

    // guises are all gone
    let res = db.query_iter(format!(r"SELECT id FROM users")).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(id < USER_ITERS);
    }

    drop(db);
}

#[test]
fn test_app_rev_gdpr_disguise() {
    init_logger();
    let dbname = "testRevGDPR".to_string();
    let mut edna = decor::EdnaClient::new(true, &dbname, SCHEMA, true);
    let mut db = mysql::Conn::new(&format!("mysql://tslilyai:pass@127.0.0.1/{}", &dbname)).unwrap();
    assert_eq!(db.ping(), true);

    let mut rng = OsRng;
    let mut priv_keys = vec![];
    let mut pub_keys = vec![];

    // INITIALIZATION
    for u in 1..USER_ITERS {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
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

    // APPLY GDPR DISGUISES
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        edna.apply_disguise(Arc::new(gdpr_disguise), HashSet::new())
            .unwrap();
    }

    // REVERSE GDPR DISGUISES
    for u in 1..USER_ITERS {
        // get tokens
        let esymks = edna.get_encrypted_symkeys_of_disguises(u, vec![0]);
        assert_eq!(esymks.len(), 1);
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey = priv_keys[u as usize - 1]
            .decrypt(padding, &esymks[0].enc_symkey)
            .expect("failed to decrypt");
        let mut hs = HashSet::new();
        hs.insert(tokens::ListSymKey {
            uid: u,
            did: 1,
            symkey: symkey,
        });
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        edna.reverse_disguise(Arc::new(gdpr_disguise), HashSet::new())
            .unwrap();

        // CHECK DISGUISE RESULTS
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT * FROM users WHERE users.username='hello{}'",
                u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let id = helpers::mysql_val_to_string(&vals[0]);
            let username = helpers::mysql_val_to_string(&vals[1]);
            let karma = helpers::mysql_val_to_string(&vals[2]);
            assert_eq!(id, u.to_string());
            results.push((id, username, karma));
        }
        assert_eq!(results.len(), 1);

        // recorrelated moderations
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                u, u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 1);

        // stories present
        let mut stories_results = vec![];
        let res = db
            .query_iter(format!(r"SELECT user_id FROM stories"))
            .unwrap();
        for _ in res {
            stories_results.push(1);
        }
        assert_eq!(stories_results.len() as u64, NSTORIES);
    }

    // guises are all gone
    let res = db.query_iter(format!(r"SELECT id FROM users")).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(id < USER_ITERS);
    }

    drop(db);
}

#[test]
/*fn test_compose_anon_gdpr_rev_gdpr_disguises() {
    init_logger();
    let dbname = "testRevCompose".to_string();
    let mut edna = decor::EdnaClient::new(true, &dbname, SCHEMA, true);
    let mut db = mysql::Conn::new(&format!("mysql://tslilyai:pass@127.0.0.1/{}", &dbname)).unwrap();
}
*/
#[test]
fn test_compose_anon_gdpr_rev_anon_disguises() {
    init_logger();
    let dbname = "testRevCompose".to_string();
    let mut edna = decor::EdnaClient::new(true, &dbname, SCHEMA, true);
    let mut db = mysql::Conn::new(&format!("mysql://tslilyai:pass@127.0.0.1/{}", &dbname)).unwrap();
    assert_eq!(db.ping(), true);

    let mut rng = OsRng;
    let mut priv_keys = vec![];
    let mut pub_keys = vec![];

    // INITIALIZATION
    for u in 1..USER_ITERS {
        // insert user into DB
        db.query_drop(format!(
            r"INSERT INTO users (id, username) VALUES ({}, 'hello{}');",
            u, u
        ))
        .unwrap();

        // insert a bunch of data for each user
        for s in 0..NSTORIES {
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

    // APPLY ANON DISGUISE
    let anon_disguise = Arc::new(disguises::universal_anon_disguise::get_disguise());
    edna.apply_disguise(anon_disguise.clone(), HashSet::new())
        .unwrap();

    // APPLY GDPR DISGUISES
    for u in 1..USER_ITERS {
        // get tokens
        let esymks = edna.get_encrypted_symkeys_of_disguises(u, vec![1]);
        assert_eq!(esymks.len(), 1);
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let symkey = priv_keys[u as usize - 1]
            .decrypt(padding, &esymks[0].enc_symkey)
            .expect("failed to decrypt");
        let mut hs = HashSet::new();
        hs.insert(tokens::ListSymKey {
            uid: u,
            did: 1,
            symkey: symkey,
        });
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        edna.apply_disguise(Arc::new(gdpr_disguise), hs).unwrap();
    }

    // TODO REVERSE ANON DISGUISE

    // CHECK DISGUISE RESULTS
    // users removed
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT * FROM users WHERE users.username='hello{}'",
                u
            ))
            .unwrap();
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
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(
                r"SELECT id FROM moderations WHERE moderator_user_id={} OR user_id={}",
                u, u
            ))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let id = helpers::mysql_val_to_string(&vals[0]);
            results.push(id);
        }
        assert_eq!(results.len(), 0);
    }
    // no correlated stories
    for u in 1..USER_ITERS {
        let mut results = vec![];
        let res = db
            .query_iter(format!(r"SELECT id FROM stories WHERE user_id={}", u))
            .unwrap();
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
    let res = db
        .query_iter(format!(r"SELECT user_id FROM stories"))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        assert!(guises.insert(user_id));
        assert!(user_id >= USER_ITERS);
        stories_results.push(user_id);
    }
    assert_eq!(stories_results.len() as u64, (USER_ITERS - 1) * NSTORIES);

    // moderations have guises as owners
    let res = db
        .query_iter(format!(
            r"SELECT moderator_user_id, user_id FROM moderations"
        ))
        .unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let moderator_user_id = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        let user_id = helpers::mysql_val_to_u64(&vals[1]).unwrap();
        assert!(guises.insert(user_id));
        assert!(guises.insert(moderator_user_id));
        assert!(user_id >= USER_ITERS);
        assert!(moderator_user_id >= USER_ITERS);
    }

    // check that all guises exist
    for u in guises {
        let res = db
            .query_iter(format!(r"SELECT * FROM users WHERE id={}", u))
            .unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let username = helpers::mysql_val_to_string(&vals[1]);
            assert_eq!(username, format!("{}", u));
        }
    }
    drop(db);
}*/
