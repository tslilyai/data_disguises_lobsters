extern crate log;
extern crate mysql;

mod disguises;
use edna::helpers;
use mysql::prelude::*;
use mysql::Opts;
use rsa::pkcs1::ToRsaPrivateKey;
use std::collections::HashSet;
use std::sync::Arc;
use std::*;
use disguises::get_table_info;

const SCHEMA: &'static str = include_str!("./schema.sql");
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

#[test]
fn test_app_rev_anon_disguise() {
    init_logger();
    let dbname = "testRevAnon".to_string();
    let guise_gen = disguises::get_guise_gen();
    let mut edna = edna::EdnaClient::new(
        true,
        true,
        "127.0.0.1",
        &dbname,
        SCHEMA,
        true,
        USER_ITERS as usize,
        guise_gen,
    );
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);
    let tinfo = get_table_info();
    let ti = tinfo.read().unwrap();

    let mut priv_keys = vec![];

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
            db.query_drop(format!(
                r"INSERT INTO moderations (moderator_user_id, story_id, user_id,
            action) VALUES ({}, {}, {}, 'bad story!');",
                u,
                s * u + s,
                u
            ))
            .unwrap();
        }

        // register user in Edna
        let private_key = edna.register_principal(&u.to_string());
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        priv_keys.push(private_key_vec.clone());
    }

    // APPLY ANON DISGUISE
    let anon_disguise = Arc::new(disguises::universal_anon_disguise::get_disguise());
    let lcs = edna
        .apply_disguise(anon_disguise.clone(), vec![], vec![])
        .unwrap();

    // REVERSE ANON DISGUISE WITH NO PRIVATE DIFFS
    edna.reverse_disguise(anon_disguise.did, &ti, vec![], vec![])
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

    // no correlated moderations (ALL DECOR OPERATIONS ARE PRIVATE)
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

    // REVERSE DISGUISE WITH USER DIFFS
    for u in 1..USER_ITERS {
        // get diffs
        let lc = match lcs.get(&(u.to_string(), 1)) {
            Some(olc) => olc.clone(),
            None => vec![],
        };
        edna.reverse_disguise(
            anon_disguise.did,
            &ti,
            priv_keys[u as usize - 1].clone(),
            lc,
        )
        .unwrap();

        // CHECK DISGUISE RESULTS: stories have been restored too
        // stories recorrelated
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
        assert_eq!(results.len(), NSTORIES as usize);

        // moderations recorrelated
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
        assert_eq!(results.len(), NSTORIES as usize);
    }
    // CHECK AFTER ALL USERS HAVE REVERSED
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
    let guise_gen = disguises::get_guise_gen();
    let mut edna = edna::EdnaClient::new(
        true,
        true,
        "127.0.0.1",
        &dbname,
        SCHEMA,
        true,
        USER_ITERS as usize,
        guise_gen,
    );
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);
    let tinfo = get_table_info();
    let ti = tinfo.read().unwrap();

    let mut priv_keys = vec![];

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
        let private_key = edna.register_principal(&u.to_string());
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        priv_keys.push(private_key_vec.clone());
    }

    // APPLY GDPR DISGUISES
    let mut lcs = vec![];
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        let did = gdpr_disguise.did;
        let lcs_map = edna
            .apply_disguise(Arc::new(gdpr_disguise), vec![], vec![])
            .unwrap();
        match lcs_map.get(&(u.to_string(), did)) {
            Some(dlc) => lcs.push(dlc.clone()),
            None => lcs.push(vec![]),
        }
    }

    // REVERSE GDPR DISGUISES
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        edna.reverse_disguise(
            gdpr_disguise.did,
            &ti,
            priv_keys[u as usize - 1].clone(),
            lcs[u as usize - 1].clone(),
        )
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
        assert_eq!(results.len(), NSTORIES as usize);

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
fn test_app_anon_gdpr_rev_gdpr_anon_disguises() {
    init_logger();
    let dbname = "testRevCompose".to_string();
    let guise_gen = disguises::get_guise_gen();
    let mut edna = edna::EdnaClient::new(
        true,
        true,
        "127.0.0.1",
        &dbname,
        SCHEMA,
        true,
        USER_ITERS as usize,
        guise_gen,
    );
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);
    let tinfo = get_table_info();
    let ti = tinfo.read().unwrap();

    let mut priv_keys = vec![];

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
            db.query_drop(format!(
                r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');",
                u,
                s * u + s,
                u
            ))
            .unwrap();
        }

        // register user in Edna
        let private_key = edna.register_principal(&u.to_string());
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        priv_keys.push(private_key_vec.clone());
    }

    // APPLY ANON DISGUISE
    let anon_disguise = Arc::new(disguises::universal_anon_disguise::get_disguise());
    let anon_lcs_map = edna
        .apply_disguise(anon_disguise.clone(), vec![], vec![])
        .unwrap();

    // APPLY GDPR DISGUISES
    let mut gdpr_lcs = vec![];
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        let did = gdpr_disguise.did;
        let anon_lc = match anon_lcs_map.get(&(u.to_string(), anon_disguise.did)) {
            Some(lc) => lc.clone(),
            None => vec![],
        };
        let lcs_map = edna
            .apply_disguise(
                Arc::new(gdpr_disguise),
                priv_keys[u as usize - 1].clone(),
                anon_lc,
            )
            .unwrap();
        // the problem here is that lcs_map may have pseudoprincipal tokens
        // but the pseudoprincipal data in edna may have been removed (because the item was deleted)
        // we need to pass in the pseudoprincipal private keys, but those are stored ownership
        // tokens of the original user
        match lcs_map.get(&(u.to_string(), did)) {
            Some(dlc) => gdpr_lcs.push(dlc.clone()),
            None => gdpr_lcs.push(vec![]),
        }
    }

    // CHECK DISGUISE RESULTS: GDPR removes everything
    {
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
        // stories removed
        let mut stories_results = vec![];
        let res = db
            .query_iter(format!(r"SELECT user_id FROM stories"))
            .unwrap();
        for _ in res {
            stories_results.push(1);
        }
        assert_eq!(stories_results.len() as u64, 0);

        // moderations have guises as owners
        let mut guises = HashSet::new();
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
    }

    // REVERSE ANON DISGUISE WITH NO DIFFS
    edna.reverse_disguise(anon_disguise.did, &ti, vec![], vec![])
        .unwrap();

    // CHECK DISGUISE RESULTS: nothing restored
    {
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
        // stories removed
        let mut stories_results = vec![];
        let res = db
            .query_iter(format!(r"SELECT user_id FROM stories"))
            .unwrap();
        for _ in res {
            stories_results.push(1);
        }
        assert_eq!(stories_results.len() as u64, 0);

        // moderations have guises as owners
        let mut guises = HashSet::new();
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
    }

    // REVERSE GDPR DISGUISES
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        let mut lcs = gdpr_lcs[u as usize - 1].clone();
        match anon_lcs_map.get(&(u.to_string(), anon_disguise.did)) {
            Some(dlc) => lcs.extend(&mut dlc.iter().cloned()),
            None => (),
        }
        edna.reverse_disguise(
            gdpr_disguise.did,
            &ti,
            priv_keys[u as usize - 1].clone(),
            lcs,
        )
        .unwrap();
    }

    // CHECK DISGUISE RESULTS: everything restored but still anon
    // users exist
    {
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
    }

    // REVERSE DISGUISE WITH USER DIFFS
    for u in 1..USER_ITERS {
        let anon_lc = match anon_lcs_map.get(&(u.to_string(), anon_disguise.did)) {
            Some(lc) => lc.clone(),
            None => vec![],
        };
        edna.reverse_disguise(
            anon_disguise.did,
            &ti,
            priv_keys[u as usize - 1].clone(),
            anon_lc,
        )
        .unwrap();

        // CHECK DISGUISE RESULTS: stories have been restored too
        // stories recorrelated
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
        assert_eq!(results.len(), NSTORIES as usize);

        // moderations recorrelated
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
        assert_eq!(results.len(), NSTORIES as usize);
    }

    // CHECK AFTER ALL USERS HAVE REVERSED
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
fn test_app_anon_gdpr_rev_anon_gdpr_disguises() {
    init_logger();
    let tinfo = get_table_info();
    let ti = tinfo.read().unwrap();

    let dbname = "testRevComposeTwo".to_string();
    let guise_gen = disguises::get_guise_gen();
    let mut edna = edna::EdnaClient::new(
        true,
        true,
        "127.0.0.1",
        &dbname,
        SCHEMA,
        true,
        USER_ITERS as usize,
        guise_gen,
    );
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let mut priv_keys = vec![];

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
            db.query_drop(format!(
                r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ({}, {}, {}, 'bad story!');",
                u,
                s * u + s,
                u
            ))
            .unwrap();
        }

        // register user in Edna
        let private_key = edna.register_principal(&u.to_string());
        let private_key_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        priv_keys.push(private_key_vec.clone());
    }

    // APPLY ANON DISGUISE
    let anon_disguise = Arc::new(disguises::universal_anon_disguise::get_disguise());
    let anon_lcs_map = edna
        .apply_disguise(anon_disguise.clone(), vec![], vec![])
        .unwrap();

    // APPLY GDPR DISGUISES
    let mut gdpr_lcs = vec![];
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        let did = gdpr_disguise.did;
        let anon_lc = match anon_lcs_map.get(&(u.to_string(), anon_disguise.did)) {
            Some(lc) => lc.clone(),
            None => vec![],
        };
        let lcs_map = edna
            .apply_disguise(
                Arc::new(gdpr_disguise),
                priv_keys[u as usize - 1].clone(),
                anon_lc,
            )
            .unwrap();
        match lcs_map.get(&(u.to_string(), did)) {
            Some(dlc) => gdpr_lcs.push(dlc.clone()),
            None => (),
        }
    }

    // REVERSE ANON DISGUISE WITH DIFFS
    for u in 1..USER_ITERS {
        // get diffs
        let anon_lc = anon_lcs_map
            .get(&(u.to_string(), anon_disguise.did))
            .unwrap();
        edna.reverse_disguise(
            anon_disguise.did,
            &ti,
            priv_keys[u as usize - 1].clone(),
            anon_lc.clone(),
        )
        .unwrap();
    }

    // CHECK DISGUISE RESULTS: nothing restored
    {
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
        // stories removed
        let mut stories_results = vec![];
        let res = db
            .query_iter(format!(r"SELECT user_id FROM stories"))
            .unwrap();
        for _ in res {
            stories_results.push(1);
        }
        assert_eq!(stories_results.len() as u64, 0);

        // moderations have guises as owners
        let mut guises = HashSet::new();
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
    }

    // REVERSE GDPR DISGUISES
    for u in 1..USER_ITERS {
        let gdpr_disguise = disguises::gdpr_disguise::get_disguise(u);
        let mut lcs = gdpr_lcs[u as usize - 1].clone();
        match anon_lcs_map.get(&(u.to_string(), anon_disguise.did)) {
            Some(dlc) => lcs.extend(&mut dlc.iter().cloned()),
            None => (),
        }
        edna.reverse_disguise(
            gdpr_disguise.did,
            &ti,
            priv_keys[u as usize - 1].clone(),
            lcs,
        )
        .unwrap();
    }

    // CHECK DISGUISE RESULTS: everything restored but still anon
    // users exist
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
}
