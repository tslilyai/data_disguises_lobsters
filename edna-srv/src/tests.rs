use super::rocket;
use crate::apiproxy::*;
use crate::*;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::local::asynchronous::Client;
use std::collections::HashMap;
use serde_json::json;
use log::warn;

pub async fn test_lobsters_disguise() {
    let client = Client::tracked(rocket(
        true,
        "127.0.0.1",
        "testdb",
        "src/lobsters_disguises/schema.sql",
        true,
        10,
        LOBSTERS_APP,
    ))
    .await
    .unwrap();
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!(
            "mysql://tslilyai:pass@{}/{}",
            "mariadb",
            "testdb"
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let mut user2decryptcap = HashMap::new();
    let mut user2caps = HashMap::new();
    let nusers: u64 = 5;
    let nstories: u64 = 5;
    let ncomments: u64 = 5;
    let mut short_id = 1;

    // create all users
    for u in 1..nusers {
        let response = client
            .post("/register_principal")
            .body(json!(u.to_string()).to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        let body: RegisterPrincipalResponse = serde_json::from_str(&strbody).unwrap();
        user2decryptcap.insert(u, body.privkey);
        db.query_drop(format!("INSERT INTO users (id, username) VALUES ({}, '{}name');", u,u)).unwrap();

        // create some number of comments and stories for each user
        for s in 0..nstories {
            short_id = short_id+1;
            db.query_drop(format!("INSERT INTO stories (short_id, user_id, url, title, created_at) VALUES ({}, {}, '{}url', '{}title', '2019-08-15 00:00:00.000');", 
                        short_id, u, s, s)).unwrap();
            for _ in 0..ncomments {
                short_id = short_id+1;
                db.query_drop(format!("INSERT INTO comments (short_id, story_id, user_id, comment, created_at) VALUES ({}, {}, {}, '{}comment', '2019-08-15 00:00:00.000');", 
                        short_id, s, u, short_id)).unwrap();
            }
        }
    }
    warn!("Created {} users", nusers);

    /***********************************
     * gdpr deletion (no composition)
     ***********************************/
    for u in 1..nusers {
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": base64::decode(&decryptcap).unwrap(),
            "locators": [],
        });

        let endpoint = format!("/apply_disguise/{}/0/{}", LOBSTERS_APP, u);
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Delete strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.locators.get(&u.to_string()) {
            user2caps.insert(u, dl.clone());
        }
        warn!("Deleted account of {}", u);
    }
    // check results of delete: user has no comments
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM stories WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM users WHERE id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
    }
    // TODO more checks for anon

    /***********************************
     * gdpr restore (no composition)
     ***********************************/
    for u in 1..nusers {
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let diffcap = user2caps.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": base64::decode(&decryptcap).unwrap(),
            "locators": [diffcap],
        });

        client
            .post("/reveal_disguise/0")
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        warn!("Restored account of {}", u);
    }
    // check
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), ncomments as usize*nstories as usize);
        let res = db
            .query_iter(format!("SELECT * FROM stories WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), nstories as usize);
        let res = db
            .query_iter(format!("SELECT * FROM users WHERE id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 1);
    }

    /**********************************
     * decay
     ***********************************/
    // decay
    for u in 1..nusers {
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": base64::decode(&decryptcap).unwrap(),
            "locators": [],
        });

        let endpoint = format!("/apply_disguise/{}/1/{}", LOBSTERS_APP, u);
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Decay strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.locators.get(&u.to_string()) {
            user2caps.insert(u, dl.clone());
        }
        warn!("Decayed account of {}", u);
    }

    // check results of decay: user has no associated data
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM stories WHERE user_id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
        let res = db
            .query_iter(format!("SELECT * FROM users WHERE id = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
    }
     /***********************************
     * gdpr deletion (composition)
     ***********************************/

    /***********************************
     * gdpr restore (composition)
     ***********************************/
    // check
}

pub async fn test_hotcrp_disguise() {
    let client = Client::tracked(rocket(
        true,
        "127.0.0.1",
        "testdb",
        "src/hotcrp_disguises/schema.sql",
        true,
        10,
        HOTCRP_APP,
    ))
    .await
    .unwrap();
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!(
            "mysql://tslilyai:pass@127.0.0.1/{}",
            "testdb"
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let mut user2decryptcap = HashMap::new();
    let mut user2caps = HashMap::new();
    let nusers: u64 = 5;

    // create all users
    for u in 1..nusers {
        let response = client
            .post("/register_principal")
            .body(json!(u.to_string()).to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        let body: RegisterPrincipalResponse = serde_json::from_str(&strbody).unwrap();
        user2decryptcap.insert(u, body.privkey);
        // TODO datagen
    }
    warn!("Created {} users", nusers);

    /***********************************
     * gdpr deletion (no composition)
     ***********************************/
    for u in 1..nusers {
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": base64::decode(&decryptcap).unwrap(),
            "locators": [],
        });

        let endpoint = format!("/apply_disguise/{}/0/{}", HOTCRP_APP, u);
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Delete strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.locators.get(&u.to_string()) {
            user2caps.insert(u, dl.clone());
        }
        warn!("Deleted account of {}", u);
    }
    // check results of delete: user has no comments
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM ContactInfo WHERE contactId = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 0);
    }

    /***********************************
     * gdpr restore (no composition)
     ***********************************/
    for u in 1..nusers {
        //let owncap = user2owncap.get(&u).unwrap();
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let diffcap = user2caps.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": base64::decode(&decryptcap).unwrap(),
            "locators": [diffcap],
        });

        client
            .post("/reveal_disguise/0")
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        warn!("Restored account of {}", u);
    }
    // check
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM ContactInfo WHERE contactId = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 1);
    }

    /**********************************
     * anon 
     ***********************************/
    // anon 
    for u in 1..nusers {
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": base64::decode(&decryptcap).unwrap(),
            "locators": [],
        });

        let endpoint = format!("/apply_disguise/{}/1/{}", HOTCRP_APP, u);
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Anon strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.locators.get(&u.to_string()) {
            user2caps.insert(u, dl.clone());
        }
        warn!("Anon account of {}", u);
    }

    // check results of decay: user has no associated data
    for u in 1..nusers {
        let res = db
            .query_iter(format!("SELECT * FROM ContactInfo WHERE contactId = {};", u))
            .unwrap();
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        assert_eq!(rows.len(), 1);
    }
     /***********************************
     * gdpr deletion (composition)
     ***********************************/

    /***********************************
     * gdpr restore (composition)
     ***********************************/
    // check
}
