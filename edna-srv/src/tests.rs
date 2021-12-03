use super::rocket;
use crate::apiproxy::*;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::local::asynchronous::Client;
use std::collections::HashMap;
use serde_json::json;
use log::warn;

pub async fn test_disguise() {
    let client = Client::tracked(rocket(
        true,
        true,
        "testdb",
        "lobsters_disguises/schema.sql",
        true,
        10,
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
    let mut user2owncap = HashMap::new();
    let mut user2diffcap = HashMap::new();
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
        for s in 1..nstories {
            short_id = short_id+1;
            db.query_drop(format!("INSERT INTO stories (short_id, user_id, url, title, created_at) VALUES ({}, {}, '{}url', '{}title', '2019-08-15 00:00:00.000');", 
                        short_id, u, s, s)).unwrap();
            for _ in 1..ncomments {
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
            "decrypt_cap": decryptcap.as_bytes(),
            "ownership_locators": [],
        });

        let endpoint = format!("/apply_disguise/0/{}", u);
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        warn!("Delete strbody response: {}", strbody);
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.diff_locators.get(&u.to_string()) {
            user2diffcap.insert(u, *dl);
        }
        if let Some(ol) = body.ownership_locators.get(&u.to_string()) {
            user2owncap.insert(u, *ol);
        }
        warn!("Deleted account of {}", u);
    }
    // check results of delete: no answers or users exist

    /***********************************
     * gdpr restore (no composition)
     ***********************************/
    for u in 1..nusers {
        let owncap = user2owncap.get(&u).unwrap();
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let diffcap = user2diffcap.get(&u).unwrap();
        let postdata = json!({
            "decrypt_cap": decryptcap.as_bytes(),
            "diff_locators": [diffcap],
            "ownership_locators": [owncap],
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

    /**********************************
     * decay
     ***********************************/
    // decay
    for u in 1..nusers {
        let endpoint = format!("/apply_disguise/1/{}", u);
        let response = client.post(&endpoint).dispatch().await;
        let strbody = response.into_string().await.unwrap();
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.diff_locators.get(&u.to_string()) {
            user2diffcap.insert(u, *dl);
        }
        if let Some(ol) = body.ownership_locators.get(&u.to_string()) {
            user2owncap.insert(u, *ol);
        }
        warn!("Decayed account of {}", u);
    }

    // check results of decay: user has no comments
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
    }
    // TODO more checks

    /***********************************
     * gdpr deletion (composition)
     ***********************************/
    for u in 0..nusers {
        let owncap = user2owncap.get(&u).unwrap();
        let decryptcap = user2decryptcap.get(&u).unwrap();

        let postdata = json!({
            "decryption_cap": decryptcap,
            "ownership_locators": [owncap]
        });

        let endpoint = format!("/apply_disguise/0/{}", u);
        let response = client
            .post(&endpoint)
            .body(postdata.to_string())
            .header(ContentType::JSON)
            .dispatch()
            .await;
        let strbody = response.into_string().await.unwrap();
        let body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        if let Some(dl) = body.diff_locators.get(&u.to_string()) {
            user2diffcap.insert(u, *dl);
        }
        if let Some(ol) = body.ownership_locators.get(&u.to_string()) {
            user2owncap.insert(u, *ol);
        }
    }
    // check results of delete: no answers or users exist

    /***********************************
     * gdpr restore (composition)
     ***********************************/
    for u in 1..nusers {
        let owncap = user2owncap.get(&u).unwrap();
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let diffcap = user2diffcap.get(&u).unwrap();
        let postdata = serde_json::to_string(&vec![
            ("diff_loc_cap", diffcap.to_string()),
            ("decryption_cap", decryptcap.to_string()),
            ("ownership_locators", format!("{}", owncap)),
        ])
        .unwrap();

        client
            .post("/reveal_disguise/0")
            .body(postdata)
            .header(ContentType::JSON)
            .dispatch()
            .await;
    }
    // check
}
