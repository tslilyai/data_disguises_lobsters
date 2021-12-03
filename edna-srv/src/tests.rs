use super::rocket;
use crate::*;
use crate::apiproxy::*;
use mysql::from_value;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::http::Status;
use rocket::local::blocking::Client;
use std::collections::{HashMap};

#[test]
fn test_disguise() {
    let matches = App::new("Edna API server")
        .arg(
            Arg::with_name("database")
                .short("d")
                .long("database-name")
                .default_value("testdb")
                .help("The MySQL database to use")
                .takes_value(true),
        )
        .arg(Arg::with_name("prime").help("Prime the database"))
        .arg(Arg::with_name("batch").help("Use token batching"))
        .arg(
            Arg::with_name("schema")
                .short("s")
                .default_value("schema.sql")
                .takes_value(true)
                .long("schema")
                .help("File containing SQL schema to use"),
        )
        .arg(
            Arg::with_name("in-memory")
                .long("memory")
                .help("Use in-memory tables."),
        )
        .arg(
            Arg::with_name("keypool-size")
                .long("keypool-size")
                .default_value("10")
                .takes_value(true),
        )
        .get_matches();

    let client = Client::tracked(
        rocket(
            true,
            matches.is_present("batch"),
            matches.value_of("database").unwrap(),
            matches.value_of("schema").unwrap(),
            matches.is_present("in-memory"),
            usize::from_str_radix(matches.value_of("keypool-size").unwrap(), 10).unwrap(),
        )
    ).expect("valid rocket instance");
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", matches.value_of("database").unwrap())).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    /*
    let mut user2decryptcap = HashMap::new();
    let mut user2owncap = HashMap::new();
    let mut user2diffcap = HashMap::new();
    let nusers : u64 = 10;

    // create all users
    for u in 0..nusers {
        let response = client
            .post("/register_principal")
            .body(u.to_string())
            .header(ContentType::JSON)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        let body: RegisterPrincipalResponse = serde_json::from_str(&mut response.body().to_string().await.unwrap()).unwrap();
        user2decryptcap.insert(u, body.privkey);
    }

    /**********************************
     * decay  
     ***********************************/
    // decay 
    for u in 0..nusers {
        let response = client.post(&format!("/apply_disguise/1/{}", u)).dispatch();
        let body: ApplyDisguiseResponse = serde_json::from_str(&mut response.body().to_string().await.unwrap()).unwrap();
        if let Some(dl) = body.diff_locators.get(&(u.to_string(), 0)) {
            user2diffcap.insert(u, *dl);
        }
        if let Some(ol) = body.ownership_locators.get(&(u.to_string(), 0)) {
            user2owncap.insert(u, *ol);
        }
    }
    
    // check results of decay: user has no comments
    for u in 0..nusers {
        let res = db.query_iter(format!("SELECT * FROM comments WHERE user_id = {};", u)).unwrap();
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
     * gdpr deletion (with composition)
     ***********************************/
    for u in 0..nusers {
        let owncap = user2owncap.get(&u).unwrap();
        let decryptcap = user2decryptcap.get(&u).unwrap();
                
        let postdata = serde_json::to_string(&vec![
            ("decryption_cap", decryptcap),
            ("ownership_locators", &format!("{}", owncap)),
        ])
        .unwrap();

        let response = client
            .post(&format!("/apply_disguise/0/{}", u))
            .body(postdata)
            .header(ContentType::JSON)
            .dispatch();
    }
    // check results of delete: no answers or users exist

    /***********************************
     * gdpr restore (with composition)
     ***********************************/
    for u in 0..nusers {
        let owncap = user2owncap.get(&u).unwrap();
        let decryptcap = user2decryptcap.get(&u).unwrap();
        let diffcap = user2diffcap.get(&u).unwrap();
        let postdata = serde_json::to_string(&vec![
            ("diff_loc_cap", diffcap.to_string()),
            ("decryption_cap", decryptcap.to_string()),
            ("ownership_loc_caps", format!("{}", owncap)),
        ])
        .unwrap();

        let response = client
            .post("/reveal_disguise/0")
            .body(postdata)
            .header(ContentType::JSON)
            .dispatch();
    }
    // check 
    */
}
