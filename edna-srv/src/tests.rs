use super::rocket;
use crate::*;
use mysql::from_value;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::http::Status;
use rocket::local::blocking::Client;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};

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
            matches.is_present("prime"),
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

    /*let mut user2apikey = HashMap::new();
    let mut user2decryptcap = HashMap::new();
    let mut user2owncap = HashMap::new();
    let mut user2diffcap = HashMap::new();

    // create all users
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let postdata = serde_urlencoded::to_string(&vec![("email", email.clone())]).unwrap();
        let response = client
            .post("/apikey/generate")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);

        // get api key
        let file = File::open(format!("{}.{}", email, APIKEY_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut apikey = String::new();
        buf_reader.read_to_string(&mut apikey).unwrap();
        user2apikey.insert(email.clone(), apikey);

        // get decryption cap
        let file = File::open(format!("{}.{}", email, DECRYPT_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut decryptcap = String::new();
        buf_reader.read_to_string(&mut decryptcap).unwrap();
        user2decryptcap.insert(email, decryptcap);
    }

    /**********************************
     * anonymization
     ***********************************/
    // login as the admin
    let postdata = serde_urlencoded::to_string(&vec![("key", config::ADMIN.1)]).unwrap();
    let response = client
        .post("/apikey/check")
        .body(postdata)
        .header(ContentType::Form)
        .dispatch();
    assert_eq!(response.status(), Status::SeeOther);

    // anonymize
    let response = client.post("/admin/anonymize").dispatch();
    assert_eq!(response.status(), Status::SeeOther);

    // get tokens
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);

        // get ownership location capability
        let file = File::open(format!("{}.{}", email, OWNCAP_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut owncap = String::new();
        buf_reader.read_to_string(&mut owncap).unwrap();
        user2owncap.insert(email.clone(), owncap);

        // check results of anonymization: user has no answers
        for l in 0..args.nlec {
            let keys: Vec<Value> = vec![l.into(), email.clone().into()];
            let res = db
                .exec_iter(
                    "SELECT answers.* FROM answers WHERE answers.lec = ? AND answers.`user` = ?;",
                    keys,
                )
                .unwrap();
            let mut rows = vec![];
            for row in res {
                let rowvals = row.unwrap().unwrap();
                let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
                rows.push(vals);
            }
            assert_eq!(rows.len(), 0);
        }
    }

    // all answers belong to anonymous principals
    for l in 0..args.nlec {
        let keys: Vec<Value> = vec![l.into()];
        let res = db
            .exec_iter("SELECT * FROM answers WHERE lec = ?;", keys)
            .unwrap();
        let mut rows = vec![];
        let mut users: HashSet<String> = HashSet::new();
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            users.insert(from_value(vals[0].clone()));
            rows.push(vals);
        }
        // a pseudoprincipal has an answer for every question for each lecture
        assert_eq!(rows.len(), args.nusers as usize * args.nqs as usize);
        assert_eq!(users.len(), args.nusers as usize);
    }

    /***********************************
     * editing anonymized data
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let owncap = user2owncap.get(&email).unwrap();
        let decryptcap = user2decryptcap.get(&email).unwrap();
    
       
        // set ownership capability as cookie
        let response = client.get(format!("/edit/{}", owncap)).dispatch();
        assert_eq!(response.status(), Status::Ok);

        // set decryption capability as cookie
        let postdata = serde_urlencoded::to_string(&vec![("decryption_cap", decryptcap)]).unwrap();
        let response = client
            .post("/edit")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
 
        // get lecture to edit as pseudoprincipal (lecture 0 for now)
        let response = client.get(format!("/edit/lec/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);

        // update answers to lecture 0
        let mut answers = vec![];
        for q in 0..args.nqs {
            answers.push((format!("answers.{}", q), format!("new_answer_user_{}_lec_{}", u, 0)));
        }
        let postdata = serde_urlencoded::to_string(&answers).unwrap();
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);


        // logged out
        let response = client.get(format!("/leclist")).dispatch();
        assert_eq!(response.status(), Status::Unauthorized);
    }
    // check answers for users for lecture 0
    let res = db
        .query_iter("SELECT answer FROM answers WHERE lec = 0;")
        .unwrap();
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        assert!(answer.contains("new_answer"));
    }

    /***********************************
     * gdpr deletion (with composition)
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let owncap = user2owncap.get(&email).unwrap();
        let apikey = user2apikey.get(&email).unwrap();
        let decryptcap = user2decryptcap.get(&email).unwrap();
                
        // login as the user
        let postdata = serde_urlencoded::to_string(&vec![("key", apikey)]).unwrap();
        let response = client
            .post("/apikey/check")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        let postdata = serde_urlencoded::to_string(&vec![
            ("decryption_cap", decryptcap),
            ("ownership_loc_caps", &format!("{}", owncap)),
        ])
        .unwrap();

        let response = client
            .post("/delete")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        // get diff location capability: GDPR deletion in this app doesn't produce anon tokens
        let file = File::open(format!("{}.{}", email, DIFFCAP_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut diffcap = String::new();
        buf_reader.read_to_string(&mut diffcap).unwrap();
        debug!(log, "Got email {} with diffcap {}", &email, diffcap);
        user2diffcap.insert(email.clone(), diffcap);
    }
    // check results of delete: no answers or users exist
    let res = db.query_iter("SELECT * FROM answers;").unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        rows.push(answer);
    }
    assert_eq!(rows.len(), 0);
    let res = db.query_iter("SELECT * FROM users;").unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        rows.push(answer);
    }
    assert_eq!(rows.len(), 1); // the admin

    /***********************************
     * gdpr restore (with composition)
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let owncap = user2owncap.get(&email).unwrap();
        let decryptcap = user2decryptcap.get(&email).unwrap();
        let diffcap = user2diffcap.get(&email).unwrap();
        let postdata = serde_urlencoded::to_string(&vec![
            ("diff_loc_cap", diffcap),
            ("decryption_cap", decryptcap),
            ("ownership_loc_caps", &format!("{}", owncap)),
        ])
        .unwrap();

        let response = client
            .post("/restore")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
    }
    // database is back in anonymized form
    // check answers for lecture 0
    let res = db
        .query_iter("SELECT answer FROM answers WHERE lec = 0;")
        .unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        assert!(answer.contains("new_answer"));
        rows.push(answer);
    }
    assert_eq!(rows.len(), args.nqs as usize * args.nusers as usize);

    let res = db.query_iter("SELECT * FROM users;").unwrap();
    let mut rows = vec![];
    for row in res {
        let rowvals = row.unwrap().unwrap();
        let answer: String = from_value(rowvals[0].clone());
        rows.push(answer);
    }
    assert_eq!(
        rows.len(),
        1 + args.nusers as usize * (args.nlec as usize + 1)
    );
    */
}
