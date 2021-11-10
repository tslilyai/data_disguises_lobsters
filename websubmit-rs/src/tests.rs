use super::rocket;
use crate::*;
use mysql::from_value;
use mysql::prelude::*;
use mysql::{Opts, Value};
use rocket::http::ContentType;
use rocket::http::Status;
use rocket::local::blocking::Client;
use std::collections::{HashMap, HashSet};
use std::fs::{OpenOptions, File};
use std::io::{BufReader, Read, Write};
use std::time;

#[test]
fn test_disguise() {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];
    let mut anon_durations = vec![];

    let client = Client::tracked(rocket()).expect("valid rocket instance");
    let args = args::parse_args();
    let config = args.config;

    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", args.class)).unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);

    let mut user2apikey = HashMap::new();
    let mut user2decryptcap = HashMap::new();
    let mut user2owncap = HashMap::new();
    let mut user2diffcap = HashMap::new();
    let log = new_logger();

    // create all users
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let postdata = serde_urlencoded::to_string(&vec![("email", email.clone())]).unwrap();
        let start = time::Instant::now();
        let response = client
            .post("/apikey/generate")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        account_durations.push(start.elapsed());
        assert_eq!(response.status(), Status::Ok);

        // get api key
        let file = File::open(format!("{}.{}", email, APIKEY_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut apikey = String::new();
        buf_reader.read_to_string(&mut apikey).unwrap();
        debug!(log, "Got email {} with apikey {}", &email, apikey);
        user2apikey.insert(email.clone(), apikey);

        // get decryption cap
        let file = File::open(format!("{}.{}", email, DECRYPT_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut decryptcap = String::new();
        buf_reader.read_to_string(&mut decryptcap).unwrap();
        debug!(log, "Got email {} with decryptcap {}", &email, decryptcap);
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
    let start = time::Instant::now();
    let response = client.post("/admin/anonymize").dispatch();
    anon_durations.push(start.elapsed());
    assert_eq!(response.status(), Status::SeeOther);

    // get tokens
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);

        // get ownership location capability
        let file = File::open(format!("{}.{}", email, OWNCAP_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut owncap = String::new();
        buf_reader.read_to_string(&mut owncap).unwrap();
        debug!(log, "Got email {} with owncap {}", &email, owncap);
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
    
        let start = time::Instant::now();
       
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
        debug!(log, "Posting to questions for lec 0 answers {}", postdata);
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        edit_durations.push(start.elapsed());

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

        let start = time::Instant::now();
        let response = client
            .post("/delete")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        delete_durations.push(start.elapsed());

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

        let start = time::Instant::now();
        let response = client
            .post("/restore")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        restore_durations.push(start.elapsed());
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
        1 + args.nusers as usize * (args``.nlec as usize + 1)
    );

    // print out stats
    // account_durations
    // anon_durations
    // edit_durations
    // delete_durations
    // restore_durations
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&format!("disguise_stats_{}lec_{}users.csv", args.nlec, args.nusers)
        .unwrap();
    writeln!(f, "{}", account_durations.iter().map(|d| d.as_millis().to_string()).collect::<Vec<String>>().join(",")).unwrap();
    writeln!(f, "{}", anon_durations.iter().map(|d| d.as_millis().to_string()).collect::<Vec<String>>().join(",")).unwrap();
    writeln!(f, "{}", edit_durations.iter().map(|d| d.as_millis().to_string()).collect::<Vec<String>>().join(",")).unwrap();
    writeln!(f, "{}", delete_durations.iter().map(|d| d.as_millis().to_string()).collect::<Vec<String>>().join(",")).unwrap();
    writeln!(f, "{}", restore_durations.iter().map(|d| d.as_millis().to_string()).collect::<Vec<String>>().join(",")).unwrap();
}