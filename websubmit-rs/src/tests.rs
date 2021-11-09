use super::rocket;
use crate::*;
use mysql::prelude::*;
use mysql::from_value;
use mysql::{Value, Opts};
use rocket::http::ContentType;
use rocket::http::Status;
use rocket::local::blocking::Client;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};

/*
         .mount(
            "/questions",
            routes![questions::questions, questions::questions_submit],
        )
        .mount("/apikey/check", routes![apikey::check])
        .mount("/apikey/generate", routes![apikey::generate])
        .mount("/answers", routes![questions::answers])
        .mount("/leclist", routes![questions::leclist])
        .mount("/login", routes![login::login])
        .mount(
            "/admin/lec/add",
            routes![admin::lec_add, admin::lec_add_submit],
        )
        .mount("/admin/users", routes![admin::get_registered_users])
        .mount(
            "/admin/lec",
            routes![admin::lec, admin::addq, admin::editq, admin::editq_submit],
        )
        .mount("/delete", routes![privacy::delete, privacy::delete_submit])
        .mount(
            "/admin/anonymize",
            routes![privacy::anonymize, privacy::anonymize_answers],
        )
        .mount("/restore", routes![privacy::restore_account, privacy::restore])
        .mount("/edit", routes![privacy::edit_as_pseudoprincipal, privacy::edit_as_pseudoprincipal_lecs])
        .mount("/edit/lec", routes![privacy::edit_lec_answers_as_pseudoprincipal])
*/

const ADMIN: (&'static str, &'static str) = (
    "malte@cs.brown.edu",
    "b4bc3cef020eb6dd20defa1a7a8340dee889bc2164612e310766e69e45a1d5a7",
);

#[test]
fn test_disguise() {
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
    let log = new_logger();

    // create all users
    for u in 0..config.nusers {
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
    let postdata = serde_urlencoded::to_string(&vec![("key", ADMIN.1)]).unwrap();
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
    for u in 0..config.nusers {
        let email = format!("{}@mail.edu", u);
        // get ownership location capability
        let file = File::open(format!("{}.{}", email, OWNCAP_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut owncap = String::new();
        buf_reader.read_to_string(&mut owncap).unwrap();
        debug!(log, "Got email {} with owncap {}", &email, owncap);
        user2owncap.insert(email.clone(), owncap);

        // check results of anonymization: user has no answers
        for l in 0..config.nlec {
            let keys : Vec<Value> = vec![l.into(), email.clone().into()];
            let res = db.exec_iter("SELECT answers.* FROM answers WHERE answers.lec = ? AND answers.`user` = ?;", keys).unwrap();
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
    for l in 0..config.nlec {
        let keys : Vec<Value> = vec![l.into()];
        let res = db.exec_iter("SELECT * FROM answers WHERE lec = ?;", keys).unwrap();
        let mut rows = vec![];
        let mut users : HashSet<String> = HashSet::new();
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            users.insert(from_value(vals[0].clone()));
            rows.push(vals);
        }
        // a pseudoprincipal has an answer for every question for each lecture
        assert_eq!(rows.len(), config.nusers as usize * config.nqs as usize);
        assert_eq!(users.len(), config.nusers as usize);
    }
    
    /***********************************
     * editing anonymized data
    ***********************************/


    /***********************************
     * gdpr deletion (with composition)
    ***********************************/

    /***********************************
     * gdpr restore (with composition)
    ***********************************/
}
