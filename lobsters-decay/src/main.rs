extern crate clap;
extern crate chrono;

use mysql::prelude::*;
use mysql::from_value;
use mysql::{Opts};
use clap::{App, Arg};
use reqwest;
use serde_json::json;
use serde::*;
use std::collections::HashMap;
use edna;
use log::warn;
use chrono::{Duration, Local};

pub const LOBSTERS_APP: &'static str = "lobsters";
pub const HOTCRP_APP: &'static str = "hotcrp";
const SERVER: &'static str = "http://localhost:8000";

#[derive(Serialize, Deserialize)]
pub struct ApplyDisguiseResponse {
    pub diff_locators: HashMap<edna::UID, edna::tokens::LocCap>,
    pub ownership_locators: HashMap<edna::UID, edna::tokens::LocCap>,
}

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

pub fn main() {
    init_logger();

    let matches = App::new("Edna API server")
        .arg(
            Arg::with_name("database")
                .short("d")
                .long("database-name")
                .default_value("testdb")
                .help("The MySQL database to use")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .default_value("mariadb")
                .help("The MySQL host server to use")
                .takes_value(true),
        )
        .get_matches();
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!(
            "mysql://root:password@{}/{}",
            matches.value_of("host").unwrap(), 
            matches.value_of("database").unwrap(), 
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(db.ping(), true);
   
    // get all users
    let mut users : Vec<String> = vec![];
    let dt = Local::now().naive_local() - Duration::days(365);
    
    warn!("Got date {}", dt.to_string());
    let res = db.query_iter(&format!(
        //"SELECT id FROM users WHERE `last_login` < '{}';", dt.to_string()
        "SELECT id FROM users WHERE `username` = 'test';"
    )).expect("Could not select inactive users?");
    for r in res {
        let r = r.unwrap().unwrap();
        let uid: String = from_value(r[0].clone());
        warn!("got id res {}", uid);
        users.push(uid);
    }
   
    let client = reqwest::blocking::Client::new();

    let postdata = json!({
        "decrypt_cap": [],
        "ownership_locators": [],
    });

    for u in &users {
        warn!("Decaying user {}", u);
        // we don't need any capabilities
        let endpoint = format!("{}/apply_disguise/lobsters/1/{}", SERVER, u);
        let response = client
            .post(&endpoint)
            .header("Content-Type", "application/json")
            .body(postdata.to_string())
            .send().unwrap();
        let strbody = response.text().unwrap();
        warn!("Decay strbody response: {}", strbody);
        let _body: ApplyDisguiseResponse = serde_json::from_str(&strbody).unwrap();
        /*if let Some(dl) = body.diff_locators.get(&u.to_string()) {
            user2diffcap.insert(u, *dl);
        }
        if let Some(ol) = body.ownership_locators.get(&u.to_string()) {
            user2owncap.insert(u, *ol);
        }*/
        // TODO send emails with locators?
    }
}
