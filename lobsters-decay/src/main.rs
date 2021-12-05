use reqwest;
use serde_json::json;
use serde::*;
use std::collections::HashMap;
use edna;
use log::warn;

pub const LOBSTERS_APP: &'static str = "lobsters";
pub const HOTCRP_APP: &'static str = "hotcrp";

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

    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .build()
        .expect("Could not build client");

    let postdata = json!({
        "decrypt_cap": [],
        "ownership_locators": [],
    });

    let nusers : usize = 10;//TODO
    for u in 0..nusers {
        let endpoint = format!("/apply_disguise/lobsters/1/{}", u);
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
    }
    // TODO send emails?
}
