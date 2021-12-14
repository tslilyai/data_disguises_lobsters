extern crate clap;
extern crate crypto;
extern crate mysql;
#[macro_use]
extern crate rocket;
extern crate lettre;
extern crate lettre_email;
extern crate rocket_sync_db_pools;
#[macro_use]
extern crate slog;
extern crate log;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;
extern crate base64;

mod admin;
mod apikey;
mod args;
mod backend;
mod config;
mod disguises;
mod email;
mod login;
mod privacy;
mod questions;
#[cfg(test)]
mod tests;

use edna::tokens::LocCap;
use backend::MySqlBackend;
use rocket::http::ContentType;
use rocket::http::CookieJar;
use rocket::http::Status;
use rocket::local::blocking::Client;
use rocket::response::Redirect;
use rocket::{Build, Rocket, State};
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::sync::{Mutex};
use std::thread;
use std::time;
use std::time::Duration;
use mysql::from_value;
use mysql::prelude::*;
use mysql::{Opts, Value};

pub const APIKEY_FILE: &'static str = "apikey.txt";
pub const DECRYPT_FILE: &'static str = "decrypt.txt";
pub const CAPS_FILE: &'static str = "caps.txt";

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

#[get("/")]
fn index(cookies: &CookieJar<'_>, bg: &State<MySqlBackend>) -> Redirect {
    if let Some(cookie) = cookies.get("apikey") {
        let apikey: String = cookie.value().parse().ok().unwrap();
        // TODO validate API key
        match apikey::check_api_key(&*bg, &apikey) {
            Ok(_user) => Redirect::to("/leclist"),
            Err(_) => Redirect::to("/login"),
        }
    } else {
        Redirect::to("/login")
    }
}

fn rocket(args: &args::Args) -> Rocket<Build> {
    let backend = MySqlBackend::new(&format!("{}", args.class), Some(new_logger()), &args).unwrap();

    rocket::build()
        .attach(Template::fairing())
        .manage(backend)
        .manage(args.config.clone())
        //.mount("/css", FileServer::from(format!("{}/css", resource_dir)))
        //.mount("/js", FileServer::from(format!("{}/js", resource_dir)))
        .mount("/", routes![index])
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
        .mount(
            "/restore",
            routes![privacy::restore_account, privacy::restore],
        )
        .mount(
            "/edit",
            routes![
                privacy::edit_as_pseudoprincipal,
                privacy::edit_as_pseudoprincipal_lecs
            ],
        )
        .mount(
            "/edit/lec",
            routes![privacy::edit_lec_answers_as_pseudoprincipal],
        )
}

#[rocket::main]
async fn main() {
    env_logger::init();
    let args = args::parse_args();
    let my_rocket = rocket(&args);
    if args.benchmark {
        if args.config.is_baseline {
            thread::spawn(move || {
                run_baseline_benchmark(&args, my_rocket);
            })
            .join()
            .expect("Thread panicked")
        } else {
            thread::spawn(move || {
                run_benchmark(&args, my_rocket);
            })
            .join()
            .expect("Thread panicked")
        }
    } else {
        my_rocket.launch().await.expect("Failed to launch rocket");
    }
}

fn run_baseline_benchmark(args: &args::Args, rocket: Rocket<Build>) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];
    let log = new_logger();

    let client = Client::tracked(rocket).expect("valid rocket instance");

    let mut user2apikey = HashMap::new();

    // create all users
    let nusers = args.nusers + 5;
    for u in 0..nusers {
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
    }

    /**********************************
     * baseline edits + delete
     ***********************************/
    for u in 0..nusers {
        let email = format!("{}@mail.edu", u);
        let apikey = user2apikey.get(&email).unwrap();

        // set api key
        let postdata = serde_urlencoded::to_string(&vec![("key", apikey)]).unwrap();
        let response = client
            .post("/apikey/check")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        // editing
        let start = time::Instant::now();
        let response = client.get(format!("/questions/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);

        let mut answers = vec![];
        for q in 0..args.nqs {
            answers.push((
                format!("answers.{}", q),
                format!("new_answer_user_{}_lec_{}", u, 0),
            ));
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

        // delete account
        let postdata = serde_urlencoded::to_string(&vec![
            ("decryption_cap", "0"),
            ("loc_caps", &serde_json::to_string(&Vec::<LocCap>::new()).unwrap()),
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

    print_stats(
        args,
        account_durations,
        anon_durations,
        edit_durations,
        delete_durations,
        vec![],
        vec![],
        vec![],
        vec![],
        true,
    );
}

fn run_benchmark(args: &args::Args, rocket: Rocket<Build>) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];
    let mut anon_durations = vec![];
    let mut edit_durations_nonanon = vec![];
    let mut delete_durations_nonanon = vec![];
    let mut restore_durations_nonanon = vec![];
 
    let client = Client::tracked(rocket).expect("valid rocket instance");
    let mut db = mysql::Conn::new(
        Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", args.class)).unwrap(),
    ).unwrap();
 
    let mut user2apikey = HashMap::new();
    let mut user2decryptcap = HashMap::new();
    let mut user2anoncaps = HashMap::new();
    let mut user2gdprcaps = HashMap::new();
    let log = new_logger();

    // create all users
    let nusers = args.nusers;
    for u in 0..nusers {
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

    /***********************************
     * editing nonanon data
     ***********************************/
    for u in 0..args.nusers {
        // login
        let email = format!("{}@mail.edu", u);
        let apikey = user2apikey.get(&email).unwrap();
        let postdata = serde_urlencoded::to_string(&vec![("key", apikey)]).unwrap();
        let response = client
            .post("/apikey/check")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);

        // editing
        let start = time::Instant::now();
        let response = client.get(format!("/questions/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);

        let mut answers = vec![];
        for q in 0..args.nqs {
            answers.push((
                format!("answers.{}", q),
                format!("new_answer_user_{}_lec_{}", u, 0),
            ));
        }
        let postdata = serde_urlencoded::to_string(&answers).unwrap();
        debug!(log, "Posting to questions for lec 0 answers {}", postdata);
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        edit_durations_nonanon.push(start.elapsed());
    }

    /***********************************
     * gdpr deletion (no composition)
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
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
            ("loc_caps", &serde_json::to_string(&Vec::<LocCap>::new()).unwrap()),
        ])
        .unwrap();

        let start = time::Instant::now();
        let response = client
            .post("/delete")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        delete_durations_nonanon.push(start.elapsed());

        // get capabilities: GDPR deletion in this app doesn't produce anon tokens
        let file = File::open(format!("{}.{}", email, CAPS_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut capstr = String::new();
        buf_reader.read_to_string(&mut capstr).unwrap();
        debug!(log, "Got email {} with cap {}", &email, capstr);
        user2gdprcaps.insert(email.clone(), capstr);
    }

    /***********************************
     * gdpr restore (without composition)
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let start = time::Instant::now();
        let decryptcap = user2decryptcap.get(&email).unwrap();
        let caps = user2gdprcaps.get(&email).unwrap();
        let postdata = serde_urlencoded::to_string(&vec![
            ("decryption_cap", decryptcap),
            ("loc_caps", caps),
        ])
        .unwrap();
        let response = client
            .post("/restore")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        restore_durations_nonanon.push(start.elapsed());
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
        let file = File::open(format!("{}.{}", email, CAPS_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut caps = String::new();
        buf_reader.read_to_string(&mut caps).unwrap();
        debug!(log, "Got email {} with caps {}", &email, caps);
        user2anoncaps.insert(email.clone(), caps);

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


    /***********************************
     * editing anonymized data
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let caps = user2anoncaps.get(&email).unwrap();
        let decryptcap = user2decryptcap.get(&email).unwrap();

        let start = time::Instant::now();

        // set ownership capability as cookie
        let response = client.get(format!("/edit")).dispatch();
        assert_eq!(response.status(), Status::Ok);

        // set decryption capability as cookie
        let postdata = serde_urlencoded::to_string(&vec![
            ("decryption_cap", decryptcap),
            ("loc_caps", caps)
        ]).unwrap();
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
            answers.push((
                format!("answers.{}", q),
                format!("new_answer_user_{}_lec_{}", u, 0),
            ));
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

        // check answers for users for lecture 0
        let res = db
            .query_iter("SELECT answer FROM answers WHERE lec = 0;")
            .unwrap();
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let answer: String = from_value(rowvals[0].clone());
            assert!(answer.contains("new_answer"));
        }
    }

    /***********************************
     * gdpr deletion (with composition)
     ***********************************/
    for u in 0..args.nusers {
        let email = format!("{}@mail.edu", u);
        let apikey = user2apikey.get(&email).unwrap();
        let anoncaps = user2anoncaps.get(&email).unwrap();
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
            ("loc_caps", anoncaps),
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

        // diff location capability: GDPR deletion in this app doesn't produce anon tokens
        let file = File::open(format!("{}.{}", email, CAPS_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut caps = String::new();
        buf_reader.read_to_string(&mut caps).unwrap();
        debug!(log, "Got email {} with caps {}", &email, caps);
        user2gdprcaps.insert(email.clone(), caps);
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
        let start = time::Instant::now();
        let anoncaps = user2anoncaps.get(&email).unwrap();
        let caps = user2gdprcaps.get(&email).unwrap();
        let mut anoncaps_vec : Vec<LocCap>= serde_json::from_str(&anoncaps).unwrap();
        let mut caps_vec : Vec<LocCap> = serde_json::from_str(&caps).unwrap();
        anoncaps_vec.append(&mut caps_vec);

        let caps = serde_json::to_string(&anoncaps_vec).unwrap();
        let decryptcap = user2decryptcap.get(&email).unwrap();
        let postdata = serde_urlencoded::to_string(&vec![
            ("decryption_cap", decryptcap),
            ("loc_caps", &format!("{}", caps)),
        ])
        .unwrap();
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
        1 + args.nusers as usize * (args.nlec as usize + 1)
    );

    print_stats(
        args,
        account_durations,
        anon_durations,
        edit_durations,
        delete_durations,
        restore_durations,
        edit_durations_nonanon,
        delete_durations_nonanon,
        restore_durations_nonanon,
        false,
    );
}

fn print_stats(
    args: &args::Args,
    account_durations: Vec<Duration>,
    anon_durations: Vec<Duration>,
    edit_durations: Vec<Duration>,
    delete_durations: Vec<Duration>,
    restore_durations: Vec<Duration>,
    edit_durations_nonanon: Vec<Duration>,
    delete_durations_nonanon: Vec<Duration>,
    restore_durations_nonanon: Vec<Duration>,
    is_baseline: bool,
) {
    let filename = if is_baseline {
        format!(
            "disguise_stats_{}lec_{}users_baseline.csv",
            args.nlec, args.nusers
        )
    } else {
        format!("disguise_stats_{}lec_{}users.csv", args.nlec, args.nusers)
    };

    // print out stats
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .unwrap();
    writeln!(
        f,
        "{}",
        account_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        anon_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        edit_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        edit_durations_nonanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations_nonanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations_nonanon
            .iter()
            .map(|d| d.as_micros().to_string())
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
