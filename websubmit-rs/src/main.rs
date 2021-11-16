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
#[cfg(feature = "flame_it")]
extern crate flame;

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

use backend::MySqlBackend;
use rocket::http::ContentType;
use rocket::http::CookieJar;
use rocket::http::Status;
use rocket::local::blocking::Client;
use rocket::response::Redirect;
use rocket::{Build, Rocket, State};
use rocket_dyn_templates::Template;
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use std::time::Duration;

pub const APIKEY_FILE: &'static str = "apikey.txt";
pub const DECRYPT_FILE: &'static str = "decrypt.txt";
pub const DIFFCAP_FILE: &'static str = "diffcap.txt";
pub const OWNCAP_FILE: &'static str = "owncap.txt";

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

#[get("/")]
fn index(cookies: &CookieJar<'_>, backend: &State<Arc<Mutex<MySqlBackend>>>) -> Redirect {
    if let Some(cookie) = cookies.get("apikey") {
        let apikey: String = cookie.value().parse().ok().unwrap();
        // TODO validate API key
        match apikey::check_api_key(&*backend, &apikey) {
            Ok(_user) => Redirect::to("/leclist"),
            Err(_) => Redirect::to("/login"),
        }
    } else {
        Redirect::to("/login")
    }
}

fn rocket(args: &args::Args) -> Rocket<Build> {
    let nusers: usize;
    if args.config.is_baseline {
        nusers = args.nusers + 5;
    } else {
        nusers = args.nusers;
    }
    let backend = Arc::new(Mutex::new(
        MySqlBackend::new(
            &format!("{}", args.class),
            Some(new_logger()),
            args.prime,
            nusers,
            args.nlec,
            args.nqs,
        )
        .unwrap(),
    ));

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

    if args.benchmark {
        if args.config.is_baseline {
            thread::spawn(move || {
                run_baseline_benchmark(&args);
            })
            .join()
            .expect("Thread panicked")
        } else {
            thread::spawn(move || {
                run_benchmark(&args);
            })
            .join()
            .expect("Thread panicked")
        }
    } else {
        rocket(&args)
            .launch()
            .await
            .expect("Failed to launch rocket");
    }
}

fn run_baseline_benchmark(args: &args::Args) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut anon_durations = vec![];

    let client = Client::tracked(rocket(args)).expect("valid rocket instance");

    let mut user2apikey = HashMap::new();
    let log = new_logger();

    // create all users
    #[cfg(feature = "flame_it")]
    flame::start("create_users");
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
        #[cfg(feature = "flame_it")]
        flame::start("read_user_files");
        let file = File::open(format!("{}.{}", email, APIKEY_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut apikey = String::new();
        buf_reader.read_to_string(&mut apikey).unwrap();
        debug!(log, "Got email {} with apikey {}", &email, apikey);
        user2apikey.insert(email.clone(), apikey);

        #[cfg(feature = "flame_it")]
        flame::end("read_user_files");
    }
    #[cfg(feature = "flame_it")]
    flame::end("create_users");

    /**********************************
     * baseline edits + delete
     ***********************************/
    for u in 0..5 {
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
        #[cfg(feature = "flame_it")]
        flame::start("edit_lec");
        let response = client.get(format!("/questions/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);
        #[cfg(feature = "flame_it")]
        flame::end("edit_lec");

        let mut answers = vec![];
        for q in 0..args.nqs {
            answers.push((
                format!("answers.{}", q),
                format!("new_answer_user_{}_lec_{}", u, 0),
            ));
        }
        let postdata = serde_urlencoded::to_string(&answers).unwrap();
        debug!(log, "Posting to questions for lec 0 answers {}", postdata);
        #[cfg(feature = "flame_it")]
        flame::start("edit_post_new_answers");
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        #[cfg(feature = "flame_it")]
        flame::end("edit_post_new_answers");
        edit_durations.push(start.elapsed());

        // delete account

        let postdata = serde_urlencoded::to_string(&vec![
            ("decryption_cap", "0"),
            ("ownership_loc_caps", "0"),
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
    #[cfg(feature = "flame_it")]
    flame::start("anonymize");
    let start = time::Instant::now();
    let response = client.post("/admin/anonymize").dispatch();
    anon_durations.push(start.elapsed());
    #[cfg(feature = "flame_it")]
    flame::end("anonymize");
    assert_eq!(response.status(), Status::SeeOther);
    
    print_stats(
        args,
        account_durations,
        anon_durations,
        edit_durations,
        delete_durations,
        vec![],
        true,
    );

    #[cfg(feature = "flame_it")]
    flame::dump_html(
        &mut File::create(&format!(
            "flamegraph_{}lec_{}users_baseline.html",
            args.nlec, args.nusers
        ))
        .unwrap(),
    )
    .unwrap();
}

fn run_benchmark(args: &args::Args) {
    let mut account_durations = vec![];
    let mut edit_durations = vec![];
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];
    let mut anon_durations = vec![];

    let client = Client::tracked(rocket(args)).expect("valid rocket instance");

    let mut user2apikey = HashMap::new();
    let mut user2decryptcap = HashMap::new();
    let mut user2owncap = HashMap::new();
    let mut user2diffcap = HashMap::new();
    let log = new_logger();

    // create all users
    #[cfg(feature = "flame_it")]
    flame::start("create_users");
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
        #[cfg(feature = "flame_it")]
        flame::start("read_user_files");
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
        #[cfg(feature = "flame_it")]
        flame::end("read_user_files");
    }
    #[cfg(feature = "flame_it")]
    flame::end("create_users");

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
    #[cfg(feature = "flame_it")]
    flame::start("anonymize");
    let start = time::Instant::now();
    let response = client.post("/admin/anonymize").dispatch();
    anon_durations.push(start.elapsed());
    #[cfg(feature = "flame_it")]
    flame::end("anonymize");
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
    }

    /***********************************
     * editing anonymized data
     ***********************************/
    #[cfg(feature = "flame_it")]
    flame::start("edit");
    for u in 0..min(5, args.nusers) {
        let email = format!("{}@mail.edu", u);
        let owncap = user2owncap.get(&email).unwrap();
        let decryptcap = user2decryptcap.get(&email).unwrap();

        let start = time::Instant::now();

        // set ownership capability as cookie
        #[cfg(feature = "flame_it")]
        flame::start("edit_owncap");
        let response = client.get(format!("/edit/{}", owncap)).dispatch();
        assert_eq!(response.status(), Status::Ok);
        #[cfg(feature = "flame_it")]
        flame::end("edit_owncap");

        // set decryption capability as cookie
        #[cfg(feature = "flame_it")]
        flame::start("edit_decryptcap");
        let postdata = serde_urlencoded::to_string(&vec![("decryption_cap", decryptcap)]).unwrap();
        let response = client
            .post("/edit")
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        #[cfg(feature = "flame_it")]
        flame::end("edit_decryptcap");

        // get lecture to edit as pseudoprincipal (lecture 0 for now)
        #[cfg(feature = "flame_it")]
        flame::start("edit_lec");
        let response = client.get(format!("/edit/lec/{}", 0)).dispatch();
        assert_eq!(response.status(), Status::Ok);
        #[cfg(feature = "flame_it")]
        flame::end("edit_lec");

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
        #[cfg(feature = "flame_it")]
        flame::start("edit_post_new_answers");
        let response = client
            .post(format!("/questions/{}", 0)) // testing lecture 0 for now
            .body(postdata)
            .header(ContentType::Form)
            .dispatch();
        assert_eq!(response.status(), Status::SeeOther);
        #[cfg(feature = "flame_it")]
        flame::end("edit_post_new_answers");
        edit_durations.push(start.elapsed());

        // logged out
        let response = client.get(format!("/leclist")).dispatch();
        assert_eq!(response.status(), Status::Unauthorized);
    }
    #[cfg(feature = "flame_it")]
    flame::end("edit");

    /***********************************
     * gdpr deletion (with composition)
     ***********************************/
    #[cfg(feature = "flame_it")]
    flame::start("delete");
    for u in 0..min(5, args.nusers) {
        let email = format!("{}@mail.edu", u);
        let apikey = user2apikey.get(&email).unwrap();
        let owncap = user2owncap.get(&email).unwrap();
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
    #[cfg(feature = "flame_it")]
    flame::end("delete");

    /***********************************
     * gdpr restore (with composition)
     ***********************************/
    #[cfg(feature = "flame_it")]
    flame::start("restore");
    for u in 0..min(5, args.nusers) {
        let email = format!("{}@mail.edu", u);
        let start = time::Instant::now();
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
        restore_durations.push(start.elapsed());
    }
    #[cfg(feature = "flame_it")]
    flame::end("restore");

    print_stats(
        args,
        account_durations,
        anon_durations,
        edit_durations,
        delete_durations,
        restore_durations,
        false,
    );

    #[cfg(feature = "flame_it")]
    flame::dump_html(
        &mut File::create(&format!(
            "flamegraph_{}lec_{}users.html",
            args.nlec, args.nusers
        ))
        .unwrap(),
    )
    .unwrap();
}

fn print_stats(
    args: &args::Args,
    account_durations: Vec<Duration>,
    anon_durations: Vec<Duration>,
    edit_durations: Vec<Duration>,
    delete_durations: Vec<Duration>,
    restore_durations: Vec<Duration>,
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
}
