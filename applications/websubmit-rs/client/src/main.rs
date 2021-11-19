extern crate clap;
extern crate crypto;
extern crate mysql;
#[macro_use]
extern crate slog;
#[cfg(feature = "flame_it")]
extern crate flame;
extern crate log;
extern crate slog_term;

use reqwest::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time;
use std::time::Duration;
use mysql::{Opts, Pool};
use mysql::prelude::*;
use rand::Rng;

mod args;

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

const TOTAL_TIME: u128 = 100000;
const SERVER: &'static str = "http://localhost:8000";
const APIKEY_FILE: &'static str = "apikey.txt";
const DECRYPT_FILE: &'static str = "decrypt.txt";
const DIFFCAP_FILE: &'static str = "diffcap.txt";

fn main() {
    let log = new_logger();
    let args = args::parse_args();

    // create all users
    let edit_durations = Arc::new(Mutex::new(vec![]));
    let delete_durations = Arc::new(Mutex::new(vec![]));
    let restore_durations = Arc::new(Mutex::new(vec![]));
    let mut user2apikey = HashMap::new();
    let mut user2decryptcap = HashMap::new();

    #[cfg(feature = "flame_it")]
    flame::start("create_users");
    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .build()
        .expect("Could not build client");

    let opts = Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", args.db)).unwrap();
    let pool = Pool::new(opts).unwrap();
    let mut db = pool.get_conn().unwrap();
    for l in 0..args.nlec {
        db.query_drop(&format!("INSERT INTO lectures VALUES ({}, 'lec{}');", l, l))
            .unwrap();
        for q in 0..args.nqs {
            db.query_drop(&format!(
                "INSERT INTO questions VALUES ({}, {}, 'lec{}question{}');",
                l, q, l, q
            ))
            .unwrap();
            for u in 0..args.nusers + args.ndisguising {
                db.query_drop(&format!("INSERT INTO answers VALUES ('{}@mail.edu', {}, {}, 'lec{}q{}answer{}', '1000-01-01 00:00:00');", 
                        u, l, q, l, q, u)).unwrap();
            }
        }
    }
    info!(log, "Inserted {} lecs with {} qs", args.nlec, args.nusers);

    for u in 0..args.nusers + args.ndisguising {
        let email = format!("{}@mail.edu", u);
        let response = client
            .post(&format!("{}/apikey/generate", SERVER))
            .form(&vec![("email", email.clone())])
            .send()
            .expect("Could not create new user");
        assert_eq!(response.status(), StatusCode::OK);
     
        // get api key
        #[cfg(feature = "flame_it")]
        flame::start("read_user_files");
        let file = File::open(format!("{}.{}", email, APIKEY_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut apikey = String::new();
        buf_reader.read_to_string(&mut apikey).unwrap();
        info!(log, "Got email {} with apikey {}", &email, apikey);
        user2apikey.insert(email.clone(), apikey);

        // get decryption cap
        let file = File::open(format!("{}.{}", email, DECRYPT_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut decryptcap = String::new();
        buf_reader.read_to_string(&mut decryptcap).unwrap();
        info!(log, "Got email {} with decryptcap {}", &email, decryptcap);
        user2decryptcap.insert(email, decryptcap);
        #[cfg(feature = "flame_it")]
        flame::end("read_user_files");
    }
    #[cfg(feature = "flame_it")]
    flame::end("create_users");

    let barrier = Arc::new(Barrier::new(args.nusers + 1));
    let mut normal_threads = vec![];
    for u in 0..args.nusers {
        let c = Arc::clone(&barrier);
        let email = format!("{}@mail.edu", u);
        let myargs = args.clone();
        let apikey = user2apikey.get(&email).unwrap().clone();
        let my_edit_durations = edit_durations.clone();
        normal_threads.push(thread::spawn(move || {
            run_normal(
                u.to_string(),
                apikey,
                new_logger(),
                myargs,
                my_edit_durations,
                c,
            )
        }));
    }
    if !args.baseline {
        let my_delete_durations = delete_durations.clone();
        let my_restore_durations = restore_durations.clone();
        let ndisguising = args.ndisguising;
        normal_threads.push(thread::spawn(move || {
            run_disguising(
                ndisguising,
                user2apikey.clone(),
                user2decryptcap.clone(),
                my_delete_durations,
                my_restore_durations,
            )
        }));
    }
    info!(log, "Waiting for barrier!");
    barrier.wait();
    let start = time::Instant::now();
    for j in normal_threads {
        j.join().expect("Could not join?").unwrap();
    }
    info!(
        log,
        "normal threads completed: {}",
        start.elapsed().as_micros()
    );
    print_stats(
        &args,
        &edit_durations.lock().unwrap(),
        &delete_durations.lock().unwrap(),
        &restore_durations.lock().unwrap(),
    );
}

fn run_normal(
    uid: String,
    apikey: String,
    log: slog::Logger,
    args: args::Args,
    edit_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    c: Arc<Barrier>,
) -> Result<()> {
    let mut my_edit_durations = vec![];
    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .build()?;
    let mut lec = 0;
    let mut q = 0;

    // set api key
    let response = client
        .post(&format!("{}/apikey/check", SERVER))
        .form(&vec![("key", apikey.clone())])
        .send()?;
    assert_eq!(response.status(), StatusCode::OK);
    c.wait();

    let overall_start = time::Instant::now();
    let mut rng = rand::thread_rng();
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        // editing
        lec = (lec + 1) % args.nlec;
        let start = time::Instant::now();
        #[cfg(feature = "flame_it")]
        flame::start("edit_lec");
        let response = client.get(format!("{}/questions/{}", SERVER, lec)).send()?;
        assert_eq!(response.status(), StatusCode::OK);
        #[cfg(feature = "flame_it")]
        flame::end("edit_lec");

        q = (q + 1) % args.nqs;
        let mut answers = vec![];
        answers.push((
            format!("answers.{}", q),
            format!("new_answer_user_{}_lec_{}", uid, lec),
        ));
        info!(
            log,
            "Posting to questions for lec {} answers {:?}", lec, answers
        );

        #[cfg(feature = "flame_it")]
        flame::start("edit_post_new_answers");
        let response = client
            .post(format!("{}/questions/{}", SERVER, 0)) // testing lecture 0 for now
            .form(&answers)
            .send()?;
        assert_eq!(response.status(), StatusCode::OK);
        #[cfg(feature = "flame_it")]
        flame::end("edit_post_new_answers");
        my_edit_durations.push((overall_start.elapsed(), start.elapsed()));
        thread::sleep(time::Duration::from_millis(rng.gen_range(50..200)));
    }
    edit_durations
        .lock()
        .unwrap()
        .append(&mut my_edit_durations);
    #[cfg(feature = "flame_it")]
    flame::dump_html(
        &mut File::create(&format!(
            "flamegraph_{}lec_{}users_baseline.html",
            args.nlec, args.nusers
        ))
        .unwrap(),
    )
    .unwrap();
    Ok(())
}

fn run_disguising(
    ndisguising: usize,
    user2apikey: HashMap<String, String>,
    user2decryptcap: HashMap<String, String>,
    delete_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    restore_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
) -> Result<()> {
    let overall_start = time::Instant::now();
    while overall_start.elapsed().as_millis() < TOTAL_TIME {
        let mut disguising_threads = vec![];
        for u in 0..ndisguising {
            let email = format!("{}@mail.edu", u);
            let apikey = user2apikey.get(&email).unwrap().clone();
            let decryptcap = user2decryptcap.get(&email).unwrap().clone();
            let my_delete_durations = delete_durations.clone();
            let my_restore_durations = restore_durations.clone();
            disguising_threads.push(thread::spawn(move || {
                run_disguising_thread(
                    u.to_string(),
                    apikey,
                    decryptcap,
                    new_logger(),
                    my_delete_durations,
                    my_restore_durations,
                    overall_start,
                )
            }));
        }
        for j in disguising_threads {
            j.join().expect("Could not join?").unwrap();
        }
        thread::sleep(time::Duration::from_millis(5000));
    }
    Ok(())
}
 
fn run_disguising_thread(
    uid: String,
    apikey: String,
    decryptcap: String,
    log: slog::Logger,
    delete_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    restore_durations: Arc<Mutex<Vec<(Duration, Duration)>>>,
    overall_start: time::Instant,
) -> Result<()> {
    let mut my_delete_durations = vec![];
    let mut my_restore_durations = vec![];

    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .build()?;

    let email = format!("{}@mail.edu", uid);

    let mut rng = rand::thread_rng();
    for _ in 0..10 {
        // login as the user
        let response = client
            .post(&format!("{}/apikey/check", SERVER))
            .form(&vec![("key", apikey.to_string())])
            .send()?;
        assert_eq!(response.status(), StatusCode::OK);

        // delete
        #[cfg(feature = "flame_it")]
        flame::start("delete");
        let start = time::Instant::now();
        let response = client
            .post(&format!("{}/delete", SERVER))
            .form(&vec![
                ("decryption_cap", decryptcap.to_string()),
                ("ownership_loc_caps", format!("{}", 0)),
            ])
            .send()?;
        assert_eq!(response.status(), StatusCode::OK);
        my_delete_durations.push((overall_start.elapsed(), start.elapsed()));

        // get diff location capability: GDPR deletion in this app doesn't produce anon tokens
        let file = File::open(format!("{}.{}", email, DIFFCAP_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut diffcap = String::new();
        buf_reader.read_to_string(&mut diffcap).unwrap();
        info!(log, "Got email {} with diffcap {}", &email, diffcap);
        #[cfg(feature = "flame_it")]
        flame::end("delete");

        // restore
        #[cfg(feature = "flame_it")]
        flame::start("restore");
        let start = time::Instant::now();
        let response = client
            .post(&format!("{}/restore", SERVER))
            .form(&vec![
                ("diff_loc_cap", diffcap),
                ("decryption_cap", decryptcap.to_string()),
                ("ownership_loc_caps", format!("{}", 0)),
            ])
            .send()?;
        assert_eq!(response.status(), StatusCode::OK);
        my_restore_durations.push((overall_start.elapsed(), start.elapsed()));
        #[cfg(feature = "flame_it")]
        flame::end("restore");
        thread::sleep(time::Duration::from_millis(rng.gen_range(50..200)));
    }
    delete_durations
        .lock()
        .unwrap()
        .append(&mut my_delete_durations);
    restore_durations
        .lock()
        .unwrap()
        .append(&mut my_restore_durations);

    #[cfg(feature = "flame_it")]
    flame::dump_html(
        &mut File::create(&format!(
            "flamegraph_{}lec_{}users.html",
            args.nlec, args.nusers
        ))
        .unwrap(),
    )
    .unwrap();
    Ok(())
}

fn print_stats(
    args: &args::Args,
    edit_durations: &Vec<(Duration, Duration)>,
    delete_durations: &Vec<(Duration, Duration)>,
    restore_durations: &Vec<(Duration, Duration)>,
) {
    let filename = if args.baseline {
        format!(
            "concurrent_disguise_stats_{}lec_{}users_{}disguisers_baseline.csv",
            args.nlec, args.nusers, args.ndisguising
        )
    } else {
        format!(
            "concurrent_disguise_stats_{}lec_{}users_{}disguisers.csv",
            args.nlec, args.nusers, args.ndisguising
        )
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
        edit_durations
            .iter()
            .map(|d| format!("{}:{}", d.0.as_millis().to_string(), d.1.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        delete_durations
            .iter()
            .map(|d| format!("{}:{}", d.0.as_millis().to_string(), d.1.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
    writeln!(
        f,
        "{}",
        restore_durations
            .iter()
            .map(|d| format!("{}:{}", d.0.as_millis().to_string(), d.1.as_micros().to_string()))
            .collect::<Vec<String>>()
            .join(",")
    )
    .unwrap();
}
