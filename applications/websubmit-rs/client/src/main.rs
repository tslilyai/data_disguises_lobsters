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

mod args;

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

const SERVER: &'static str = "http://localhost:8000";
const APIKEY_FILE: &'static str = "apikey.txt";
const DECRYPT_FILE: &'static str = "decrypt.txt";
const DIFFCAP_FILE: &'static str = "diffcap.txt";

fn main() {
    let log = new_logger();
    let args = args::parse_args();

    // create all users
    let mut account_durations = vec![];
    let mut user2apikey = HashMap::new();
    let mut user2decryptcap = HashMap::new();

    #[cfg(feature = "flame_it")]
    flame::start("create_users");
    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .build()
        .expect("Could not build client");
    for u in 0..args.nusers + args.ndisguising {
        let email = format!("{}@mail.edu", u);
        let start = time::Instant::now();
        let response = client
            .post(&format!("{}/apikey/generate", SERVER))
            .form(&vec![("email", email.clone())])
            .send()
            .expect("Could not create new user");
        account_durations.push(start.elapsed());
        assert_eq!(response.status(), StatusCode::OK);

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

        #[cfg(feature = "flame_it")]
        flame::end("read_user_files");
    }
    #[cfg(feature = "flame_it")]
    flame::end("create_users");

    let barrier = Arc::new(Barrier::new(args.ndisguising + args.nusers + 1));
    let mut normal_threads = vec![];
    for u in 0..args.nusers {
        let c = Arc::clone(&barrier);
        let email = format!("{}@mail.edu", u);
        let myargs = args.clone();
        let apikey = user2apikey.get(&email).unwrap().clone();
        normal_threads.push(thread::spawn(move || {
            run_normal(u.to_string(), apikey, new_logger(), myargs, c)
        }));
    }
    let mut disguising_threads = vec![];
    for u in args.nusers..args.ndisguising {
        let c = Arc::clone(&barrier);
        if !args.baseline {
            let email = format!("{}@mail.edu", u);
            let myargs = args.clone();
            let apikey = user2apikey.get(&email).unwrap().clone();
            let decryptcap = user2decryptcap.get(&email).unwrap().clone();
            disguising_threads.push(thread::spawn(move || {
                run_disguising(u.to_string(), apikey, decryptcap, new_logger(), myargs, c)
            }));
        } else {
            disguising_threads.push(thread::spawn(move || {
                c.wait();
                Ok(())
            }));
        }
    }
    barrier.wait();
    let start = time::Instant::now();

    for j in normal_threads {
        j.join().expect("Could not join?").unwrap();
    }
    info!(log, "normal threads completed: {}", start.elapsed().as_micros());
    for j in disguising_threads {
        j.join().expect("Could not join?").unwrap();
    }
    info!(log, "disguising threads completed: {}", start.elapsed().as_micros());
}

fn run_normal(
    uid: String,
    apikey: String,
    log: slog::Logger,
    args: args::Args,
    c: Arc<Barrier>,
) -> Result<()> {
    let mut edit_durations = vec![];
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
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    c.wait();
    for _ in 0..args.niters {
        // editing
        lec = (lec + 1) % args.nlec;
        let start = time::Instant::now();
        #[cfg(feature = "flame_it")]
        flame::start("edit_lec");
        let response = client.get(format!("/questions/{}", lec)).send()?;
        assert_eq!(response.status(), StatusCode::OK);
        #[cfg(feature = "flame_it")]
        flame::end("edit_lec");

        q = (q + 1) % args.nqs;
        let mut answers = vec![];
        answers.push((
            format!("answers.{}", q),
            format!("new_answer_user_{}_lec_{}", uid, lec),
        ));
        debug!(
            log,
            "Posting to questions for lec {} answers {:?}", lec, answers
        );

        #[cfg(feature = "flame_it")]
        flame::start("edit_post_new_answers");
        let response = client
            .post(format!("{}/questions/{}", SERVER, 0)) // testing lecture 0 for now
            .form(&answers)
            .send()?;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        #[cfg(feature = "flame_it")]
        flame::end("edit_post_new_answers");
        edit_durations.push(start.elapsed());
    }
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
    uid: String,
    apikey: String,
    decryptcap: String,
    log: slog::Logger,
    args: args::Args,
    c: Arc<Barrier>,
) -> Result<()> {
    let mut delete_durations = vec![];
    let mut restore_durations = vec![];

    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .build()?;

    let email = format!("{}@mail.edu", uid);
    c.wait();
    for _ in 0..args.ndisguise_iters {
        // login as the user
        let response = client
            .post(&format!("{}/apikey/check", SERVER))
            .form(&vec![("key", apikey.to_string())])
            .send()?;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);

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
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        delete_durations.push(start.elapsed());

        // get diff location capability: GDPR deletion in this app doesn't produce anon tokens
        let file = File::open(format!("{}.{}", email, DIFFCAP_FILE)).unwrap();
        let mut buf_reader = BufReader::new(file);
        let mut diffcap = String::new();
        buf_reader.read_to_string(&mut diffcap).unwrap();
        debug!(log, "Got email {} with diffcap {}", &email, diffcap);
        #[cfg(feature = "flame_it")]
        flame::end("delete");

        thread::sleep(time::Duration::from_millis(1));

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
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        restore_durations.push(start.elapsed());
        #[cfg(feature = "flame_it")]
        flame::end("restore");

        thread::sleep(time::Duration::from_millis(1));
    }

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
