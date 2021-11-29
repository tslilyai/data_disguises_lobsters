extern crate clap;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

use rocket::http::ContentType;
use rocket::http::Status;
use rocket::local::blocking::Client;
use rocket::response::Redirect;
use rocket::{Build, Rocket, State};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::sync::{Mutex};
use std::thread;
use std::time;
use std::time::Duration;

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

#[get("/")]
fn index() -> &'static str {
    "Edna API server"
}

fn rocket() -> Rocket<Build> {
    rocket::build()
        .mount("/", routes![index])
}

#[rocket::main]
async fn main() {
    let my_rocket = rocket();
    my_rocket.launch().await.expect("Failed to launch rocket");
}
