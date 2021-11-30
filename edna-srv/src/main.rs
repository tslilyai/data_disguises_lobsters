extern crate clap;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

mod guises;

use clap::{App, Arg};
use rocket::{Build, Rocket, State};
use std::sync::{Arc, Mutex};

pub fn new_logger() -> slog::Logger {
    use slog::Drain;
    use slog::Logger;
    use slog_term::term_full;
    Logger::root(Mutex::new(term_full()).fuse(), o!())
}

#[get("/")]
fn index() -> &'static str {
    "Edna API server\n"
}

fn rocket(
    prime: bool,
    db: &str,
    schema: &str,
    in_memory: bool,
    keypool_size: usize,
) -> Rocket<Build> {
    let edna_client = edna::EdnaClient::new(
        prime,
        db,
        schema,
        in_memory,
        keypool_size,
        guises::get_guise_gen(),
    );
    rocket::build()
        .manage(Arc::new(Mutex::new(edna_client)))
        .mount("/", routes![index])
}

#[rocket::main]
async fn main() {
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
    let my_rocket = rocket(
        matches.is_present("prime"),
        matches.value_of("database").unwrap(),
        matches.value_of("schema").unwrap(),
        matches.is_present("in-memory"),
        usize::from_str_radix(matches.value_of("keypool-size").unwrap(), 10).unwrap(),
    );
    my_rocket.launch().await.expect("Failed to launch rocket");
}
