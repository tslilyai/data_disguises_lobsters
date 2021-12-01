extern crate clap;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

mod apiproxy;
mod lobsters_disguises;

use clap::{App, Arg};
use edna::EdnaClient;
use rocket::{Build, Rocket};
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
    let edna_client = EdnaClient::new(
        prime,
        db,
        schema,
        in_memory,
        keypool_size,
        lobsters_disguises::get_guise_gen(),
    );
    rocket::build()
        .manage(Arc::new(Mutex::new(edna_client)))
        .mount("/", routes![index])
        .mount("/register_principal", routes![apiproxy::register_principal])
        .mount("/start_disguise", routes![apiproxy::start_disguise])
        .mount("/end_disguise", routes![apiproxy::end_disguise])
        .mount("/apply_disguise", routes![apiproxy::apply_disguise])
        .mount("/reveal_disguise", routes![apiproxy::reveal_disguise])
        .mount(
            "/get_pseudoprincipals_of",
            routes![apiproxy::get_pseudoprincipals_of],
        )
        .mount(
            "/get_tokens_of_disguise",
            routes![apiproxy::get_tokens_of_disguise],
        )
        .mount("/save_diff_token", routes![apiproxy::save_diff_token])
        .mount(
            "/save_pseudoprincipal_token",
            routes![apiproxy::save_pseudoprincipal_token],
        )
        .mount(
            "/create_pseudoprincipal",
            routes![apiproxy::create_pseudoprincipal],
        )
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
