extern crate clap;
#[macro_use]
extern crate rocket;
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

mod apiproxy;
mod lobsters_disguises;
mod tests;

use clap::{App, Arg};
use edna::EdnaClient;
use rocket::{Build, Rocket};
use std::sync::{Arc, Mutex};
use std::fs;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Warn)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}

#[get("/")]
fn index() -> &'static str {
    "Edna API server\n"
}

fn rocket(
    prime: bool,
    batch: bool,
    db: &str,
    schema: &str,
    in_memory: bool,
    keypool_size: usize,
) -> Rocket<Build> {
    let schemastr = fs::read_to_string(schema).unwrap();
    let edna_client = EdnaClient::new(
        prime,
        batch,
        db,
        &schemastr,
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
        .arg(Arg::with_name("prime").help("Prime the database"))
        .arg(Arg::with_name("batch").help("Use token batching"))
        .arg(Arg::with_name("test").help("Run the test"))
        .arg(
            Arg::with_name("schema")
                .short("s")
                .default_value("lobsters_disguises/schema.sql")
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

    //if matches.is_present("test") {
        tests::test_disguise().await;
        //test::test_decay_disguise();
        return;
    //}

    let my_rocket = rocket(
        matches.is_present("prime"),
        matches.is_present("batch"),
        matches.value_of("database").unwrap(),
        matches.value_of("schema").unwrap(),
        matches.is_present("in-memory"),
        usize::from_str_radix(matches.value_of("keypool-size").unwrap(), 10).unwrap(),
    );
    my_rocket.launch().await.expect("Failed to launch rocket");
}
