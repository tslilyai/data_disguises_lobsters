extern crate clap;
#[macro_use]
extern crate rocket;
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate serde_derive;

mod apiproxy;
mod lobsters_disguises;
mod hotcrp_disguises;
mod tests;

use clap::{App, Arg};
use edna::EdnaClient;
use rocket::{Build, Rocket};
use std::sync::{Arc, Mutex};
use std::fs;

pub const LOBSTERS_APP: &'static str = "lobsters";
pub const HOTCRP_APP: &'static str = "hotcrp";

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
    host: &str,
    db: &str,
    schema: &str,
    in_memory: bool,
    keypool_size: usize,
    app: &str,
) -> Rocket<Build> {
    let schemastr = fs::read_to_string(schema).unwrap();
    let guise_gen = match app {
        LOBSTERS_APP => lobsters_disguises::get_guise_gen(),
        HOTCRP_APP => hotcrp_disguises::get_guise_gen(),
        _ => unimplemented!("unsupported app")
    };
    let edna_client = EdnaClient::new(
        prime,
        batch,
        host,
        db,
        &schemastr,
        in_memory,
        keypool_size,
        guise_gen,
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
        .mount(
            "/cleanup_tokens_of_disguise",
            routes![apiproxy::cleanup_tokens_of_disguise],
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
         .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .default_value("mariadb")
                .help("The MySQL server host to use")
                .takes_value(true),
        )
        .arg(Arg::with_name("edna-prime").help("Use Edna to prime the database"))
        .arg(Arg::with_name("batch").help("Use token batching"))
        .arg(Arg::with_name("test").help("Run the test"))
        .arg(
            Arg::with_name("schema")
                .short("s")
                .default_value("src/lobsters_disguises/schema.sql")
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
        .arg(
            Arg::with_name("application")
                .short("a")
                .default_value("lobsters")
                .takes_value(true)
                .long("app")
                .help("Which application to run"),
        )
        .get_matches();

    /*if matches.is_present("test") {
        match matches.value_of("app").unwrap() {
            "lobsters" => tests::test_lobsters_disguise().await,
            "hotcrp" => tests::test_hotcrp_disguise().await,
            _ => unimplemented!("unsupported app"),
        }
        return;
    }
    tests::test_lobsters_disguise().await;
    return;
    */

    let my_rocket = rocket(
        matches.is_present("edna-prime"),
        matches.is_present("batch"),
        matches.value_of("host").unwrap(),
        matches.value_of("database").unwrap(),
        matches.value_of("schema").unwrap(),
        matches.is_present("in-memory"),
        usize::from_str_radix(matches.value_of("keypool-size").unwrap(), 10).unwrap(),
        matches.value_of("application").unwrap(),
    );
    my_rocket.launch().await.expect("Failed to launch rocket");
}
