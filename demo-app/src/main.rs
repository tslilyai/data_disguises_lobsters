#[macro_use] extern crate rocket;
#[macro_use] extern crate rocket_sync_db_pools;

use rocket_sync_db_pools::{diesel, database};

#[database("mysql_db")]
struct DbConn(diesel::MysqlConnection);

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(DbConn::fairing())
        .mount("/", routes![index])
}
