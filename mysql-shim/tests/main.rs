extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use std::*;
mod mysql_shim;

const SCHEMA : &'static str = include_str!("./schema.sql");
const CONFIG : &'static str = include_str!("./config.json");

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
            // just for testing
            db.query_drop("DROP DATABASE gdpr;").unwrap();
            db.query_drop("CREATE DATABASE gdpr;").unwrap();
            assert_eq!(db.ping(), true);
            MysqlIntermediary::run_on_tcp(shim::Shim::new(db, CONFIG, SCHEMA), s).unwrap();
        }
    });

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}/gdpr", port)).unwrap();
    assert_eq!(db.ping(), true);
    //assert_eq!(db.select_db("gdpr"), true);
    //println!("Done selecting db");
    assert_eq!(db.query_iter("SELECT * FROM comments").unwrap().count(), 0);
    //assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 1);*/
    drop(db);
    jh.join().unwrap();
}
