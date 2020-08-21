extern crate chrono;
extern crate msql_srv;
extern crate mysql;
extern crate mysql_common as myc;
extern crate nom;

use mysql::prelude::*;
use std::io;
use std::net;
use std::thread;

use msql_srv::{
    Column, ErrorKind, InitWriter, MysqlIntermediary, MysqlShim, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};

const SCHEMA : &'static str = include_str!("./schema.sql");
const CONFIG : &'static str = include_str!("./config.json");

fn test(c: C) -> ()
    C: FnOnce(&mut mysql::Conn) -> (),
{
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let jh = thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
            assert_eq!(db.ping(), true);
            db.query_drop("DROP DATABASE test;").unwrap();
            db.query_drop("CREATE DATABASE test;").unwrap();
            MysqlIntermediary::run_on_tcp(shim::Shim::new(db, CONFIG, SCHEMA), s).unwrap();
        }
    });

    let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}/test", port)).unwrap();
    db.query_drop("USE `test`;").unwrap();
    c(&mut db);
    drop(db);
    jh.join().unwrap().unwrap();
}

#[test]
fn insert_ok() {
    test(|db| match db.query_iter("INSERT INTO `test`;") {
        Ok(_) => assert!(true),
        Err(_) => assert!(false),
    });
}

#[test]
fn on_query_readonly_ok() {

    test(|db| match db.query_drop("USE `test`;") {
        Ok(_) => assert!(true),
        Err(_) => assert!(false),
    });
}

#[test]
fn really_long_query() {
    let long = "CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    TestingShim::new(
        move |q, w| {
            assert_eq!(q, long);
            w.start(&[])?.finish()
        },
        |_| 0,
        |_, _, _| unreachable!(),
        |_, _| unreachable!(),
    )
    .test(move |db| {
        db.query_iter(long).unwrap();
    })
}
