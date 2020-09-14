// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate mysql;

use mysql::prelude::*;
use datadriven::walk;
use std::*;
use sql_parser::parser;
use msql_srv::*;
use mysql_shim::*;

const SCHEMA : &'static str = include_str!("./schema.sql");
const CONFIG : &'static str = include_str!("./config.json");
const GHOST_ID_START : u64 = 1<<20;

fn mysql_val_to_parser_val(val: &mysql::Value) -> sql_parser::ast::Value {
    match val {
        mysql::Value::NULL => sql_parser::ast::Value::Null,
        mysql::Value::Bytes(bs) => {
            let res = str::from_utf8(&bs);
            match res {
                Err(_) => sql_parser::ast::Value::String(String::new()),
                Ok(s) => sql_parser::ast::Value::String(s.to_string()),
            }
        }
        mysql::Value::Int(i) => sql_parser::ast::Value::Number(format!("{}", i)),
        mysql::Value::UInt(i) => sql_parser::ast::Value::Number(format!("{}", i)),
        mysql::Value::Float(f) => sql_parser::ast::Value::Number(format!("{}", f)),
        _ => unimplemented!("No sqlparser support for dates yet?")
        /*mysql::Date(u16, u8, u8, u8, u8, u8, u32),
        mysql::Time(bool, u32, u8, u8, u8, u32),8*/
    }
}

fn trim_one<'a>(s: &'a str) -> &'a str {
    if s.ends_with('\n') {
        &s[..s.len() - 1]
    } else {
        s
    }
}

#[test]
fn test_mvtrans_datadriven() {
    let cfg = config::parse_config(include_str!("./config_mvtrans_test.json")).unwrap();
    walk("tests/testdata", |f| {
        f.run(|test_case| -> String {
            let mut mv_trans = mv_transformer::MVTransformer::new(&cfg);
            match test_case.directive.as_str() {
                "parse-statement" => {
                    let sql = trim_one(&test_case.input).to_owned();
                    match parser::parse_statements(sql) {
                        Ok(s) => {
                            if s.len() != 1 {
                                "expected exactly one statement".to_string()
                            } else {
                                let stmt = s.iter().next().unwrap();
                                let (mv_stmt, _write_query) = mv_trans.stmt_to_mv_stmt(stmt);
                                if test_case.args.get("roundtrip").is_some() {
                                    format!("{}\n", mv_stmt.to_string())
                                } else {
                                    format!("{}\n=>\n{:?}\n", stmt.to_string(), mv_stmt)
                                }
                            }
                        }
                        Err(e) => format!("error:\n{}\n", e),
                    }
                }
                "parse-scalar" => {
                    let sql = test_case.input.trim().to_owned();
                    match parser::parse_expr(sql) {
                        Ok(s) => {
                            if test_case.args.get("roundtrip").is_some() {
                                format!("{}\n", s.to_string())
                            } else {
                                format!("{:?}\n", s)
                            }
                        }
                        Err(e) => format!("error:\n{}\n", e),
                    }
                }
                dir => {
                    panic!("unhandled directive {}", dir);
                }
            }
        })
    });
}

struct Tester;

impl Tester {
    fn new() -> Self {
        return Tester;
    }
    fn setup_test_database(&self) {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let jh = thread::spawn(move || {
            if let Ok((s, _)) = listener.accept() {
                let mut db = mysql::Conn::new("mysql://tslilyai:pass@localhost").unwrap();
                db.query_drop("DROP DATABASE IF EXISTS gdpr;").unwrap();
                db.query_drop("CREATE DATABASE gdpr;").unwrap();
                assert_eq!(db.ping(), true);
                MysqlIntermediary::run_on_tcp(mysql_shim::Shim::new(db, CONFIG, SCHEMA), s).unwrap();
            }
        });

        let mut db = mysql::Conn::new(&format!("mysql://127.0.0.1:{}", port)).unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db("gdpr"), true);

        /*
         * NOTE: the column types are all right, but the mysql value returned is always Bytes,
         * so it always parses as a String
         */

        /* 
         * TEST 1: all tables successfully created 
         */
        let mut results = vec![];
        let res = db.query_iter("SHOW tables;").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            let name = format!("{}", mysql_val_to_parser_val(&vals[0]));
            let trimmed = name.trim_end_matches("\'").trim_start_matches("\'").to_string();
            results.push(trimmed);
        }
        let tables = vec![
            "ghosts",
            "stories", "storiesmv",
            "users", "usersmv",
            "moderations", "moderationsmv",
        ];
        assert_eq!(results.len(), tables.len());
        for tab in results {
            assert!(tables.iter().any(|tt| &tab == *tt));
        }

        /*
         * TEST 2: insert users works properly
         */
        let mut results = vec![];
        db.query_drop(r"INSERT INTO users (username) VALUES ('hello_1'), ('hello_2');").unwrap();
        let res = db.query_iter(r"SELECT * FROM users WHERE users.username='hello_1' OR users.username='hello_2';").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 3);
            let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
            let username = format!("{}", mysql_val_to_parser_val(&vals[1]));
            let karma = format!("{}", mysql_val_to_parser_val(&vals[2]));
            results.push((id, username, karma));
        }
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], ("'1'".to_string(), "'hello_1'".to_string(), "'0'".to_string()));
        assert_eq!(results[1], ("'2'".to_string(), "'hello_2'".to_string(), "'0'".to_string()));
   
        //  No ghost entries added
        let mut results = vec![];
        let res = db.query_iter(r"SELECT COUNT(*) FROM ghosts;").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 1);
            let count = format!("{}", mysql_val_to_parser_val(&vals[0]));
            results.push(count);
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], "'0'".to_string());

        /*
         * TEST 3: insert into datatables works properly
         */
        let mut results = vec![];
        db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES (1, 0, 2, 'bad story!');").unwrap();
        let res = db.query_iter(r"SELECT * FROM moderations;").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 5);
            let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
            let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
            let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
            let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
            let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
            results.push((id, mod_id, story_id, user_id, action));
        }
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 
                   ("'1'".to_string(), 
                    "'1'".to_string(), 
                    "'0'".to_string(), 
                    "'2'".to_string(), 
                    "'bad story!'".to_string()));

        // two ghost entries added, beginning from GHOST_ID_START
        let mut results = vec![];
        let res = db.query_iter(r"SELECT * FROM ghosts;").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 2);
            let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
            let uid = format!("{}", mysql_val_to_parser_val(&vals[1]));
            results.push((gid, uid));
        }
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (format!("'{}'", GHOST_ID_START), "'1'".to_string()));
        assert_eq!(results[1], (format!("'{}'", GHOST_ID_START+1), "'2'".to_string()));

        /*
         * TEST 4: complex insert into datatables works properly
         */
        let mut results = vec![];
        db.query_drop(r"INSERT INTO moderations (moderator_user_id, story_id, user_id, action) VALUES ((SELECT id FROM users WHERE username='hello_2'), '0', '1', 'worst story!');").unwrap();
        let res = db.query_iter(r"SELECT * FROM moderations;").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 5);
            let id = format!("{}", mysql_val_to_parser_val(&vals[0]));
            let mod_id = format!("{}", mysql_val_to_parser_val(&vals[1]));
            let story_id = format!("{}", mysql_val_to_parser_val(&vals[2]));
            let user_id = format!("{}", mysql_val_to_parser_val(&vals[3]));
            let action = format!("{}", mysql_val_to_parser_val(&vals[4]));
            results.push((id, mod_id, story_id, user_id, action));
        }
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], ("'1'".to_string(), "'1'".to_string(), "'0'".to_string(), "'2'".to_string(), "'bad story!'".to_string()));
        assert_eq!(results[1], ("'2'".to_string(), "'2'".to_string(), "'0'".to_string(), "'1'".to_string(), "'worst story!'".to_string()));

        // two ghost entries added, beginning from GHOST_ID_START
        let mut results = vec![];
        let res = db.query_iter(r"SELECT * FROM ghosts;").unwrap();
        for row in res {
            let vals = row.unwrap().unwrap();
            assert_eq!(vals.len(), 2);
            let gid = format!("{}", mysql_val_to_parser_val(&vals[0]));
            let uid = format!("{}", mysql_val_to_parser_val(&vals[1]));
            results.push((gid, uid));
        }
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (format!("'{}'", GHOST_ID_START), "'1'".to_string()));
        assert_eq!(results[1], (format!("'{}'", GHOST_ID_START+1), "'2'".to_string()));
        assert_eq!(results[2], (format!("'{}'", GHOST_ID_START+2), "'2'".to_string()));
        assert_eq!(results[3], (format!("'{}'", GHOST_ID_START+3), "'1'".to_string()));

        // TEST 4: update users correctly changes ghost values
        
        drop(db);
        jh.join().unwrap();
    }
}

#[test]
fn test_dttrans_insert() {
    let tester = Tester::new();
    tester.setup_test_database();
    let cfg = config::parse_config(CONFIG).unwrap();
}
