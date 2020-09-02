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

use datadriven::walk;

use std::*;
use sql_parser::parser;
use mysql_shim::*;

const SCHEMA : &'static str = include_str!("./schema.sql");
const CONFIG : &'static str = include_str!("./config.json");

fn trim_one<'a>(s: &'a str) -> &'a str {
    if s.ends_with('\n') {
        &s[..s.len() - 1]
    } else {
        s
    }
}

#[test]
fn datadriven() {
    /*struct Visitor<'a> {
        seen_idents: Vec<&'a str>,
    }

    impl<'a> Visit<'a> for Visitor<'a> {
        fn visit_ident(&mut self, ident: &'a Ident) {
            self.seen_idents.push(ident.as_str());
        }
    }*/

    let cfg = config::parse_config(CONFIG).unwrap();
    walk("tests/testdata", |f| {
        f.run(|test_case| -> String {
            let mut mv_trans = mv_transformer::MVTransformer::new(cfg.clone());
            let mut dt_trans = datatable_transformer::DataTableTransformer::new(cfg.clone());
            match test_case.directive.as_str() {
                "parse-statement" => {
                    let sql = trim_one(&test_case.input).to_owned();
                    match parser::parse_statements(sql) {
                        Ok(s) => {
                            if s.len() != 1 {
                                "expected exactly one statement".to_string()
                            } else {
                                let stmt = s.iter().next().unwrap();
                                let (mv_stmt, write_query) = mv_trans.stmt_to_mv_stmt(stmt);
                                if let Some(dt_stmt) = dt_trans.stmt_to_datatable_stmt(&stmt) {
                                    // TODO
                                }
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
                                // TODO(justin): it would be nice to have a middle-ground between this
                                // all-on-one-line and {:#?}'s huge number of lines.
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
