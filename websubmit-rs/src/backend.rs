use edna::EdnaClient;
use mysql::prelude::*;
use mysql::Opts;
pub use mysql::Value;
use mysql::*;
use sql_parser::ast::*;
use std::collections::HashMap;

pub struct MySqlBackend {
    pub handle: mysql::Conn,
    _log: slog::Logger,
    _schema: String,

    // table name --> (keys, columns)
    tables: HashMap<String, (Vec<String>, Vec<String>)>,
    queries: HashMap<String, mysql::Statement>,
    edna: EdnaClient,
}

impl MySqlBackend {
    pub fn new(dbname: &str, log: Option<slog::Logger>) -> Result<Self> {
        let log = match log {
            None => slog::Logger::root(slog::Discard, o!()),
            Some(l) => l,
        };

        let schema = std::fs::read_to_string("src/schema.sql")?;

        // connect to everything
        debug!(
            log,
            "Connecting to MySql DB and initializing schema {}...", dbname
        );
        let edna = EdnaClient::new(true /*prime*/, dbname, &schema, true /*in-mem*/);
        let mut db = mysql::Conn::new(
            Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname)).unwrap(),
        )
        .unwrap();
        assert_eq!(db.ping(), true);

        // save table and query information
        let mut tables = HashMap::new();
        let mut queries = HashMap::new();
        let mut stmt = String::new();
        let mut is_query = false;
        let mut is_view = false;
        for line in schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if line.starts_with("QUERY") {
                is_query = true;
            }
            if line.starts_with("CREATE VIEW") {
                is_view = true;
            }
            if !stmt.is_empty() {
                stmt.push_str(" ");
            }
            stmt.push_str(line);
            if stmt.ends_with(';') {
                if is_view {
                    db.query_drop(stmt).unwrap();
                }
                else if is_query {
                    let t = stmt.trim_start_matches("QUERY ");
                    let end_bytes = t.find(":").unwrap_or(t.len());
                    let name = &t[..end_bytes];
                    let query = &t[(end_bytes + 1)..];
                    let prepstmt = db.prep(query).unwrap();
                    queries.insert(name.to_string(), prepstmt);
                } else {
                    let asts = sql_parser::parser::parse_statements(stmt.to_string())
                        .expect(&format!("could not parse stmt {}!", stmt));
                    if asts.len() != 1 {
                        panic!("More than one stmt {:?}", asts);
                    }
                    let parsed = &asts[0];

                    if let sql_parser::ast::Statement::CreateTable(CreateTableStatement {
                        name,
                        columns,
                        constraints,
                        ..
                    }) = parsed
                    {
                        let mut tab_keys = vec![];
                        let tab_cols = columns.iter().map(|c| c.name.to_string()).collect();
                        for constraint in constraints {
                            match constraint {
                                TableConstraint::Unique {
                                    columns,
                                    is_primary,
                                    ..
                                } => {
                                    if *is_primary {
                                        tab_keys
                                            .push(columns.iter().map(|c| c.to_string()).collect());
                                    }
                                }
                                _ => (),
                            }
                        }
                        tables.insert(name.to_string(), (tab_keys, tab_cols));
                    }
                }
                stmt = String::new();
                is_query = false;
                is_view = false;
            }
        }
        Ok(MySqlBackend {
            handle: db,
            _log: log,
            _schema: schema.to_owned(),

            tables: tables,
            queries: queries,
            edna: edna,
        })
    }

    pub fn query_exec(&mut self, qname: &str, keys: Vec<Value>) -> Vec<Vec<Value>> {
        let q = self.queries.get(qname).unwrap();
        let res = self
            .handle
            .exec_iter(q, keys)
            .expect(&format!("failed to select from {}", qname));
        let mut rows = vec![];
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals: Vec<Value> = rowvals.iter().map(|v| v.clone().into()).collect();
            rows.push(vals);
        }
        return rows;
    }

    pub fn insert(&mut self, table: &str, vals: Vec<Value>) {
        let valstrs: Vec<&str> = vals
            .iter()
            .map(|_| "?")
            .collect();
        let q = format!(r"INSERT INTO {} VALUES ({});", table, valstrs.join(","));
        self.handle
            .exec_drop(q.clone(), vals)
            .expect(&format!("failed to insert into {}, query {}!", table, q));
    }

    pub fn update(&mut self, table: &str, keys: Vec<Value>, vals: Vec<(usize, Value)>) {
        let (key_cols, cols) = self
            .tables
            .get(table)
            .expect(&format!("Incorrect table in update? {}", table));
        let mut assignments = vec![];
        let mut args = vec![];
        for (index, value) in vals {
            assignments.push(format!(
                "{} = ?",
                cols[index],
            ));
            args.push(value.clone());
        }
        let mut conds = vec![];
        for (i, value) in keys.iter().enumerate() {
            conds.push(format!(
                "{} = ?",
                key_cols[i],
            ));
            args.push(value.clone());
        }
        let q = format!(
            r"UPDATE {} SET {} WHERE {};",
            table,
            assignments.join(","),
            conds.join(" AND ")
        );
        self.handle
            .exec_drop(q.clone(), args)
            .expect(&format!("failed to update {}, query {}!", table, q));
    }

    pub fn insert_or_update(
        &mut self,
        table: &str,
        rec: Vec<Value>,
        update_vals: Vec<(u64, Value)>,
    ) {
        let (key_cols, cols) = self
            .tables
            .get(table)
            .expect(&format!("Incorrect table in update? {}", table));
        let mut args = vec![];
        let mut assignments = vec![];
        for (index, value) in update_vals {
            assignments.push(format!(
                "{} = ?",
                cols[index as usize],
            ));
            args.push(value.clone());
        }
        let mut conds = vec![];
        for (i, value) in rec.iter().enumerate() {
            conds.push(format!(
                "{} = ?",
                key_cols[i],
            ));
            args.push(value.clone());
        }
        let recstrs: Vec<&str> = rec
            .iter()
            .map(|v| {args.push(v.clone()); "?"})
            .collect();
        let q = format!(
            r"UPDATE {} SET {} WHERE {} IF @@ROWCOUNT=0 INSERT INTO {} VALUES ({});",
            table,
            assignments.join(","),
            conds.join(" AND "),
            table,
            recstrs.join(",")
        );
        self.handle
            .exec_drop(q.clone(), args)
            .expect(&format!("failed to insert-update {}, query {}!", table, q));
    }
}
