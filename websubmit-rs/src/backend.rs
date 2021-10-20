pub use crate::datatype::DataType;
use mysql::prelude::*;
use mysql::Opts;
use edna::EdnaClient;
use std::collections::HashMap;
use sql_parser::ast::*;

pub struct MySqlBackend {
    pub handle: mysql::Conn,
    _rt: tokio::runtime::Runtime,
    _log: slog::Logger,

    _schema: String,

    // table name --> (keys, columns)
    tables: HashMap<String, (Vec<String>, Vec<String>)>,

    // view name --> query
    views: HashMap<String, String>,
}

impl MySqlBackend {
    pub fn new(dbname: &str, log: Option<slog::Logger>) -> Result<Self, std::io::Error> {
        let log = match log {
            None => slog::Logger::root(slog::Discard, o!()),
            Some(l) => l,
        };

        let schema = std::fs::read_to_string("src/schema.sql")?;

        debug!(log, "Connecting to MySql DB and initializing schema {}...", dbname);
        EdnaClient::new(true /*prime*/, dbname, schema, true /*in-mem*/);
        let mut db = mysql::Conn::new(Opts::from_url(&format!("mysql://tslilyai:pass@127.0.0.1/{}", dbname))).unwrap();
        assert_eq!(db.ping(), true);

        // save table and view information
        let mut tables = HashMap::new();
        let mut stmt = String::new();
        let mut is_view = false;
        for line in schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if line.starts_with("QUERY") {
                is_view = true;
            }
            if !stmt.is_empty() {
                stmt.push_str(" ");
            }
            stmt.push_str(line);
            if stmt.ends_with(';') {
                if is_view {
                    let t = stmt.trim_start_matches("QUERY ");
                    let end_bytes = t.find(":").unwrap_or(t.len()); 
                    let name = &t[..end_bytes];
                    let query = &t[(end_bytes+1)..];
                    db.query_drop("CREATE VIEW {} AS {}", name, query).expected("could not create view {} as {}", name, query);
                } else {
                    let asts = sql_parser::parser::parse_statements(stmt.to_string()).expect("could not parse stmt {}!", stmt);
                    if asts.len() != 1 {
                        panic!("More than one stmt {:?}", asts);
                    }
                    let parsed = asts[0];
                    
                    if let Statement::CreateTable(CreateTableStatement{
                        name,
                        columns,
                        constraints,
                        ..
                    }) = parsed {
                        let mut tab_keys = vec![];
                        let tab_cols = columns.iter().map(|c| c.name.to_string()).collect();
                        for constraint in constraints {
                            match constraint {
                                TableConstraint::Unique{columns, is_primary, ..} =>  {
                                    if *is_primary {
                                        tab_keys.push(columns.iter().map(|c| c.to_string()));
                                    }
                                }
                            }
                        }
                        tables.push(name, (tab_keys, tab_cols))
                    }
                }
                stmt = String::new();
                is_view = false;
            }
        }
        Ok(MySqlBackend {
            handle: db,
            _log: log,
            _schema: schema.to_owned(),

            tables: tables,
        })
    }

    pub fn view_lookup<T: FromRow>(&mut self, view: &str, keys: Vec<DataType>) -> Vec<Vec<DataType>> {
        let mut conds = vec![];
        for (i, value) in keys.iter().enumerate() {
            conds.push(format!("{} = {}", key_cols[i], value));
        }
        let q = format!(r"SELECT FROM {} WHERE {};", view, conds.join(","));
        let res = self.handle.query_iter(q).expect("failed to select from {}, query {}!", view, q);
        let mut rows = vec![];
        for row in res {
            let rowvals = res.unwrap().unwrap();
            let vals: Vec<DataType> = rowvals 
                .iter()
                .map(|v| v.clone().into())
                .collect();
            rows.push(vals);
        }
        return rows;
    }

    pub fn insert(&mut self, table: &str, vals: Vec<DataType>) {
        let q = format!(r"INSERT INTO {} VALUES ({});", table, vals.join(","));
        self.handle.query_drop(q).expect("failed to insert into {}, query {}!", table, q);
    }

    pub fn update(&mut self, table: &str, keys: Vec<DataType>, vals: Vec<(usize, DataType)>) {
        let (key_cols, cols) = self.tables.get(table).expect("Incorrect table in update? {}", table);
        let mut assignments = vec![];
        for (index, value) in vals {
            assignments.push(format!("{} = {}", cols[index], value));
        }
        let mut conds = vec![];
        for (i, value) in keys.iter().enumerate() {
            conds.push(format!("{} = {}", key_cols[i], value));
        }
        let q = format!(r"UPDATE {} SET {} WHERE {};", table, assignments.join(","), conds.join(" AND "));
        self.handle.query_drop(q).expect("failed to update {}, query {}!", table, q);
    }

    pub fn insert_or_update(&mut self, table: &str, rec: Vec<DataType>, update_vals: Vec<(usize,DataType)>) {
        let (key_cols, cols) = self.tables.get(table).expect("Incorrect table in update? {}", table);
        let mut assignments = vec![];
        for (index, value) in update_vals {
            assignments.push(format!("{} = {}", cols[index], value));
        }
        let mut conds = vec![];
        for (i, value) in rec.iter().enumerate() {
            conds.push(format!("{} = {}", key_cols[i], value));
        }
        let q = format!(r"UPDATE {} SET {} WHERE {} IF @@ROWCOUNT=0 INSERT INTO {} VALUES ({});",
            table, assignments.join(","), conds.join(" AND "), table, rec.join(","));
        self.handle.query_drop(q).expect("failed to insert-update {}, query {}!", table, q);
    }
}
