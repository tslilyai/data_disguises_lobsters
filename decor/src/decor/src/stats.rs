use sql_parser::ast::*;
use sql_parser::parser::*;
use std::fs::File;
use std::io::prelude::*;
use std::time::Duration;
use std::*;

#[derive(Debug, Clone)]
pub enum QueryType {
    Read,
    Update,
    Insert,
    Delete,
    WriteOther,
    Unsub,
    Resub,
    None,
}

#[derive(Debug, Clone)]
pub struct QueryStat {
    pub duration: Duration,
    pub nqueries: usize,
    pub nobjects: usize,
    pub nqueries_vault: usize,
    pub remove_dur: Duration,
    pub decor_dur: Duration,
    pub mod_dur: Duration,
    pub record_dur: Duration,
    pub qtype: QueryType,
}

impl QueryStat {
    pub fn new() -> Self {
        QueryStat {
            duration: Duration::new(0, 0),
            remove_dur: Duration::new(0, 0),
            decor_dur: Duration::new(0, 0),
            mod_dur: Duration::new(0, 0),
            record_dur: Duration::new(0, 0),
            nqueries: 0,
            nobjects: 0,
            nqueries_vault: 0,
            qtype: QueryType::None,
        }
    }

    pub fn clear(&mut self) {
        self.duration = Duration::new(0, 0);
        self.remove_dur = Duration::new(0, 0);
        self.decor_dur = Duration::new(0, 0);
        self.mod_dur = Duration::new(0, 0);
        self.record_dur = Duration::new(0, 0);
        self.nqueries = 0;
        self.nqueries_vault = 0;
        self.qtype = QueryType::None;
    }
}

pub fn get_qtype(query: &str) -> Result<QueryType, mysql::Error> {
    let asts = parse_statements(query.to_string());
    match asts {
        Err(e) => Err(mysql::Error::IoError(io::Error::new(
            io::ErrorKind::InvalidInput,
            e,
        ))),
        Ok(asts) => {
            if asts.len() != 1 {
                return Err(mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "More than one stmt",
                )));
            }
            match asts[0] {
                Statement::Insert(InsertStatement { .. }) => Ok(QueryType::Insert),
                Statement::Update(UpdateStatement { .. }) => Ok(QueryType::Update),
                Statement::Delete(DeleteStatement { .. }) => Ok(QueryType::Delete),
                Statement::CreateView(CreateViewStatement { .. }) => Ok(QueryType::WriteOther),
                Statement::CreateTable(CreateTableStatement { .. })
                | Statement::CreateIndex(CreateIndexStatement { .. })
                | Statement::AlterObjectRename(AlterObjectRenameStatement { .. })
                | Statement::DropObjects(DropObjectsStatement { .. }) => Ok(QueryType::WriteOther),
                _ => Ok(QueryType::Read),
            }
        }
    }
}

pub fn print_stats(stats: &Vec<QueryStat>, filename: String) {
    let mut read_latencies = vec![];
    let mut insert_latencies = vec![];
    let mut update_latencies = vec![];
    let mut unsub_latencies = vec![];
    let mut resub_latencies = vec![];
    let mut other_latencies = vec![];
    let mut max = 0;
    for stat in stats {
        if stat.duration.as_micros() > max {
            max = stat.duration.as_micros();
        }
        match stat.qtype {
            QueryType::Read => {
                read_latencies.push((stat.nobjects, stat.duration.as_micros()));
            }
            QueryType::Update => {
                update_latencies.push((stat.nobjects, stat.duration.as_micros()));
            }
            QueryType::Insert => {
                insert_latencies.push((stat.nobjects, stat.duration.as_micros()));
            }
            QueryType::Unsub => {
                unsub_latencies.push((stat.nobjects, stat.duration.as_micros()));
            }
            QueryType::Resub => {
                resub_latencies.push((stat.nobjects, stat.duration.as_micros()));
            }
            _ => {
                //error!("Found other query type {:?}", stat.qtype);
                other_latencies.push((stat.nobjects, stat.duration.as_micros()));
            }
        }
    }

    let mut file = File::create(format!("{}.csv", filename)).unwrap();
    for v in read_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.write(b"\n").unwrap();
    for v in update_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.write(b"\n").unwrap();
    for v in insert_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.write(b"\n").unwrap();
    for v in unsub_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.write(b"\n").unwrap();
    for v in resub_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.write(b"\n").unwrap();
    for v in other_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.flush().unwrap();
}
