use std::fs::File;
use std::io::prelude::*;
use std::time::Duration;
use log::{warn};

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
    pub nqueries: u64,
    pub qtype: QueryType,
}

impl QueryStat {
    pub fn new() -> Self {
        QueryStat {
            duration: Duration::new(0,0),
            nqueries : 1,
            qtype : QueryType::None,
        }
    }

    pub fn clear(&mut self) {
        self.duration = Duration::new(0,0);
        self.nqueries = 1;
        self.qtype = QueryType::None;
    }
}

pub fn print_stats(stats: &Vec<QueryStat>, filename: String) {
    let mut read_latencies = vec![];
    let mut insert_latencies = vec![];
    let mut update_latencies = vec![];
    let mut other_latencies = vec![];
    let mut max = 0;
    for stat in stats {
        if stat.duration.as_micros() > max {
            max = stat.duration.as_micros();
        }
        match stat.qtype {
            QueryType::Read => {
                read_latencies.push((stat.nqueries, stat.duration.as_micros()));
            }
            QueryType::Update => {
                update_latencies.push((stat.nqueries, stat.duration.as_micros()));
            }
            QueryType::Insert => {
                insert_latencies.push((stat.nqueries, stat.duration.as_micros()));
            }
            _ => {
                other_latencies.push((stat.nqueries, stat.duration.as_micros()));
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
    for v in other_latencies {
        file.write(format!("{},{}; ", v.0, v.1).as_bytes()).unwrap();
    }
    file.flush().unwrap();
}
