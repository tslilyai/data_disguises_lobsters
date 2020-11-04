use std::time::Duration;
use log::{warn};
use plotlib::page::Page;
use plotlib::repr::{Histogram, HistogramBins};
use plotlib::view::ContinuousView;
use plotlib::style::{PointMarker, PointStyle};

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

pub fn print_stats(stats: &Vec<QueryStat>) {
    let mut qs_vs_latency = vec![];
    let mut read_latencies = vec![];
    let mut insert_latencies = vec![];
    let mut update_latencies = vec![];
    let mut other_latencies = vec![];
    for stat in stats {
        qs_vs_latency.push((stat.nqueries, stat.duration));
        match stat.qtype {
            QueryType::Read => {
                read_latencies.push(stat.duration.as_millis() as f64)
            }
            QueryType::Update => {
                update_latencies.push(stat.duration.as_millis() as f64)
            }
            QueryType::Insert => {
                insert_latencies.push(stat.duration.as_millis() as f64)
            }
            _ => 
                other_latencies.push(stat.duration.as_millis() as f64)
        }
    }

    let s1: Histogram = Histogram::from_slice(
        &read_latencies[..],
        HistogramBins::Count(30));
    let s2: Histogram = Histogram::from_slice(
        &update_latencies[..], 
        HistogramBins::Count(30));

    let v = ContinuousView::new()
        .add(s1)
        .add(s2)
        .x_range(0., 10.)
        .y_range(0., 100.)
        .x_label("Some varying variable")
        .y_label("The response of something");

    // A page with a single view is then saved to an SVG file
    Page::single(&v).save("histograms.svg").unwrap();
}
