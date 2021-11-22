use clap::{App, Arg};
use std::str::FromStr;

#[cfg_attr(rustfmt, rustfmt_skip)]

pub const TEST_BASELINE : u64 = 0;
pub const TEST_NORMAL_DISGUISING : u64 = 1;
pub const TEST_BATCH_DISGUISING : u64 = 2;

#[derive(Clone, Debug)]
pub struct Args {
    pub nusers: usize,
    pub ndisguising: usize,
    pub nlec: usize,
    pub nqs: usize,
    pub test: u64,
    pub db: String,
}

pub fn parse_args() -> Args {
    let args = App::new("websubmit")
        .version("0.0.1")
        .about("Class submission system.")
        .arg(
            Arg::with_name("db")
                .long("db")
                .takes_value(true)
                .value_name("DBNAME")
                .default_value("myclass"),
        )
        .arg(
            Arg::with_name("ndisguising")
                .short("d")
                .long("ndisguising")
                .takes_value(true)
                .value_name("NDIGUISING")
                .default_value("2"),
        )
        .arg(
            Arg::with_name("nusers")
                .short("u")
                .long("nusers")
                .takes_value(true)
                .value_name("NUSERS")
                .default_value("2"),
        )
        .arg(
            Arg::with_name("nlec")
                .short("l")
                .long("nlec")
                .takes_value(true)
                .value_name("NLEC")
                .default_value("2"),
        )
        .arg(
            Arg::with_name("nqs")
                .short("q")
                .long("nqs")
                .takes_value(true)
                .value_name("NQS")
                .default_value("2"),
        ).arg(
            Arg::with_name("test")
                .short("t")
                .long("test")
                .takes_value(true)
                .value_name("test")
                .default_value("false"),
        )
        .get_matches();
    Args {
        nusers: usize::from_str(args.value_of("nusers").unwrap()).unwrap(),
        ndisguising: usize::from_str(args.value_of("ndisguising").unwrap()).unwrap(),
        nlec: usize::from_str(args.value_of("nlec").unwrap()).unwrap(),
        nqs: usize::from_str(args.value_of("nqs").unwrap()).unwrap(),
        test: u64::from_str(args.value_of("test").unwrap()).unwrap(),
        db: String::from(args.value_of("db").unwrap()),
    }
}
