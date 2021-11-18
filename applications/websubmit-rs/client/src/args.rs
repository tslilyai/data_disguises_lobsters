use clap::{App, Arg};
use std::str::FromStr;

#[cfg_attr(rustfmt, rustfmt_skip)]

#[derive(Clone, Debug)]
pub struct Args {
    pub nusers: usize,
    pub ndisguising: usize,
    pub nlec: usize,
    pub nqs: usize,
    pub niters: usize,
    pub ndisguise_iters: usize,
    pub baseline: bool,
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
        )
        .arg(
            Arg::with_name("niters")
                .short("i")
                .long("niters")
                .takes_value(true)
                .value_name("NITERS")
                .default_value("2000"),
        ).arg(
            Arg::with_name("ndisguise_iters")
                .long("ndisguise_iters")
                .takes_value(true)
                .value_name("NDISGUISE_ITERS")
                .default_value("200"),
        ).arg(
            Arg::with_name("baseline")
                .short("b")
                .long("baseline")
                .takes_value(true)
                .value_name("BASELINE")
                .default_value("false"),
        )
        .get_matches();
    Args {
        nusers: usize::from_str(args.value_of("nusers").unwrap()).unwrap(),
        ndisguising: usize::from_str(args.value_of("ndisguising").unwrap()).unwrap(),
        nlec: usize::from_str(args.value_of("nlec").unwrap()).unwrap(),
        nqs: usize::from_str(args.value_of("nqs").unwrap()).unwrap(),
        niters: usize::from_str(args.value_of("niters").unwrap()).unwrap(),
        ndisguise_iters: usize::from_str(args.value_of("ndisguise_iters").unwrap()).unwrap(),
        baseline: bool::from_str(args.value_of("baseline").unwrap()).unwrap(),
        db: String::from(args.value_of("db").unwrap()),
    }
}
