use clap::{App, Arg};
use std::str::FromStr;

#[cfg_attr(rustfmt, rustfmt_skip)]

#[derive(Clone, Debug)]
pub struct Args {
    pub nusers: usize,
    //pub ndisguising: usize,
    pub nsleep: u64,
    pub nlec: usize,
    pub nqs: usize,
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
            Arg::with_name("nsleep")
                .short("s")
                .long("nsleep")
                .takes_value(true)
                .value_name("NSLEEP")
                .default_value("10000"),
        )
        /*.arg(
            Arg::with_name("ndisguising")
                .short("d")
                .long("ndisguising")
                .takes_value(true)
                .value_name("NDIGUISING")
                .default_value("2"),
        )*/
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
                .default_value("4"),
        )
        .get_matches();
    Args {
        nusers: usize::from_str(args.value_of("nusers").unwrap()).unwrap(),
        nsleep: u64::from_str(args.value_of("nsleep").unwrap()).unwrap(),
        nlec: usize::from_str(args.value_of("nlec").unwrap()).unwrap(),
        nqs: usize::from_str(args.value_of("nqs").unwrap()).unwrap(),
        db: String::from(args.value_of("db").unwrap()),
    }
}
