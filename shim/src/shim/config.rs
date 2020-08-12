use std::fs::File;
use std::io::Read;
use std::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct UserTable{
    pub name : String,
    pub id_col : String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct DataTable{
    pub name : String,
    pub id_col : String,
    pub user_cols : Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub user_table: UserTable,
    pub data_tables: Vec<DataTable>,
}

pub fn parse_config(filename : String) -> io::Result<Config> {
    let mut file = File::open(filename).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let cfg: Config = serde_json::from_str(&contents)?;
    return Ok(cfg);
}
