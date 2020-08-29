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
    pub user_cols : Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub user_table: UserTable,
    pub data_tables: Vec<DataTable>,
}

pub fn parse_config(contents: &str) -> io::Result<Config> {
    let cfg: Config = serde_json::from_str(contents)?;
    return Ok(cfg);
}
