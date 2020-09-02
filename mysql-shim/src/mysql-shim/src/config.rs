use std::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserTable{
    pub name : String,
    pub id_col : String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataTable{
    pub name : String,
    pub user_cols : Vec<String>,
    pub id_col: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataTable{
    pub name : String,
    pub user_cols : Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonConfig {
    pub user_table: UserTable,
    pub data_tables: Vec<JsonDataTable>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub user_table: UserTable,
    pub data_tables: Vec<DataTable>,
}

pub fn parse_config(contents: &str) -> io::Result<Config> {
    let jcfg: JsonConfig = serde_json::from_str(contents)?;
    let cfg = Config {
        user_table: jcfg.user_table,
        data_tables: jcfg.data_tables
            .iter()
            .map(|jdt| DataTable{
                name: jdt.name.clone(),
                user_cols: jdt.user_cols.clone(),
                id_col: String::new(),
            })
        .collect(),
    };
    return Ok(cfg);
}
