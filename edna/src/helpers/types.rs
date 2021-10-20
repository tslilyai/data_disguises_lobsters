use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColFormat {
    Quoted,
    NonQuoted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RowVal {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableColumns {
    pub name: String,
    pub cols: Vec<String>,
    pub colformats: Vec<ColFormat>,
}
