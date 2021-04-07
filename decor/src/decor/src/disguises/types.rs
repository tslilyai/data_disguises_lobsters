use serde::{Deserialize, Serialize};
use sql_parser::ast::Statement;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowVal {
    pub column: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FK {
    pub referencer_col: String,
    pub fk_name: String,
    pub fk_col: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFKs {
    pub name: String,
    pub fks: Vec<FK>,
}

pub struct Application {
    pub disguises: Vec<Box<dyn Fn(&mut mysql::Conn) -> Result<(), mysql::Error>>>,
    pub schema: Vec<Statement>,
    pub vault: Vec<Statement>,
}
