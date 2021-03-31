use sql_parser::ast::*;

pub const TARGET: &'static str = "target";

pub struct DisguiseTxn {
    pub predicate: Vec<Statement>,
    pub obj_updates: Vec<Statement>,
    pub vault_updates: Vec<Statement>,
}

pub struct Disguise {
    pub txns: Vec<DisguiseTxn>,
    pub vaults: Vec<CreateTableStatement>
}

pub struct Application {
    pub disguises: Vec<Disguise>,
    pub schema: Vec<CreateTableStatement>
}
