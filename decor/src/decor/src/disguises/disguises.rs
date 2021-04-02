use sql_parser::ast::*;

pub struct DisguiseTxn {
    pub pred: Select,

    // vault updates always occur before obj updates
    // (ala write-ahead logging)
    pub vault_updates: Vec<Statement>,
    
    // UPDATE/DELETE _ (all conditioned on WHERE pred)
    pub obj_updates: Vec<Statement>,
}

pub struct Disguise(Vec<DisguiseTxn>);

pub struct Application {
    pub disguises: Vec<Disguise>,
    pub schema: Vec<CreateTableStatement>,
    pub vaults: Vec<CreateTableStatement>
}
