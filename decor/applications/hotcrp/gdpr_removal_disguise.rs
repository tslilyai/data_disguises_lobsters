use decor::disguise::*;
use sql_parser::ast::{DataType, Expr, Ident, ObjectName, Statement, UnaryOperator, Value};

pub fn get_gdpr_removal_disguise() -> Disguise {
    let mut txns = vec![];
    Disguise(txns)
}

