use std::collections::HashMap;
use crate::types::*;
use crate::*;
use crate::helpers::*;

pub fn get_ids(tableinfo: &TableInfo, row: &Vec<RowVal>) -> Vec<String> {
    tableinfo
        .id_cols
        .iter()
        .map(|id_col| get_value_of_col(row, &id_col).unwrap())
        .collect()
}

pub fn get_select(
    user_id: Option<u64>,
    tableinfo: &TableInfo,
    disguise: &Disguise,
) -> Option<Expr> {
    let mut select = None;
    match user_id {
        Some(user_id) => {
            let mut selection = Expr::Value(Value::Boolean(false));
            // if this is the user table, check for ID equivalence
            if tableinfo.name == disguise.guise_info.name {
                selection = Expr::BinaryOp {
                    left: Box::new(selection),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(
                            disguise.guise_info.id_col.to_string(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                    }),
                };
            } else {
                // otherwise, we want to remove all objects possibly referencing the user
                // NOTE : this assumes that all "fks_to_decor" point to users table
                for fk in &tableinfo.fks_to_decor {
                    selection = Expr::BinaryOp {
                        left: Box::new(selection),
                        op: BinaryOperator::Or,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(vec![Ident::new(
                                fk.referencer_col.to_string(),
                            )])),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                        }),
                    };
                }
            }
            select = Some(selection);
        }
        None => (),
    }
    select
}

pub fn get_select_of_row(tableinfo: &TableInfo, row: &Vec<RowVal>) -> Expr {
    let mut selection = Expr::Value(Value::Boolean(true));
    let ids = get_ids(tableinfo, row);
    for (i, id) in ids.iter().enumerate() {
        let eq_selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(tableinfo.id_cols[i].clone())])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::String(id.to_string()))),
        };
        selection = Expr::BinaryOp {
            left: Box::new(selection),
            op: BinaryOperator::And,
            right: Box::new(eq_selection),
        };
    }
    selection
}

pub fn merge_vector_hashmaps(
    h1: &HashMap<String, Vec<String>>,
    h2: &HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    let mut hm = h1.clone();
    for (k, vs1) in hm.iter_mut() {
        if let Some(vs2) = h2.get(k) {
            vs1.extend_from_slice(vs2);
        }
    }
    for (k, vs2) in h2.iter() {
        if let Some(vs1) = hm.get_mut(k) {
            vs1.extend_from_slice(vs2);
        } else {
            hm.insert(k.to_string(), vs2.clone());
        }
    }
    hm
}

