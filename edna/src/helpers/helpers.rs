use crate::*;
use log::debug;
use std::collections::HashMap;
use serde::Serialize;

pub fn size_of_vec<T>(vec: &Vec<T>) -> usize {
    std::mem::size_of_val(vec) + vec.capacity() * std::mem::size_of::<T>()
}

pub fn serialize_to_bytes<T: Serialize>(item: &T) -> Vec<u8> {
    bincode::serialize(item).unwrap()
}

pub fn vec_to_expr<T: Serialize>(vs: &Vec<T>) -> Expr {
    if vs.is_empty() {
        Expr::Value(Value::Null)
    } else {
        let serialized = serde_json::to_string(&vs).unwrap();
        Expr::Value(Value::String(serialized))
    }
}

pub fn get_value_of_col(row: &Vec<RowVal>, col: &str) -> Option<String> {
    for rv in row {
        if &rv.column == col {
            return Some(rv.value.clone());
        }
    }
    debug!("No value for col {} in row {:?}", col, row);
    None
}

pub fn get_ids(id_cols: &Vec<String>, row: &Vec<RowVal>) -> Vec<RowVal> {
    id_cols
        .iter()
        .map(|id_col| RowVal {
            column: id_col.clone(),
            value: get_value_of_col(row, &id_col).unwrap(),
        })
        .collect()
}

pub fn get_select_of_ids(ids: &Vec<RowVal>) -> Expr {
    let mut selection = Expr::Value(Value::Boolean(true));
    for id in ids {
        let eq_selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(id.column.clone())])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::String(id.value.clone()))),
        };
        selection = Expr::BinaryOp {
            left: Box::new(selection),
            op: BinaryOperator::And,
            right: Box::new(eq_selection),
        };
    }
    selection
}

pub fn get_select_of_row(id_cols: &Vec<String>, row: &Vec<RowVal>) -> Expr {
    let mut selection = Expr::Value(Value::Boolean(true));
    let ids = get_ids(id_cols, row);
    for (i, id) in ids.iter().enumerate() {
        let eq_selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(vec![Ident::new(id_cols[i].clone())])),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::String(id.value.clone()))),
        };
        selection = Expr::BinaryOp {
            left: Box::new(selection),
            op: BinaryOperator::And,
            right: Box::new(eq_selection),
        };
    }
    selection
}

pub fn merge_vector_hashmaps<T: Clone>(
    h1: &HashMap<String, Vec<T>>,
    h2: &HashMap<String, Vec<T>>,
) -> HashMap<String, Vec<T>> {
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

pub fn max(a: u64, b: u64) -> u64 {
    if a >= b {
        a
    } else {
        b
    }
}
