use sql_parser::ast::{ObjectName};
use std::*;
use log::{debug};
use crate::{policy, views};

pub fn get_guise_parent_key_names_of_datatable(decor_config: &policy::MaskPolicy, table_name: &ObjectName) -> Vec<(String, String)> {
    let mut c = vec![];
    if let Some(policies) = decor_config.edge_policies.get(&table_name.to_string()) {
        for policy in &*policies.clone() {
            match policy.pc_policy {
                policy::EdgePolicyType::Decorrelate(_) => c.push((policy.column.clone(), policy.parent.clone())),
                _ => ()
            }
        }
    } 
    c
}

pub fn get_guise_parent_key_indices_of_datatable(decor_config: &policy::MaskPolicy, table_name: &str, columns: &Vec<views::TableColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(policies) = decor_config.edge_policies.get(table_name) {
        for policy in &*policies.clone() {
            match policy.pc_policy {
                policy::EdgePolicyType::Decorrelate(_) => cis.push(
                    (get_col_index(&policy.column, columns).unwrap(),
                        policy.parent.clone())),
                _ => (),
            }
        } 
    }
    cis
}

pub fn get_parent_col_indices_of_datatable(decor_config: &policy::MaskPolicy, table_name: &ObjectName, columns: &Vec<sql_parser::ast::ColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(policies) = decor_config.edge_policies.get(&table_name.to_string()) {
        for policy in &*policies.clone() {
            cis.push(
                (columns.iter().position(|c| c.name.to_string() == policy.column).unwrap(), 
                 policy.parent.clone()));
        } 
    }
    cis
}

pub fn tablecolumn_matches_col(c: &views::TableColumnDef, col: &str) -> bool {
    debug!("matching {} or {} to {}", c.colname, c.fullname, col);
    (col.len() < c.fullname.len() && c.colname == col) || c.fullname == col
}

pub fn get_col_index(col: &str, columns: &Vec<views::TableColumnDef>) -> Option<usize> {
    let pos = columns.iter().position(|c| tablecolumn_matches_col(c, col));
    debug!("found position {:?} for col {}", pos, col);
    pos
}

