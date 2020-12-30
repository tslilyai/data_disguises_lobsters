use sql_parser::ast::{ObjectName};
use std::*;
use log::{debug, warn};
use crate::{policy, views};

pub fn contains_ghosted_columns(decor_config: &policy::Config, table_name: &str) -> bool {
    if let Some(policies) = decor_config.table2policies.get(&table_name.to_string()) {
        for policy in policies {
            match policy.pc_policy {
                policy::UnsubscribePolicy::Decorrelate(_) => return true,
                _ => ()
            }

        }      
    }
    false
}
pub fn get_ghosted_cols_of_datatable(decor_config: &policy::Config, table_name: &ObjectName) -> Vec<(String, String)> {
    let mut c = vec![];
    if let Some(policies) = decor_config.table2policies.get(&table_name.to_string()) {
        for policy in policies {
            match policy.pc_policy {
                policy::UnsubscribePolicy::Decorrelate(_) => c.push((policy.column, policy.parent)),
                _ => ()
            }
        }
    } 
    c
}

pub fn get_ghosted_col_indices_of_datatable(decor_config: &policy::Config, table_name: &str, columns: &Vec<sql_parser::ast::TableColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(policies) = decor_config.table2policies.get(table_name) {
        for policy in policies {
            match policy.pc_policy {
                policy::UnsubscribePolicy::Decorrelate(_) => cis.push(
                    (get_col_index(&policy.column, columns).unwrap(),
                        policy.parent.clone())),
                _ => (),
            }
        } 
    }
    cis
}

pub fn get_parent_col_indices_of_datatable(decor_config: &policy::Config, table_name: &ObjectName, columns: &Vec<sql_parser::ast::ColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(policies) = decor_config.table2policies.get(&table_name.to_string()) {
        for policy in policies {
            cis.push(
                (columns.iter().position(|c| c.name.to_string() == policy.column).unwrap(), 
                 policy.parent.clone()));
        } 
    }
    cis
}

pub fn tablecolumn_matches_col(c: &views::TableColumnDef, col: &str) -> bool {
    //warn!("matching {} or {} to {}", c.colname, c.fullname, col);
    (col.len() < c.fullname.len() && c.colname == col) || c.fullname == col
}

pub fn get_col_index(col: &str, columns: &Vec<views::TableColumnDef>) -> Option<usize> {
    let pos = columns.iter().position(|c| tablecolumn_matches_col(c, col));
    warn!("found position {:?} for col {}", pos, col);
    pos
}
