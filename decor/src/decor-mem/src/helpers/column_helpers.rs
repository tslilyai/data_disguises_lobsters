use sql_parser::ast::{ObjectName};
use std::*;
use log::{debug};
use crate::{policy, views};

pub fn contains_ghosted_columns(decor_config: &policy::Config, table_name: &str) -> bool {
    decor_config.parent_child_ghosted_tables.contains_key(table_name)
    || decor_config.child_parent_ghosted_tables.contains_key(table_name)
}
pub fn get_ghosted_cols_of_datatable(decor_config: &policy::Config, table_name: &ObjectName) -> Vec<(String, String)> {
    let mut c = vec![];
    if let Some(colnames) = decor_config.parent_child_ghosted_tables.get(&table_name.to_string()) {
        c.append(&mut colnames.clone());
    } 
    c
}
pub fn get_ghosted_col_indices_of(decor_config: &policy::Config, table_name: &str, columns: &Vec<views::TableColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(colnames) = decor_config.parent_child_ghosted_tables.get(&table_name.to_string()) {
        for colname in colnames {
            debug!("Getting index for col {:?} in columns {:?}", colname, columns);
            cis.push((columns.iter().position(|c| c.colname == colname.0).unwrap(), colname.1.clone()));
        } 
    } 
    cis
}

pub fn get_sensitive_col_indices_of(decor_config: &policy::Config, table_name: &str, columns: &Vec<views::TableColumnDef>) -> Vec<(usize, String, f64)> {
    let mut cis = vec![];
    if let Some(colnames) = decor_config.parent_child_sensitive_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.colname == colname.0).unwrap(), colname.1.clone(), colname.2));
        } 
    } 
    cis
}

pub fn get_parent_col_indices_of_datatable(decor_config: &policy::Config, table_name: &ObjectName, columns: &Vec<sql_parser::ast::ColumnDef>) 
    -> Vec<(usize, String)> 
{
    let mut cis = vec![];
    if let Some(colnames) = decor_config.parent_child_ghosted_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.name.to_string() == colname.0).unwrap(), colname.1.clone()));
        } 
    }
    if let Some(colnames) = decor_config.parent_child_sensitive_tables.get(&table_name.to_string()) {
        for colname in colnames {
            cis.push((columns.iter().position(|c| c.name.to_string() == colname.0).unwrap(), colname.1.clone()));
        } 
    } 
    cis
}

pub fn tablecolumn_matches_col(c: &views::TableColumnDef, col: &str) -> bool {
    debug!("matching {} or {} to {}", c.colname, c.fullname, col);
    c.colname == col || c.fullname == col
}

pub fn get_col_index(col: &str, columns: &Vec<views::TableColumnDef>) -> Option<usize> {
    columns.iter().position(|c| tablecolumn_matches_col(c, col))
}

