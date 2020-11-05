use sql_parser::ast::*;
use super::{helpers, config, view};
use std::*;
use std::collections::HashMap;

pub struct MVTransformer {
    pub cfg: config::Config,
    views: HashMap<String, View>,
}

/********************************************************
 * Processing statements to use materialized views      
 * ******************************************************/
impl MVTransformer {
    pub fn new(cfg: &config::Config) -> Self {
        MVTransformer{
            cfg: cfg.clone(),
            q2mvq: HashMap::new(),
        }
    }   
}
