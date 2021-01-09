use std::collections::HashMap;
use sql_parser::ast::Statement;
use std::*;
use sql_parser::parser::*;

pub struct ParserCache {
    // caches
    cache: HashMap<String, Statement>,
}

impl ParserCache{
    pub fn new() -> Self {
        ParserCache {
            cache: HashMap::new(),
        }
    }   

    pub fn get_single_parsed_stmt(&mut self, stmt: &String) 
        -> Result<Statement, mysql::Error> 
    {
        match self.cache.get(stmt) {
            None => {
                let stmt_copy = stmt.clone();
                let asts = parse_statements(stmt.to_string());
                match asts {
                    Err(e) => Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::InvalidInput, e))),
                    Ok(asts) => {
                        if asts.len() != 1 {
                            return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::InvalidInput, "More than one stmt")));
                        }
                        self.cache.insert(stmt_copy, asts[0].clone());
                        Ok(asts[0].clone())
                    }
                }
            }
            Some(ast) => Ok(ast.clone())
        }
    }
}
