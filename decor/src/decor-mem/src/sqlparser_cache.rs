use std::collections::HashMap;
use sql_parser::ast::Statement;
use std::*;
use sql_parser::parser::*;
use log::warn;

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
        let asts = parse_statements(stmt.to_string());
        match asts {
            Err(e) => Err(mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::InvalidInput, e))),
            Ok(asts) => {
                if asts.len() != 1 {
                    return Err(mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::InvalidInput, "More than one stmt")));
                }
                Ok(asts[0].clone())
            }
        }
         /*match self.cache.get(stmt) {
            None => {
                let start = time::Instant::now();
                let asts = parse_statements(stmt.to_string());
                let dur = start.elapsed();
                warn!("sqlparsing {} took {}us", stmt, dur.as_micros());
                match asts {
                    Err(e) => Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::InvalidInput, e))),
                    Ok(asts) => {
                        if asts.len() != 1 {
                            return Err(mysql::Error::IoError(io::Error::new(
                                io::ErrorKind::InvalidInput, "More than one stmt")));
                        }
                        let start = time::Instant::now();
                        self.cache.insert(stmt.clone(), asts[0].clone());
                        let dur = start.elapsed();
                        warn!("sqlparsing insert {} took {}us", stmt, dur.as_micros());
                        Ok(asts[0].clone())
                    }
                }
            }
            Some(ast) => Ok(ast.clone())
        }*/
    }
}
