use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use crate::{helpers, io, policy};
use rand;

pub type GuiseModifications = Vec<(TableCol, Box<dyn Fn(&str) -> String>)>;

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ID,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct TableCol {
    pub table: String,
    pub col_index: usize,
    pub col_name: String,
}

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyCol {
    pub referencer_table: String,
    pub referenced_table: String,
    pub col_index: usize,
    pub col_name: String,
}

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct ID {
    pub table: String,
    pub id: u64,
    pub id_col_name: String,
    pub id_col_index: usize,
}

impl ID {
    pub fn get_row(&self, db: &mut mysql::Conn) -> Result<Row, mysql::Error> {
        let res = db.query_iter(&format!("SELECT * FROM {} WHERE {}={} LIMIT 1", self.table, self.id_col_name, self.id))?;
        let cols = res.columns().as_ref()
                .iter()
                .map(|c| c.name_str().to_string())
                .collect();
        for row in res {
            let rowvals = row.unwrap().unwrap();
            let vals = rowvals.iter().map(|v| helpers::mysql_val_to_string(v)).collect();
            return Ok(Row {
                id: self.clone(),
                columns: cols,
                values: vals,
            })
        }
        Err(mysql::Error::IoError(io::Error::new(io::ErrorKind::NotFound, format!("ID {}.{} not found", self.table, self.id))))
    }

    pub fn update_row_with_modifications(&self, modifications: &GuiseModifications, db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        let row = self.get_row(db)?;
        let mut set_strs = vec![]; 
        for (tc, f) in modifications {
            set_strs.push(format!("{} = {}", 
                row.columns[tc.col_index], 
                f(&row.values[tc.col_index]))
            );
        }
        let set_str = set_strs.join(",");
        db.query_drop(&format!("UPDATE {} SET {} WHERE {}={}", 
                               self.table, set_str, self.id_col_name, self.id))
    }
    pub fn copy_row_with_modifications(&self, modifications: &GuiseModifications, db: &mut mysql::Conn) 
        -> Result<u64, mysql::Error> 
    {
        let mut row = self.get_row(db)?;
        for (tc, f) in modifications {
            row.values[tc.col_index] = f(&row.values[tc.col_index]);
        }
        // generate a random ID for now
        let newid = rand::random::<u32>() as u64;
        row.values[row.id.id_col_index] = newid.to_string();
        let values_str = row.values.join(",");
        db.query_drop(&format!("INSERT INTO {} VALUES ({})", self.table, values_str))?;
        Ok(newid)
    }
    pub fn get_referencers(&self, schema: &policy::SchemaConfig, db: &mut mysql::Conn) -> Result<Vec<(Row, ForeignKeyCol)>, mysql::Error> {
        let mut referencers = vec![];
        if let Some(tabinfo) = schema.table_info.get(&self.table) {
            let fkcols = &tabinfo.referencers;
            let id_col_info = &tabinfo.id_col_info;
            for fk in fkcols {
                let res = db.query_iter(&format!("SELECT * FROM {} WHERE {}={}", 
                        fk.referencer_table, fk.col_name, self.id))?;
                let cols = res.columns().as_ref()
                        .iter()
                        .map(|c| c.name_str().to_string())
                        .collect();
                for row in res {
                    let rowvals = row.unwrap().unwrap();
                    let vals = rowvals.iter().map(|v| helpers::mysql_val_to_string(v)).collect();
                    referencers.push((Row {
                        id: ID {
                            table: fk.referencer_table.clone(),
                            id: self.id, 
                            id_col_index: id_col_info.col_index,
                            id_col_name: id_col_info.col_name.clone()
                        },
                        columns: cols,
                        values: vals,
                    }, fk.clone()));
                }
            } 
        }
        Ok(referencers)
    }
}
