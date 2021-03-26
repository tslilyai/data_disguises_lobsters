use crate::{helpers, io, policy};
use mysql::prelude::*;
use rand;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

pub type GuiseModifications = Vec<(TableCol, Box<dyn Fn(&str) -> String>)>;

pub type TableName = String;

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct TableCol {
    pub table: TableName,
    pub col_index: usize,
    pub col_name: String,
}

#[derive(Serialize, Deserialize, PartialOrd, Ord, Debug, Clone)]
pub struct TableNamePair {
    pub type1: TableName,
    pub type2: TableName,
}
impl Hash for TableNamePair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.type1 < self.type2 {
            self.type1.hash(state);
            self.type2.hash(state);
        } else {
            self.type2.hash(state);
            self.type1.hash(state);
        }
    }
}
impl PartialEq for TableNamePair {
    fn eq(&self, other: &TableNamePair) -> bool {
        (self.type2 == other.type2 && self.type1 == other.type1)
            || (self.type1 == other.type2 && self.type2 == other.type1)
    }
}
impl Eq for TableNamePair {}
impl TableNamePair {
    pub fn get_node_to_modify(&self) -> TableName {
        self.type2.clone()
    }
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

#[derive(Hash, Serialize, Deserialize, PartialOrd, Ord, Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ID,
    pub columns: Vec<TableCol>,
    pub values: Vec<String>,
}

impl ID {
    pub fn get_row(&self, db: &mut mysql::Conn) -> Result<Row, mysql::Error> {
        let q = &format!(
            "SELECT * FROM {} WHERE {}={} LIMIT 1",
            self.table, self.id_col_name, self.id
        );
        let rows = helpers::get_rows_of_query(q, &self.table, &self.id_col_name, db)?;
        if rows.len() != 1 {
            Err(mysql::Error::IoError(io::Error::new(
                io::ErrorKind::NotFound,
                format!("ID {}.{} not found", self.table, self.id),
            )))
        } else {
            Ok(rows[0].clone())
        }
    }

    pub fn update_row_with_modifications(
        &self,
        modifications: &GuiseModifications,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        let row = self.get_row(db)?;
        let mut set_strs = vec![];
        for (tc, f) in modifications {
            set_strs.push(format!(
                "{} = {}",
                row.columns[tc.col_index].col_name,
                f(&row.values[tc.col_index])
            ));
        }
        let set_str = set_strs.join(",");
        db.query_drop(&format!(
            "UPDATE {} SET {} WHERE {}={}",
            self.table, set_str, self.id_col_name, self.id
        ))
    }

    pub fn copy_row_with_modifications(
        &self,
        modifications: &GuiseModifications,
        db: &mut mysql::Conn,
    ) -> Result<u64, mysql::Error> {
        let mut row = self.get_row(db)?;
        for (tc, f) in modifications {
            row.values[tc.col_index] = f(&row.values[tc.col_index]);
        }
        // generate a random ID for now
        let newid = rand::random::<u32>() as u64;
        row.values[row.id.id_col_index] = newid.to_string();
        let values_str = row.values.join(",");
        db.query_drop(&format!(
            "INSERT INTO {} VALUES ({})",
            self.table, values_str
        ))?;
        Ok(newid)
    }

    pub fn get_referencers(
        &self,
        schema: &policy::SchemaConfig,
        db: &mut mysql::Conn,
    ) -> Result<Vec<(Vec<Row>, ForeignKeyCol)>, mysql::Error> {
        let mut referencers = vec![];
        if let Some(tabinfo) = schema.table_info.get(&self.table) {
            let fkcols = &tabinfo.referencers;
            for fk in fkcols {
                let q = &format!(
                    "SELECT * FROM {} WHERE {}={}",
                    fk.referencer_table, fk.col_name, self.id
                );
                let id_col_info = &schema.table_info.get(&fk.referencer_table).unwrap().id_col_info;
                referencers.push((helpers::get_rows_of_query(q, &fk.referencer_table, &id_col_info.col_name, db)?, fk.clone()));
            }
        }
        Ok(referencers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let hm = HashMap::new();
        hm.insert(
            TableNamePair {
                type1: "hello",
                type2: "world",
            },
            "first",
        );
        assert_eq!(
            hm.get(&TableNamePair {
                type1: "hello",
                type2: "world",
            }),
            Some("first")
        );
        assert_eq!(
            hm.get(&TableNamePair {
                type1: "world",
                type2: "hello",
            }),
            Some("first")
        );
        hm.insert(
            TableNamePair {
                type1: "world",
                type2: "hello",
            },
            "second",
        );
        assert_eq!(
            hm.get(&TableNamePair {
                type1: "hello",
                type2: "world",
            }),
            Some("second")
        );

        assert_eq!(hm.len(), 1);
    }
}
