use crate::datagen;
use decor::{helpers, types};
use mysql::prelude::*;
use std::collections::HashMap;

// note: guises are violating ref integrity, just some arbitrary high value
pub fn get_decor_filters(tablefks: &Vec<types::TableFKs>) -> HashMap<String, Vec<String>> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
    let table_cols = datagen::get_schema_tables();
    for tablefk in tablefks {
        for table in &table_cols {
            let name = helpers::trim_quotes(&table.name);
            if name == tablefk.name {
                let mut normal_cols = vec![];
                let mut fk_cols = vec![];
                for col in &table.cols {
                    let col = helpers::trim_quotes(&col);
                    let mut found = false;
                    for fk in &tablefk.fks {
                        if &fk.referencer_col == col {
                            fk_cols.push(format!("0 as {}", col));
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        normal_cols.push(format!("{}.{} as {}", tablefk.name, col, col));
                    }
                }
                let filter = format!(
                    "SELECT {}, {} FROM {};",
                    normal_cols.join(", "),
                    fk_cols.join(", "),
                    tablefk.name
                );
                match filters.get_mut(&tablefk.name) {
                    Some(fs) => fs.push(filter),
                    None => {
                        filters.insert(tablefk.name.clone(), vec![filter]);
                    }
                }

                break;
            }
        }
    }
    filters
}

pub fn get_remove_where_fk_filters(
    fk_val: &str,
    tablefks: &Vec<types::TableFKs>,
) -> HashMap<String, Vec<String>> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
    for tablefk in tablefks {
        let mut fk_comps = vec![];
        for fk in &tablefk.fks {
            fk_comps.push(format!("{} != {}", fk.referencer_col, fk_val));
        }
        if fk_comps.is_empty() {
            fk_comps = vec!["TRUE".to_string()]
        }
        let filter = format!(
            "SELECT * FROM {} WHERE {};",
            tablefk.name,
            fk_comps.join(" AND "),
        );
        match filters.get_mut(&tablefk.name) {
            Some(fs) => fs.push(filter),
            None => {
                filters.insert(tablefk.name.clone(), vec![filter]);
            }
        }
    }
    filters
}

pub fn create_mv_from_filters(
    db: &mut mysql::Conn,
    filters: &HashMap<String, Vec<String>>,
) -> Result<(), mysql::Error> {
    for (table, filters) in filters.iter() {
        for f in filters {
            db.query_drop(format!("CREATE TEMPORARY TABLE {} AS {}", table, f))?;
        }
    }
    Ok(())
}
