use crate::datagen;
use decor::{types, helpers};

// note: guises are violating ref integrity, just some arbitrary high value
pub fn get_decor_filters(tablefks: &Vec<types::TableFKs>) -> Vec<String> {
    let mut filters = vec![];
    let table_cols = datagen::get_schema_tables();
    for tablefk in tablefks {
        for table in &table_cols {
            let name = helpers::trim_quotes(&table.name);
            if name == tablefk.name {
                let mut normal_cols = vec![];
                let mut fk_cols = vec![];
                for col in &table.cols {
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
                filters.push(format!("SELECT {}, {} FROM {};", 
                    normal_cols.join(", "),
                    fk_cols.join(", "), 
                    tablefk.name));
                break;
            }
        }
    }
    filters
}

pub fn get_remove_filters(id_val: &str, tablefks: &Vec<types::TableFKs>) -> Vec<String> {
    let mut filters = vec![];
    for tablefk in tablefks {
        println!("Remove filters: Looking at tablefk {:?}", tablefk);
        let mut fk_comps = vec![];
        for fk in &tablefk.fks {
            fk_comps.push(format!("{} != {}", fk.referencer_col, id_val)); 
        }
        if fk_comps.is_empty() {
            fk_comps = vec!["TRUE".to_string()]
        }
        filters.push(format!("SELECT * FROM {} WHERE {};", 
            tablefk.name,
            fk_comps.join(" AND "),
        ));
    }
    filters
}
