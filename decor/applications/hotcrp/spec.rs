use crate::datagen;
use decor::types;

// note: guises are violating ref integrity, just some arbitrary high value
pub fn get_decor_filters(tablefks: &Vec<types::TableFKs>) -> Vec<String> {
    let mut filters = vec![];
    let table_cols = datagen::get_schema_tables();
    for tablefk in tablefks {
        for table in &table_cols {
            if table.name == tablefk.name {
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
