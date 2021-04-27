use crate::datagen;
use decor::{helpers, types};
use sql_parser::ast::*;
use std::collections::HashMap;

pub fn merge_hashmaps(
    h1: &mut HashMap<String, Vec<String>>,
    h2: &mut HashMap<String, Vec<String>>,
) {
    for (k, vs1) in h1.iter_mut() {
        if let Some(mut vs2) = h2.get_mut(k) {
            vs1.append(&mut vs2);
        }
    }
    for (k, vs2) in h2.iter_mut() {
        if let Some(vs1) = h1.get_mut(k) {
            vs1.append(vs2);
        } else {
            h1.insert(k.to_string(), vs2.clone());
        }
    }
}


// note: guises are violating ref integrity, just some arbitrary high value
pub fn get_disguise_filters(
    user_id: Option<&str>,
    disguise: &types::Disguise,
) -> HashMap<String, Vec<String>> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();
   
    if let Some(uid) = user_id {
        get_remove_where_fk_filters(uid, &disguise.remove_names, &mut filters);
    } else {
        get_remove_filters(&disguise.remove_names, &mut filters);
    }

    get_update_filters(user_id, &disguise.update_names, &mut filters);
    filters
}

// note: guises are violating ref integrity, just some arbitrary high value
fn get_update_filters(
    user_id: Option<&str>,
    tableinfos: &Vec<types::TableInfo>,
    filters: &mut HashMap<String, Vec<String>>,
) {
    let table_cols = datagen::get_schema_tables();

    // for each table
    for tableinfo in tableinfos {
        let table_info = &table_cols
            .iter()
            .find(|&tc| helpers::trim_quotes(&tc.name) == tableinfo.name)
            .unwrap();

        let table = helpers::trim_quotes(&table_info.name);
        let cols: Vec<&str> = table_info
            .cols
            .iter()
            .map(|c| helpers::trim_quotes(c))
            .collect();

        let mut normal_cols = vec![];
        let mut modified_cols = vec![];
        let mut fk_cols = vec![];
        let mut where_fk = vec![];
        let mut where_not_fk = vec![];

        for (i, col) in cols.iter().enumerate() {
            if tableinfo.used_fks.iter().find(|fk| fk.referencer_col == *col).is_some() {
                fk_cols.push(format!("0 as `{}`", col));
                if let Some(v) = user_id {
                    where_fk.push(format!("`{}` = {}", col, v));
                    where_not_fk.push(format!("`{}` != {}", col, v));
                }
            } else if let Some(mc) = tableinfo.used_cols.iter().find(|mc| mc.col == *col) {
                match &table_info.colformats[i] {
                    types::ColFormat::NonQuoted => modified_cols.push(format!("{} as `{}`", (*mc.generate_modified_value)(), col)),
                    types::ColFormat::Quoted => modified_cols.push(format!("'{}' as `{}`", (*mc.generate_modified_value)(), col)),
                }
            } else {
                normal_cols.push(format!("`{}`.`{}` as `{}`", table, col, col));
            }
        }

        let filter: String;
        normal_cols.append(&mut modified_cols);
        normal_cols.append(&mut fk_cols);
        if !where_fk.is_empty() {
            // put all column selections together
            filter = format!(
                "SELECT {} FROM {} WHERE {} UNION SELECT * FROM {} WHERE {};",
                normal_cols.join(", "),
                table,
                where_fk.join(" OR "),
                table,
                where_not_fk.join(" AND "),
            );
        } else {
            filter = format!(
                "SELECT {} FROM {};",
                normal_cols.join(", "),
                table,
            );
        }
        match filters.get_mut(table) {
            Some(fs) => fs.push(filter),
            None => {
                filters.insert(table.to_string(), vec![filter]);
            }
        }
    }
}

fn get_remove_filters(
    tableinfos: &Vec<types::TableInfo>,
    filters: &mut HashMap<String, Vec<String>>,
) {
    for tableinfo in tableinfos {
        let filter = format!(
            "SELECT * FROM {} WHERE FALSE;",
            tableinfo.name
        );
        match filters.get_mut(&tableinfo.name) {
            Some(fs) => fs.push(filter),
            None => {
                filters.insert(tableinfo.name.clone(), vec![filter]);
            }
        }
    }
}

fn get_remove_where_fk_filters(
    user_id: &str,
    tableinfos: &Vec<types::TableInfo>,
    filters: &mut HashMap<String, Vec<String>>,
) {
    for tableinfo in tableinfos {
        let mut fk_comps = vec![];
        for fk in &tableinfo.used_fks {
            fk_comps.push(format!("{} != {}", fk.referencer_col, user_id));
        }
        if fk_comps.is_empty() {
            fk_comps = vec!["TRUE".to_string()]
        }
        let filter = format!(
            "SELECT * FROM {} WHERE {};",
            tableinfo.name,
            fk_comps.join(" AND "),
        );
        match filters.get_mut(&tableinfo.name) {
            Some(fs) => fs.push(filter),
            None => {
                filters.insert(tableinfo.name.clone(), vec![filter]);
            }
        }
    }
}

pub fn create_mv_from_filters_stmts(
    filters: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut results = vec![];
    for (table, filters) in filters.iter() {
        let mut parsed_fs: Vec<Statement> = filters
            .iter()
            .map(|f| helpers::get_single_parsed_stmt(&f).unwrap())
            .collect();

        // TODO sort filters

        let mut last_name: Option<String> = None;
        for (i, f) in parsed_fs.iter_mut().enumerate() {
            match f {
                Statement::Select(SelectStatement { query, .. }) => {
                    helpers::update_select_from(&mut query.body, &last_name);
                    last_name = Some(format!("{}{}", table, i));
                }
                _ => unimplemented!("Not a select projection filter?"),
            }
        }
        let total_filters = parsed_fs.len();
        for (i, f) in parsed_fs.iter_mut().enumerate() {
            let create_stmt: String;
            if i == total_filters-1 {
                // last created table replaces the name of the original base table!
                create_stmt = format!("CREATE TEMPORARY TABLE {} AS {}", table, f.to_string());
            } else {
                create_stmt = format!("CREATE VIEW {}{} AS {}", table, i, f.to_string());
            }
            results.push(create_stmt);
        }
    }
    results
}

pub fn check_spec_assertions(
    filters: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let mut results = vec![];
    for (table, filters) in filters.iter() {
        let mut parsed_fs: Vec<Statement> = filters
            .iter()
            .map(|f| helpers::get_single_parsed_stmt(&f).unwrap())
            .collect();

        // TODO sort filters

        let mut last_name: Option<String> = None;
        for (i, f) in parsed_fs.iter_mut().enumerate() {
            match f {
                Statement::Select(SelectStatement { query, .. }) => {
                    helpers::update_select_from(&mut query.body, &last_name);
                    last_name = Some(format!("{}{}", table, i));
                }
                _ => unimplemented!("Not a select projection filter?"),
            }
        }
        let total_filters = parsed_fs.len();
        for (i, f) in parsed_fs.iter_mut().enumerate() {
            let create_stmt: String;
            if i == total_filters-1 {
                // last created table replaces the name of the original base table!
                create_stmt = format!("CREATE TEMPORARY TABLE {} AS {}", table, f.to_string());
            } else {
                create_stmt = format!("CREATE VIEW {}{} AS {}", table, i, f.to_string());
            }
            results.push(create_stmt);
        }
    }
    results
}
