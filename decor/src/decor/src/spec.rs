use crate::{helpers, types};
use log::warn;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::str::FromStr;

const GUISE_ID: u64 = 0;

pub fn check_disguise_properties(
    disguise: &types::Disguise,
    db: &mut mysql::Conn,
) -> Result<bool, mysql::Error> {
    let mut correct = true;

    for tableinfo in &disguise.remove_names {
        let tmp_name = format!("{}Temp", tableinfo.name);
        let select_temp = helpers::get_select(disguise.user_id, &tableinfo, disguise);
        correct &=
            helpers::get_query_rows_db(&helpers::select_1_statement(&tmp_name, select_temp), db)?
                .is_empty();
    }
    for tableinfo in &disguise.update_names {
        // what rows matched this initially?
        let select_phys = helpers::get_select(disguise.user_id, &tableinfo, disguise);
        let matching = helpers::get_query_rows_db(
            &helpers::select_statement(&tableinfo.name, select_phys),
            db,
        )?;

        correct &= properly_modified(&matching, &tableinfo, db)?;
        correct &= properly_decorrelated(&matching, &tableinfo, disguise, db)?;
    }

    Ok(correct)
}

fn properly_decorrelated(
    matching: &Vec<Vec<types::RowVal>>,
    tableinfo: &types::TableInfo,
    disguise: &types::Disguise,
    db: &mut mysql::Conn,
) -> Result<bool, mysql::Error> {
    let tmp_name = format!("{}Temp", tableinfo.name);
    for row in matching {
        let selection = helpers::get_select_of_row(tableinfo, row);
        let tmp_rows =
            helpers::get_query_rows_db(&helpers::select_statement(&tmp_name, Some(selection)), db)?;
        assert!(tmp_rows.len() <= 1);
        if tmp_rows.is_empty() {
            // ok this row was removed, that's fine
            warn!("Selection of row {:?} returns nothing", row);
            continue;
        }

        for fk in &tableinfo.fks_to_decor {
            // should have referential integrity!
            let value_tmp = u64::from_str(&helpers::get_value_of_col(&tmp_rows[0], &fk.referencer_col).unwrap()).unwrap();
            let value_orig = u64::from_str(&helpers::get_value_of_col(&row, &fk.referencer_col).unwrap()).unwrap();
            warn!(
                "Checking decorrelation for fk col {:?}, value_tmp {:?}, value_orig {:?}",
                fk.referencer_col, value_tmp, value_orig
            );
            match disguise.user_id {
                // return false if FK still points to user
                Some(uid) => {
                    if value_tmp == uid {
                        warn!("Decor check tmp value {:?} == uid {}", value_tmp, uid);
                        return Ok(false);
                    } else if value_tmp != value_orig && value_tmp != GUISE_ID {
                        warn!("Decor check tmp value {} != original value {} or guise {}", value_tmp, value_orig, GUISE_ID);
                        return Ok(false);
                    }
                }
                // return false if FK still points to any user
                None => {
                    if value_tmp != GUISE_ID {
                        warn!("Decor check tmp value {:?} != guise {}", value_tmp, GUISE_ID);
                        return Ok(false);
                    }
                }
            }
        }
    }
    Ok(true)
}

fn properly_modified(
    matching: &Vec<Vec<types::RowVal>>,
    tableinfo: &types::TableInfo,
    db: &mut mysql::Conn,
) -> Result<bool, mysql::Error> {
    let tmp_name = format!("{}Temp", tableinfo.name);
    for row in matching {
        let selection = helpers::get_select_of_row(tableinfo, row);
        let tmp_rows =
            helpers::get_query_rows_db(&helpers::select_statement(&tmp_name, Some(selection)), db)?;
        assert!(tmp_rows.len() <= 1);
        if tmp_rows.is_empty() {
            // ok this row was removed, that's fine
            warn!("Selection of row {:?} returns nothing", row);
            continue;
        }

        for colmod in &tableinfo.cols_to_update {
            let value_tmp = helpers::get_value_of_col(&tmp_rows[0], &colmod.col).unwrap();
            warn!(
                "Checking modification for fk col {:?}, value {:?}",
                colmod.col, value_tmp
            );
            if !(*colmod.satisfies_modification)(&value_tmp) {
                let value_orig = helpers::get_value_of_col(&row, &colmod.col).unwrap();
                warn!("Modified check tmp value {:?},  original value {:?}", value_tmp, value_orig);
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// note: guises are violating ref integrity, just some arbitrary 0 value for now
pub fn get_disguise_filters(
    table_cols: &Vec<types::TableColumns>,
    disguise: &types::Disguise,
) -> HashMap<String, Vec<String>> {
    let mut filters: HashMap<String, Vec<String>> = HashMap::new();

    if let Some(uid) = disguise.user_id {
        get_remove_where_fk_filters(uid, &disguise.remove_names, &mut filters);
    } else {
        get_remove_filters(&disguise.remove_names, &mut filters);
    }

    get_update_filters(
        table_cols,
        disguise.user_id,
        &disguise.update_names,
        &mut filters,
    );
    filters
}

// note: guises are violating ref integrity, just some arbitrary high value
fn get_update_filters(
    table_cols: &Vec<types::TableColumns>,
    user_id: Option<u64>,
    tableinfos: &Vec<types::TableInfo>,
    filters: &mut HashMap<String, Vec<String>>,
) {
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
            if tableinfo
                .fks_to_decor
                .iter()
                .find(|fk| fk.referencer_col == *col)
                .is_some()
            {
                fk_cols.push(format!("{} as `{}`", GUISE_ID, col));
                if let Some(v) = user_id {
                    where_fk.push(format!("`{}` = {}", col, v));
                    where_not_fk.push(format!("`{}` != {}", col, v));
                }
            } else if let Some(mc) = tableinfo.cols_to_update.iter().find(|mc| mc.col == *col) {
                match &table_info.colformats[i] {
                    types::ColFormat::NonQuoted => modified_cols.push(format!(
                        "{} as `{}`",
                        (*mc.generate_modified_value)(),
                        col
                    )),
                    types::ColFormat::Quoted => modified_cols.push(format!(
                        "'{}' as `{}`",
                        (*mc.generate_modified_value)(),
                        col
                    )),
                }
            } else {
                normal_cols.push(format!("`{}` as `{}`", col, col));
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
            filter = format!("SELECT {} FROM {};", normal_cols.join(", "), table,);
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
        let filter = format!("SELECT * FROM {} WHERE FALSE;", tableinfo.name);
        match filters.get_mut(&tableinfo.name) {
            Some(fs) => fs.push(filter),
            None => {
                filters.insert(tableinfo.name.clone(), vec![filter]);
            }
        }
    }
}

fn get_remove_where_fk_filters(
    user_id: u64,
    tableinfos: &Vec<types::TableInfo>,
    filters: &mut HashMap<String, Vec<String>>,
) {
    for tableinfo in tableinfos {
        let mut fk_comps = vec![];
        for fk in &tableinfo.fks_to_decor {
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

pub fn create_mv_from_filters_stmts(filters: &HashMap<String, Vec<String>>) -> Vec<String> {
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
            if i == total_filters - 1 {
                // last created table replaces the name of the original base table!
                create_stmt = format!("CREATE TEMPORARY TABLE {}Temp AS {}", table, f.to_string());
            } else {
                create_stmt = format!("CREATE VIEW {}{} AS {}", table, i, f.to_string());
            }
            results.push(create_stmt);
        }
    }
    results
}
