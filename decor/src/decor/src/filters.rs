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
    use types::Transform::*;
    let mut correct = true;

    // TODO don't use autoinc id_col
    for table_disguise in &disguise.table_disguises {
        warn!(
            "Checking disguise {}, table {}",
            disguise.disguise_id, table_disguise.name
        );
        for t in &table_disguise.transforms {
            match t {
                Decor {
                    pred,
                    referencer_col,
                    ..
                } => {
                    correct &= properly_decorrelated(
                        &pred,
                        &table_disguise.name,
                        &table_disguise.id_cols,
                        &referencer_col,
                        db,
                    )?;
                }
                Remove { pred, .. } => {
                    correct &=
                        properly_removed(&pred, &table_disguise.name, &table_disguise.id_cols, db)?;
                }
                Modify {
                    pred,
                    col,
                    satisfies_modification,
                    ..
                } => {
                    correct &= properly_modified(
                        &pred,
                        &table_disguise.name,
                        &table_disguise.id_cols,
                        &col,
                        satisfies_modification,
                        db,
                    )?;
                }
            }
        }
    }
    Ok(correct)
}

fn properly_removed(
    pred: &Option<Expr>,
    table_name: &str,
    id_cols: &Vec<String>,
    db: &mut mysql::Conn,
) -> Result<bool, mysql::Error> {
    let matching = helpers::get_query_rows_db(&helpers::select_statement(&table_name, pred), db)?;
    warn!("Found {} rows matching {:?}", matching.len(), pred);
    let tmp_name = format!("{}Temp", table_name);
    for row in &matching {
        let selection = helpers::get_select_of_row(id_cols, row);
        let tmp_rows = helpers::get_query_rows_db(
            &helpers::select_statement(&tmp_name, &Some(selection)),
            db,
        )?;
        if !tmp_rows.is_empty() {
            warn!("Row not properly removed {:?}", row);
            return Ok(false);
        }
    }
    Ok(true)
}

fn properly_decorrelated(
    pred: &Option<Expr>,
    table_name: &str,
    id_cols: &Vec<String>,
    referencer_col: &str,
    db: &mut mysql::Conn,
) -> Result<bool, mysql::Error> {
    let matching = helpers::get_query_rows_db(&helpers::select_statement(&table_name, pred), db)?;
    warn!("Found {} rows matching {:?}", matching.len(), pred);

    let tmp_name = format!("{}Temp", table_name);
    for row in &matching {
        let selection = helpers::get_select_of_row(id_cols, row);
        let tmp_rows = helpers::get_query_rows_db(
            &helpers::select_statement(&tmp_name, &Some(selection)),
            db,
        )?;
        assert!(tmp_rows.len() <= 1);
        if tmp_rows.is_empty() {
            // ok this row was removed, that's fine
            warn!(
                "Selection of row {:?} returns nothing, matching pred was {:?}",
                row, pred
            );
            continue;
        }

        // should have referential integrity!
        let value_tmp =
            u64::from_str(&helpers::get_value_of_col(&tmp_rows[0], referencer_col).unwrap())
                .unwrap();
        /*let value_orig =
            u64::from_str(&helpers::get_value_of_col(&row, referencer_col).unwrap()).unwrap();
        warn!(
            "Checking decorrelation for fk col {:?}, value_tmp {:?}, {:?}",
            referencer_col, value_tmp, value_orig
        );*/
        if value_tmp != GUISE_ID {
            warn!(
                "Improperly decorrelated tmp value {} != guise {}",
                value_tmp, GUISE_ID
            );
            return Ok(false);
        }
    }
    Ok(true)
}

fn properly_modified(
    pred: &Option<Expr>,
    table_name: &str,
    id_cols: &Vec<String>,
    col: &str,
    satisfies_modification: &Box<dyn Fn(&str) -> bool>,
    db: &mut mysql::Conn,
) -> Result<bool, mysql::Error> {
    let matching = helpers::get_query_rows_db(&helpers::select_statement(&table_name, pred), db)?;
    warn!("Found {} rows matching {:?}", matching.len(), pred);

    let tmp_name = format!("{}Temp", table_name);
    for row in &matching {
        let selection = helpers::get_select_of_row(id_cols, row);
        let tmp_rows = helpers::get_query_rows_db(
            &helpers::select_statement(&tmp_name, &Some(selection)),
            db,
        )?;
        assert!(tmp_rows.len() <= 1);
        if tmp_rows.is_empty() {
            // ok this row was removed, that's fine
            warn!(
                "Selection of row {:?} returns nothing, matching pred was {:?}",
                row, pred
            );
            continue;
        }

        let value_tmp = helpers::get_value_of_col(&tmp_rows[0], col).unwrap();
        /*warn!(
            "Checking modification for fk col {:?}, value {:?}",
            col, value_tmp
        );*/
        if !(*satisfies_modification)(&value_tmp) {
            let value_orig = helpers::get_value_of_col(&row, &col).unwrap();
            warn!(
                "Improperly modified check tmp value {:?},  original value {:?}",
                value_tmp, value_orig
            );
            return Ok(false);
        }
    }
    Ok(true)
}

// note: guises are violating ref integrity, just some arbitrary 0 value for now
pub fn get_disguise_filters(
    table_cols: &Vec<types::TableColumns>,
    disguise: &types::Disguise,
) -> HashMap<String, Vec<(types::TransformType, Statement)>> {
    let mut filters: HashMap<String, Vec<(types::TransformType, Statement)>> = HashMap::new();

    get_remove_filters(&disguise.table_disguises, &mut filters);

    get_update_filters(table_cols, &disguise.table_disguises, &mut filters);
    filters
}

// note: guises are violating ref integrity, just some arbitrary high value
fn get_update_filters(
    table_cols: &Vec<types::TableColumns>,
    table_disguises: &Vec<types::TableDisguise>,
    filters: &mut HashMap<String, Vec<(types::TransformType, Statement)>>,
) {
    // for each table
    for table_disguise in table_disguises {
        let table_info = &table_cols
            .iter()
            .find(|&tc| helpers::trim_quotes(&tc.name) == table_disguise.name)
            .unwrap();

        let table = helpers::trim_quotes(&table_info.name);
        let cols: Vec<&str> = table_info
            .cols
            .iter()
            .map(|c| helpers::trim_quotes(c))
            .collect();

        // for each transformation (note there can be multiple pred-trans pairs for each col)
        for t in &table_disguise.transforms {
            let mut normal_cols = vec![];
            let mut modified_cols = vec![];
            let mut fk_cols = vec![];
            let mut where_pred = vec![];
            let mut transform_type = types::TransformType::Decor;

            for (i, c) in cols.iter().enumerate() {
                let mut found = false;
                match t {
                    types::Transform::Decor {
                        pred,
                        referencer_col,
                        ..
                    } => {
                        if referencer_col == *c {
                            fk_cols.push(format!("{} as `{}`", GUISE_ID, c));
                            if let Some(p) = pred {
                                where_pred.push(p.to_string());
                            }
                            found = true;
                        }
                    }
                    types::Transform::Modify {
                        pred,
                        col,
                        generate_modified_value,
                        ..
                    } => {
                        if col == *c {
                            match &table_info.colformats[i] {
                                types::ColFormat::NonQuoted => modified_cols.push(format!(
                                    "{} as `{}`",
                                    (*generate_modified_value)("old value"), // XXX
                                    c
                                )),
                                types::ColFormat::Quoted => modified_cols.push(format!(
                                    "'{}' as `{}`",
                                    (*generate_modified_value)("old value"), // XXX
                                    c
                                )),
                            }
                            if let Some(p) = pred {
                                where_pred.push(p.to_string());
                            }
                            found = true;
                            transform_type = types::TransformType::Modify;
                        }
                    }
                    _ => (),
                }
                if !found {
                    normal_cols.push(format!("`{}` as `{}`", c, c));
                }
            }

            let filter: String;
            normal_cols.append(&mut modified_cols);
            normal_cols.append(&mut fk_cols);
            if !where_pred.is_empty() {
                let preds = where_pred.join(" OR ");
                // put all column selections together
                filter = format!(
                    "SELECT {} FROM {} WHERE {} UNION SELECT * FROM {} WHERE NOT ({});",
                    normal_cols.join(", "),
                    table,
                    preds,
                    table,
                    preds,
                );
            } else {
                filter = format!("SELECT {} FROM {};", normal_cols.join(", "), table,);
            }
            match filters.get_mut(table) {
                Some(fs) => fs.push((
                    transform_type,
                    helpers::get_single_parsed_stmt(&filter).unwrap(),
                )),
                None => {
                    filters.insert(
                        table.to_string(),
                        vec![(
                            transform_type,
                            helpers::get_single_parsed_stmt(&filter).unwrap(),
                        )],
                    );
                }
            }
        }
    }
}

fn get_remove_filters(
    table_disguises: &Vec<types::TableDisguise>,
    filters: &mut HashMap<String, Vec<(types::TransformType, Statement)>>,
) {
    for table_disguise in table_disguises {
        for transform in &table_disguise.transforms {
            match transform {
                types::Transform::Remove { pred } => {
                    let pred = match pred {
                        None => "false".to_string(),
                        Some(s) => s.to_string(),
                    };
                    let filter = format!(
                        "SELECT * FROM {} WHERE NOT ({});",
                        table_disguise.name, pred
                    );
                    match filters.get_mut(&table_disguise.name) {
                        Some(fs) => fs.push((
                            types::TransformType::Remove,
                            helpers::get_single_parsed_stmt(&filter).unwrap(),
                        )),
                        None => {
                            filters.insert(
                                table_disguise.name.clone(),
                                vec![(
                                    types::TransformType::Remove,
                                    helpers::get_single_parsed_stmt(&filter).unwrap(),
                                )],
                            );
                        }
                    }
                }
                _ => (),
            }
        }
    }
}

/*pub fn create_mv_from_filters_stmts(
    all: &mut HashMap<String, Vec<(types::TransformType, Statement)>>,
) -> Vec<String> {
    let mut results = vec![];
    for (table, filters) in all.iter_mut() {
        let mut prior_updated: Vec<HashSet<String>> = HashSet::new();
        let mut prior_pred: Vec<HashSet<String>> = HashSet::new();

        // TODO sort filters
        for f in filters.iter() {
            match &f.1 {
                Statement::Select(SelectStatement { query, .. }) => {
                    let updated_cols = helpers::get_updated_cols(&query.body);
                    let pred_cols = helpers::get_conditional_cols(&query.body);
                    let rar: HashSet<_> = prior_pred.intersection(&pred_cols).collect();
                    let raw: HashSet<_> = prior_pred.intersection(&pred_cols).collect();
                    let war: HashSet<_> = prior_pred.intersection(&pred_cols).collect();
                    if !rar.is_empty() {
                        // predicated on the same thing as prior filters
                    }
                    if !raw.is_empty() {
                        // read-after-write
                    }
                    if !war.is_empty() {
                        // write-after-read
                    }
                }
                _ => unimplemented!("Not a select projection filter?"),
            }
        }

        let mut last_name: Option<String> = None;
        for (i, f) in filters.iter_mut().enumerate() {
            match &mut f.1 {
                Statement::Select(SelectStatement { query, .. }) => {
                    helpers::update_select_from(&mut query.body, &last_name);
                    last_name = Some(format!("{}{}", table, i));
                }
                _ => unimplemented!("Not a select projection filter?"),
            }
        }
        let total_filters = filters.len();
        for (i, f) in filters.iter().enumerate() {
            let create_stmt: String;
            if i == total_filters - 1 {
                // last created table replaces the name of the original base table!
                create_stmt = format!(
                    "CREATE TEMPORARY TABLE {}Temp AS {}",
                    table,
                    f.1.to_string()
                );
            } else {
                create_stmt = format!("CREATE VIEW {}{} AS {}", table, i, f.1.to_string());
            }
            results.push(create_stmt);
        }
    }
    results
}*/
