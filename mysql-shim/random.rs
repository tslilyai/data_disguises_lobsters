 /*fn substs_for_selection_expr(&mut self, user_cols: &Vec<String>, s: &Expr) -> Result<(Vec<SelectSubst>, String), mysql::Error>
    {
        let mut substs = Vec::<SelectSubst>::new();
        let mut selection_str_ghosts = format!("{:?}", s);

        // if the user id is used as part of the selection, we need to instead
        // use the corresponding ghost id
        // currently only match on `user_id = [x] AND ...` type selections
        // TODO actually check if string matches this pattern,
        // otherwise might end up with some funny results...
 
        // split selections into pairs of col_name, select_value
        let s_to_split = selection_str_ghosts.clone();
        let selections : Vec<&str> = s_to_split.split(" AND ").collect();
        for sel in selections {
            // TODO could support more operands?
            let pair : Vec<&str> = sel.split("=").collect();
            if pair.len() != 2 {
                unimplemented!("Not a supported selection pattern: {}", s);
            } else {
                let col = pair[0].replace(" ", "");
                let user_val = pair[1].replace(" ", "");
                if user_cols.iter().any(|c| *c == col) {
                    // check to see if there's a match of the userid with a ghostid 
                    // if this is the case, we can save this mapping so
                    // that we don't need to look up the ghost id later on
                    let get_ghost_record_q = format!(
                        "SELECT ghost_id FROM ghosts WHERE user_id = {}", 
                        user_val);
                    let mut matching_ghost_ids = Vec::<String>::new();
                    let rows = self.db.query_iter(get_ghost_record_q)?;
                    for r in rows {
                        let vals = r.unwrap().unwrap();
                        matching_ghost_ids.push(format!("{}", vals[0].as_sql(true /*no backslash escape*/)));
                    }
                   
                    // replace in the selection string
                    let ghost_ids_set_str = self.vals_to_sql_set(&matching_ghost_ids);
                    selection_str_ghosts = selection_str_ghosts
                        .replace(" = ", "=") // get rid of whitespace just in case
                        .replace(&format!("{}={}", col, user_val),
                            &format!("{} IN {}", col, ghost_ids_set_str));

                    // save the replacement mapping 
                    substs.push(SelectSubst{
                        user_col : col.to_string(), 
                        user_val : user_val.to_string(),
                        ghost_ids: matching_ghost_ids,
                    });
            
                    // we don't really care if there is no matching gid because no entry will match in underlying
                    // data table either
                    // NOTE potential optimization---ignore this query because it won't
                    // actually affect any data?
                    // TODO this can race with concurrent queries... keep ghost-to-user mapping in memory and protect with locks?
                }
            }
        }
        Ok((substs, format!("WHERE {};", selection_str_ghosts)))
    }
}*/


    fn vals_to_sql_set(&self, vals: &Vec<String>) -> String {
        let mut s = String::from("(");
        for (i, v) in vals.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(v);
        }
        s.push_str(")");
        s
    }
// not needed since we're just creating another table, not using real materialized views 
// (with joins, etc.)
/*fn gen_create_mv_query(
    table_name: &String, 
    user_id_columns: &Vec<String>,
    all_columns: &Vec<String>) -> String
{
    let mut user_id_col_str = String::new();
    let mut join_on_str = String::new();
    for (i, col) in user_id_columns.iter().enumerate() {
        // only replace user id with ghost id if its present in the appropriate user id column
        user_id_col_str.push_str(&format!(
                "COALESCE(ghosts.user_id, {}.{}) as {}",
                    table_name, col, col));
        join_on_str.push_str(&format!(
                "ghosts.ghost_id = {}.{}",
                    table_name, col));

        if i < user_id_columns.len()-1 {
            user_id_col_str.push_str(", ");
            join_on_str.push_str(" OR ");
        }
    }
    
    let mut data_col_str = String::new();
    for col in all_columns {
        if user_id_columns.contains(&col) {
            continue;
        }
        data_col_str.push_str(", ");
        data_col_str.push_str(&format!("{}.{}", table_name, col));
    }

    // XXX mysql doesn't support materialized views
    let query = format!(
    "CREATE TABLE {table_name}{suffix} AS (SELECT 
            {id_col_str} 
            {data_col_str}
        FROM {table_name} LEFT JOIN ghosts
        ON {join_on_str};",
        suffix = MV_SUFFIX,
        table_name = table_name,
        join_on_str = join_on_str,
        id_col_str = user_id_col_str,
        data_col_str = data_col_str);
    println!("Create data mv query: {}", query);
    return query;
}*/

