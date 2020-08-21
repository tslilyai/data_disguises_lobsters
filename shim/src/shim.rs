extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use regex::Regex;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::*;
use std::collections::HashMap;
use std::*;
mod config;

pub const SCHEMA : &'static str = include_str!("./schema.sql");
const DIALECT : sqlparser::dialect::MySqlDialect = MySqlDialect{};
const MV_SUFFIX : &'static str = "_mv"; 

fn gen_create_ghost_metadata_query() -> String {
    return r"CREATE TABLE IF NOT EXISTS `ghost_metadata` (
                `ghost_id` int unsigned NOT NULL AUTO_INCREMENT,
                `user_id` int unsigned,
                );".to_string();
}

fn gen_create_mv_query(
    table_name: &String, 
    user_id_columns: &Vec<String>,
    all_columns: &Vec<String>) -> String
{
    let mut user_id_col_str = String::new();
    let mut join_on_str = String::new();
    for (i, col) in user_id_columns.iter().enumerate() {
        // only replace user id with ghost id if its present in the appropriate user id column
        user_id_col_str.push_str(&format!(
                "COALESCE(ghost_metadata.user_id, {}.{}) as {}",
                    table_name, col, col));
        join_on_str.push_str(&format!(
                "ghost_metadata.ghost_id = {}.{}",
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

    // XXX mysql doesn't support materialized views? only views?
    // add GROUP BY to ensure that we only get one row per data table record
    // this is necessary in cases where a data record has more than 1 user_id_col
    let query = format!(
    "CREATE TABLE {table_name}{suffix} AS (SELECT 
            {id_col_str} 
            {data_col_str}
        FROM {table_name} LEFT JOIN ghost_metadata
        ON {join_on_str};",
        suffix = MV_SUFFIX,
        table_name = table_name,
        join_on_str = join_on_str,
        id_col_str = user_id_col_str,
        data_col_str = data_col_str);
    println!("Create data mv query: {}", query);
    return query;
}

struct Prepared {
    stmt: mysql::Statement,
    params: Vec<Column>,
}

struct SelectSubst {
    user_col: String,
    user_val: String,
    ghost_ids: Vec<String>,
}

pub struct Shim { 
    db: mysql::Conn,
    cfg: config::Config,
    table_names: Vec<String>,
    // NOTE: not *actually* static, but tied to our connection's lifetime.
    prepared: HashMap<u32, Prepared>,
}

impl Shim {
    pub fn new(db: mysql::Conn, cfg_json: &str) -> Self {
        let cfg = config::parse_config(cfg_json).unwrap();
        let mut table_names = Vec::<String>::new();
        table_names.push(cfg.user_table.name.clone());
        for dt in &cfg.data_tables{
            table_names.push(dt.name.clone());
        }
        let prepared = HashMap::new();
        Shim{db, cfg, table_names, prepared}
    }   

    /* 
     * Set all user_ids in the ghost_metadata table to NULL
     * refresh "materialized views"
     */
    pub fn unsubscribe() -> bool {
        false
    }

    /* 
     * Set all user_ids in the ghost_metadata table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     */
    pub fn resubscribe() -> bool {
        false
    }

    fn get_user_cols_of(&self, table_name: String) -> Option<&Vec<String>> {
         for dt in &self.cfg.data_tables {
             if table_name == dt.name {
                 return Some(&dt.user_cols);
             }
         }
         None
    }
    
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

    fn query_using_mv_tables(&self, query: &str) -> String {
        let mut changed_q = query.to_string();
        let mut new_name : String; 
        for table_name in &self.table_names {
            new_name = table_name.clone();
            new_name.push_str(MV_SUFFIX);
            changed_q = changed_q.replace(table_name, &new_name);
        }
        changed_q
    }

    fn substs_for_selection_expr(&mut self, user_cols: &Vec<String>, s: &Expr) -> Result<(Vec<SelectSubst>, String), mysql::Error>
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
                        "SELECT ghost_id FROM ghost_metadata WHERE user_id = {}", 
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
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.prepared.clear();
        // drop the connection (implicitly done).
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = mysql::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        // TODO save prepared stmts modified for MVs and ghost_metadata table
        match self.db.prep(self.query_using_mv_tables(query)) {
            Ok(stmt) => {
                let params: Vec<_> = stmt
                    .params()
                    .into_iter()
                    .map(|p| {
                        Column {
                            table: p.table_str().to_string(),
                            column: p.name_str().to_string(),
                            coltype: get_coltype(&p.column_type()),
                            colflags: ColumnFlags::from_bits(p.flags().bits()).unwrap(),
                        }
                    })
                    .collect();
                let columns: Vec<_> = stmt
                    .columns()
                    .into_iter()
                    .map(|c| {
                        Column {
                            table: c.table_str().to_string(),
                            column: c.name_str().to_string(),
                            coltype: get_coltype(&c.column_type()),
                            colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                        }
                    })
                    .collect();
                info.reply(stmt.id(), &params, &columns)?;
                self.prepared.insert(stmt.id(), Prepared{stmt: stmt.clone(), params});
            },
            Err(e) => {
                match e {
                    mysql::Error::MySqlError(merr) => {
                        info.error(ErrorKind::ER_NO, merr.message.as_bytes())?;
                    },
                    _ => return Err(e),
                }
            }
        }
        Ok(())
    }
    
    fn on_execute(
        &mut self,
        id: u32,
        ps: ParamParser,
        results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        match self.prepared.get(&id) {
            None => return Ok(results.error(ErrorKind::ER_NO, b"no such prepared statement")?),
            Some(prepped) => {
                // parse params
                let args : Vec<mysql::Value> = ps
                    .into_iter()
                    .map(|p| match p.value.into_inner() {
                        msql_srv::ValueInner::NULL => {
                            mysql::Value::NULL
                        }
                        ValueInner::Bytes(bs) => {
                            mysql::Value::Bytes(bs.to_vec())
                        }
                        ValueInner::Int(v) => {
                            mysql::Value::Int(v)
                        }
                        ValueInner::UInt(v) => {
                            mysql::Value::UInt(v)
                        }
                        ValueInner::Double(v) => {
                            mysql::Value::Float(v)
                        }
                        ValueInner::Date(bs) => {
                            assert!(bs.len() == 7);
                            mysql::Value::Date(bs[0].into(), bs[1].into(), bs[2], bs[3], bs[4], bs[5], bs[6].into())
                        }
                        ValueInner::Time(bs) => {
                            assert!(bs.len() == 6);
                            mysql::Value::Time(bs[0] == 0, bs[1].into(), bs[2], bs[3], bs[4], bs[5].into())
                        }
                        ct => unimplemented!("no translation for param type {:?}", ct)
                    }).collect();

                let res = self.db.exec_iter(
                    prepped.stmt.clone(), 
                    mysql::params::Params::Positional(args),
                );

                // TODO get response
                return Ok(());
                //answer_rows(results, self.db.query_iter(self.query_using_mv_tables("")))
            }
        }
    }
    
    fn on_close(&mut self, id: u32) {
        match self.prepared.get(&id) {
            None => return,
            Some(prepped) => {
                // XXX todo should handle error
                // why doesn't on_close return a result?
                if let Err(e) = self.db.close(prepped.stmt.clone()){
                    eprintln!("{}", e);
                };
                self.prepared.remove(&id); 
            }
        }
    }

    fn on_init(&mut self, schema: &str, w: InitWriter<W>) -> Result<(), Self::Error> {
        println!("On init called!");
        let res = self.db.select_db(schema);
        if !res {
            w.error(ErrorKind::ER_BAD_DB_ERROR, b"select db failed")?;
            return Ok(());
        }   

        /* Set up schema */
        let mut current_q = String::new();
        for line in SCHEMA.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !current_q.is_empty() {
                current_q.push_str(" ");
            }
            current_q.push_str(line);
            if current_q.ends_with(';') {
                self.db.query_drop(&current_q).unwrap();
                println!("Query executed: {}", current_q);
                current_q.clear();
            }
        }

        /* create ghost metadata table with boolean cols for each user id */
        let create_ghost_table_q = gen_create_ghost_metadata_query();
        // XXX temp: create a new ghost metadata table
        self.db.query_drop("DROP TABLE IF EXISTS ghost_metadata;").unwrap();
        self.db.query_drop(create_ghost_table_q).unwrap();
        
        /* create materialized view for all data tables */
        let mut sql = String::new();
        let mut mv_query : String;
        for line in SCHEMA.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !sql.is_empty() {
                sql.push_str(" ");
            }
            sql.push_str(line);
            if sql.ends_with(';') {
                sql.push_str("\n");
            }
        }

        // XXX hack because parser doesn't support these: note that we just need the table and col names
        // seems like mysql specific parsing hasn't yet been merge in official crate
        // - remove backticks
        // - remove autoincrement options
        // - remove unsigned options
        // - remove tinyint(1) 
        let re = Regex::new(r"`|(?i)AUTO_INCREMENT|unsigned|tiny|\(1\)")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        sql = re.replace_all(&sql, "").to_string();
        
        // sqlparser also doesn't support indexes in create table stmts for mysql
        let re_end = Regex::new(", (fulltext|UNIQUE)? INDEX .*")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        sql = re_end.replace_all(&sql, ");").to_string();

        let stmts = Parser::parse_sql(&DIALECT, &sql).unwrap();

        for stmt in stmts {
            match stmt {
                Statement::CreateTable {
                    name,
                    columns,
                    ..
                } => {
                    // construct query to create MV 
                    let mut col_names = Vec::<String>::new();
                    for col in columns {
                        col_names.push(col.name.to_string());
                    }
                    if name.to_string() == self.cfg.user_table.name {
                        mv_query = gen_create_mv_query(
                            &self.cfg.user_table.name, 
                            &vec![self.cfg.user_table.id_col.clone()], 
                            &col_names);
                    } else {
                        let dtopt = self.cfg.data_tables.iter().find(|&dt| dt.name == name.to_string());
                        match dtopt {
                            Some(dt) => {
                                mv_query = gen_create_mv_query(
                                    &dt.name,
                                    &dt.user_cols,
                                    &col_names)
                            },
                            _ => continue,
                        }                     
                    }
                    // execute query
                    self.db.query_drop(mv_query).unwrap();
                },
                _ => continue, // we only handle create table stmts
            }
        }
        println!("done with init!");
        w.ok()?;
        Ok(())
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let stmts_res = Parser::parse_sql(&DIALECT, &query);
        let mut data_table_query = String::from(query);
        let mv_query = self.query_using_mv_tables(query);

        match stmts_res {
            Err(e) => {
                results.error(ErrorKind::ER_PARSE_ERROR, format!("{:?}", e).as_bytes())?;
                return Ok(());
            }
            Ok(stmts) => {
                for stmt in stmts {
                    match stmt {
                        Statement::Insert {
                            table_name,
                            columns,
                            source,
                        } => {
                            let value_exprs : &[Vec<Expr>];
                            match &source.body {
                                SetExpr::Values(Values(values)) => value_exprs = values.as_slice(),
                                // TODO currently not supporting nested queries (insert into _ with _ as select _)
                                // we could probably support this by issuing the select statement
                                // and getting values from the MVs, or by substituting ghost values
                                // into the select statement
                                _=> unimplemented!("No support for nested queries in VALUES"),
                            }
                            // we want to insert into both the MV and the data table
                            // and to insert a unique ghost_id in place of the user_id 
                            // 1. check if this table even has user_ids that we need to replace
                            let mut user_cols : Vec<String> = Vec::<String>::new();
                            match self.get_user_cols_of(table_name.to_string()) {
                                Some(uc) => user_cols = uc.clone(),
                                None => (),
                            }
                            for (i, c) in columns.iter().enumerate() {
                                if user_cols.iter().any(|col| *col == c.value) {
                                    // 2. get param value of user_id columns
                                    // 3. insert user_id val into the ghost_metadata table
                                    //    as a new ghost_id entry
                                    let user_id_q = format!("{:?}", value_exprs[i]);
                                    self.db.query_drop(format!("INSERT INTO ghost_metadata (user_id) VALUES ({});", 
                                                               self.query_using_mv_tables(&user_id_q)))?;
                                    // 4. get the ghost_id field of the new entry 
                                    let res = self.db.query_iter("SELECT LAST_INSERT_ID()")?;
                                    match res.last_insert_id() {
                                        None => return Ok(results.error(
                                                ErrorKind::ER_INTERNAL_ERROR, 
                                                b"call to last insert ID failed")?),
                                        Some(ghost_id) => {
                                            // 5. replace user_id value in query with ghost_id
                                            // XXX this seems brittle (could replace more than once? would that be problematic?)
                                            data_table_query = data_table_query.replace(&user_id_q, &format!("{:?}", ghost_id));
                                        }
                                    }
                                }
                            }
                            // 4. issue the MODIFIED query to the data table (err if error)
                            self.db.query_drop(&data_table_query)?;
                            // 5. issue the UNMODIFIED query to the "materialized view" table by
                            //    replacing table names.
                            //    Note that this may insert the actual user_id, which is fine
                            return answer_rows(results, self.db.query_iter(mv_query));
                        }
                        Statement::Update{
                            table_name,
                            assignments,
                            selection,
                        } => {
                            match selection {
                                None => (),
                                Some(ref s) => {
                                    let mut user_cols : Vec<String> = Vec::<String>::new();
                                    match self.get_user_cols_of(table_name.to_string()) {
                                        Some(uc) => user_cols = uc.clone(),
                                        None => (),
                                    }

                                    // get substitued selection string, get user_id to ghost_ids mapping
                                    let (_substs, selection_str_ghosts) = self.substs_for_selection_expr(&user_cols, s)?;
                                    
                                    // next, if there are assignments to user IDs, we need to update the
                                    // corresponding ghost values in the ghost metadata table 
                                    for assn in assignments {
                                        // go column by column and update ghost ids respectively
                                        if user_cols.iter().any(|col| *col == assn.id.to_string()) {

                                            // TODO can do for all user_cols at once 
                                            // rather than one query for each user_col
                                            // get ghost value in corresponding to this usercol
                                            let update_select_str = format!("SELECT {} FROM {} {}", 
                                                assn.id, table_name, selection_str_ghosts);
                                            // update corresponding of ghost id in table to point to new user id
                                            let update_ghost_val_q = format!(
                                                "UPDATE ghost_metadata SET user_id = {} WHERE ghost_id = {};", 
                                                format!("{:?}", assn.value),
                                                update_select_str);
                                            self.db.query_drop(update_ghost_val_q)?;

                                            // make sure that the update doesn't replace the ghost
                                            // id in the data table by not updating this entry
                                            // in the data table (just set it back to the same
                                            // value)
                                            data_table_query = data_table_query
                                                .replace(&format!("{}", assn), &format!("{} = {}", assn.id, assn.id));
                                        }
                                    }
                                    let parts : Vec<&str> = data_table_query.split(" WHERE ").collect();
                                    if parts.len() != 2 {
                                        unimplemented!("Not a supported selection pattern: {}", s);
                                    }
                                    data_table_query = format!("{} WHERE {};", parts[0], selection_str_ghosts);
                                }
                            }
                            self.db.query_drop(data_table_query)?;
                            // return the "results" from the query
                            return answer_rows(results, self.db.query_iter(mv_query));
                        }
                        Statement::Delete {
                            table_name,
                            selection,
                        } => {
                            let mut user_cols : Vec<String> = Vec::<String>::new();
                            match self.get_user_cols_of(table_name.to_string()) {
                                Some(uc) => user_cols = uc.clone(),
                                None => (),
                            }

                            match selection {
                                None => (),
                                Some(ref s) => {
                                    // update the selection str with userids replaced with
                                    // relevant ghost ids
                                    let (_substs, selection_str_ghosts) = self.substs_for_selection_expr(&user_cols, s)?;
                                    let parts : Vec<&str> = data_table_query.split(" WHERE ").collect();
                                    if parts.len() != 2 {
                                        unimplemented!("Not a supported selection pattern: {}", s);
                                    }
                                    data_table_query = format!("{} WHERE {};", parts[0], selection_str_ghosts);
                                    self.db.query_drop(data_table_query)?;

                                    // actually perform query on MV
                                    return answer_rows(results, self.db.query_iter(mv_query));
                                }
                            }
                        }
                        // TODO support adding and modifying data tables (create table, etc.)
                        _ => {
                            // return the result from the MV
                            return answer_rows(results, self.db.query_iter(mv_query));
                        } 
                    }
                }
                Ok(())
            }
        }
    }
}
fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>,
) -> Result<(), mysql::Error> 
{
    match rows {
        Ok(rows) => {
            let cols : Vec<_> = rows
                .columns()
                .as_ref()
                .into_iter()
                .map(|c| {
                    Column {
                    table : c.table_str().to_string(),
                    column : c.name_str().to_string(),
                    coltype : get_coltype(&c.column_type()),
                    colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                }
            })
            .collect();
        let mut writer = results.start(&cols)?;
        for row in rows {
            let vals = row.unwrap();
            for (c, col) in cols.iter().enumerate() {
                match col.coltype {
                    ColumnType::MYSQL_TYPE_DECIMAL => writer.write_col(vals.get::<f64, _>(c))?,
                    ColumnType::MYSQL_TYPE_TINY => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_SHORT => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_LONG => writer.write_col(vals.get::<i32, _>(c))?,
                    ColumnType::MYSQL_TYPE_FLOAT => writer.write_col(vals.get::<f32, _>(c))?,
                    ColumnType::MYSQL_TYPE_DOUBLE => writer.write_col(vals.get::<f64, _>(c))?,
                    ColumnType::MYSQL_TYPE_NULL => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_LONGLONG => writer.write_col(vals.get::<i64, _>(c))?,
                    ColumnType::MYSQL_TYPE_INT24 => writer.write_col(vals.get::<i32, _>(c))?,
                    ColumnType::MYSQL_TYPE_VARCHAR => writer.write_col(vals.get::<String, _>(c))?,
                    ColumnType::MYSQL_TYPE_BIT => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_TINY_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_MEDIUM_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_LONG_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_VAR_STRING => writer.write_col(vals.get::<String, _>(c))?,
                    ColumnType::MYSQL_TYPE_STRING => writer.write_col(vals.get::<String, _>(c))?,
                    ColumnType::MYSQL_TYPE_GEOMETRY => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIMESTAMP => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_DATE => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIME => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_DATETIME => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_YEAR => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_NEWDATE => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIMESTAMP2 => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_DATETIME2 => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIME2 => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_JSON => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_NEWDECIMAL => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_ENUM => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_SET => writer.write_col(vals.get::<i16, _>(c))?,
                    ct => unimplemented!("Cannot translate row type {:?} into value", ct),
                }
            }
            writer.end_row()?;
        }
        writer.finish()?;
    }
    Err(e) => {
        results.error(ErrorKind::ER_BAD_SLAVE, format!("{:?}", e).as_bytes())?;
    }
}
Ok(())
}

/// Convert a MySQL type to MySQL_svr type 
fn get_coltype(t: &mysql::consts::ColumnType) -> ColumnType {
    match t {
        mysql::consts::ColumnType::MYSQL_TYPE_DECIMAL => ColumnType::MYSQL_TYPE_DECIMAL,
        mysql::consts::ColumnType::MYSQL_TYPE_TINY => ColumnType::MYSQL_TYPE_TINY,
        mysql::consts::ColumnType::MYSQL_TYPE_SHORT => ColumnType::MYSQL_TYPE_SHORT,
        mysql::consts::ColumnType::MYSQL_TYPE_LONG => ColumnType::MYSQL_TYPE_LONG,
        mysql::consts::ColumnType::MYSQL_TYPE_FLOAT => ColumnType::MYSQL_TYPE_FLOAT,
        mysql::consts::ColumnType::MYSQL_TYPE_DOUBLE => ColumnType::MYSQL_TYPE_DOUBLE,
        mysql::consts::ColumnType::MYSQL_TYPE_NULL => ColumnType::MYSQL_TYPE_NULL,
        mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => ColumnType::MYSQL_TYPE_TIMESTAMP,
        mysql::consts::ColumnType::MYSQL_TYPE_LONGLONG => ColumnType::MYSQL_TYPE_LONGLONG,
        mysql::consts::ColumnType::MYSQL_TYPE_INT24 => ColumnType::MYSQL_TYPE_INT24,
        mysql::consts::ColumnType::MYSQL_TYPE_DATE => ColumnType::MYSQL_TYPE_DATE,
        mysql::consts::ColumnType::MYSQL_TYPE_TIME => ColumnType::MYSQL_TYPE_TIME,
        mysql::consts::ColumnType::MYSQL_TYPE_DATETIME => ColumnType::MYSQL_TYPE_DATETIME,
        mysql::consts::ColumnType::MYSQL_TYPE_YEAR => ColumnType::MYSQL_TYPE_YEAR,
        mysql::consts::ColumnType::MYSQL_TYPE_NEWDATE => ColumnType::MYSQL_TYPE_NEWDATE,
        mysql::consts::ColumnType::MYSQL_TYPE_VARCHAR => ColumnType::MYSQL_TYPE_VARCHAR,
        mysql::consts::ColumnType::MYSQL_TYPE_BIT => ColumnType::MYSQL_TYPE_BIT,
        mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP2 => ColumnType::MYSQL_TYPE_TIMESTAMP2,
        mysql::consts::ColumnType::MYSQL_TYPE_DATETIME2 => ColumnType::MYSQL_TYPE_DATETIME2,
        mysql::consts::ColumnType::MYSQL_TYPE_TIME2 => ColumnType::MYSQL_TYPE_TIME2,
        mysql::consts::ColumnType::MYSQL_TYPE_JSON => ColumnType::MYSQL_TYPE_JSON,
        mysql::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        mysql::consts::ColumnType::MYSQL_TYPE_ENUM => ColumnType::MYSQL_TYPE_ENUM,
        mysql::consts::ColumnType::MYSQL_TYPE_SET => ColumnType::MYSQL_TYPE_SET,
        mysql::consts::ColumnType::MYSQL_TYPE_TINY_BLOB => ColumnType::MYSQL_TYPE_TINY_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_MEDIUM_BLOB => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_LONG_BLOB => ColumnType::MYSQL_TYPE_LONG_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_BLOB => ColumnType::MYSQL_TYPE_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_VAR_STRING => ColumnType::MYSQL_TYPE_VAR_STRING,
        mysql::consts::ColumnType::MYSQL_TYPE_STRING => ColumnType::MYSQL_TYPE_STRING,
        mysql::consts::ColumnType::MYSQL_TYPE_GEOMETRY => ColumnType::MYSQL_TYPE_GEOMETRY,
    }
}
