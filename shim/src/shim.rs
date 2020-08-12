mod config;

extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::*;
use std::*;

const SCHEMA : &'static str = include_str!("schema.sql");
const CONFIG_FILE : &'static str = "config.json";

pub struct Shim { db: mysql::Conn }

impl Shim {
    pub fn new(db: mysql::Conn) -> Self {
        Shim{db}
    }   
}

impl Drop for Shim {
    fn drop(&mut self) {
        // drop the connection (implicitly done).
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }
    
    fn on_execute(
        &mut self,
        _: u32,
        _: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(0, 0)
    }
    
    fn on_close(&mut self, _: u32) {
    }

    fn on_init(&mut self, schema: &str, _: InitWriter<W>) -> io::Result<()> { 
        let res = self.db.select_db(schema);
        if !res {
            return Err(
                io::Error::new(
                    io::ErrorKind::Other,
                    "select db packet error",
                ));
        }   
        /* create ghost metadata table */
        self.db.query_drop(
            r"CREATE TABLE IF NOT EXISTS ghost_metadata (
                user_id int unsigned,
                ghost_id int unsigned NOT NULL,
                record_id int unsigned NOT NULL,
                table_id varchar(50),
                UNIQUE INDEX ghost_id (ghost_id)
            );"
        ).unwrap();

        /* get config so we can detect data tables */
        let cfg = config::parse_config(CONFIG_FILE.to_string());

        /* create materialized view for all data tables */
        /* for debugging purposes, print these queries out... */
        let dialect = MySqlDialect{};
        let mut q = String::new();
        for line in SCHEMA.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !q.is_empty() {
                q.push_str(" ");
            }
            q.push_str(line);
        }
        let stmts = Parser::parse_sql(&dialect, &q).unwrap();
        for stmt in stmts {
            match stmt {
                Statement::CreateTable {
                    name,
                    //columns,
                    //constraints,
                    //with_options,
                    ..
                } => {
                    let query  = r"SELECT
                        COALESCE (ghost_metadata.ghost_id, users.id) AS user_id,
                        users.username,
                        users.karma
                    FROM ghost_metadata LEFT JOIN users
                    ON ghost_metadata.user_id = users.id;";

                    // TODO 
                    // - construct query properly
                    // - match against user id cols, table names
                    // - actually execute query
                    // - write tests
                    let mut query_stmts = Parser::parse_sql(&dialect, &query).unwrap();
                    let only_query = query_stmts.pop().unwrap(); 
                    match only_query {
                        Statement::Query(query) => {
                            let mut mvname = name.to_string();
                            mvname.push_str("_mv");
                            let view_q = Statement::CreateView {
                                name: ObjectName(vec![Ident::new(mvname)]),
                                columns: Vec::<Ident>::new(),
                                query: Box::new(*query),
                                materialized: true,
                                with_options: Vec::<SqlOption>::new(), 
                            };
                        },
                        _ => panic!("Expected Query"),
                    }
                },
                _ => continue, // we only handle create table stmts
            }
        }
        Ok(())
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        match self.db.query_iter(query) {
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
