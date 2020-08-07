extern crate mysql;
use msql_srv::*;
use std::*;

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
        if res {
            return Ok(());
        }
        else {
            return Err(
                io::Error::new(
                    io::ErrorKind::Other,
                    "select db packet error",
                ));
        }   
    }

    fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        let cols = [
            Column {
                table: "foo".to_string(),
                column: "a".to_string(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "foo".to_string(),
                column: "b".to_string(),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            },
        ];

        let mut rw = results.start(&cols)?;
        rw.write_col(42)?;
        rw.write_col("b's value")?;
        rw.finish()
    }
}
