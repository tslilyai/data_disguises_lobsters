use log::warn;
use msql_srv::QueryResultWriter;
use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::*;

use crate::disguises;
use crate::{helpers, subscriber};

/*
 * The controller issues queries to the database
 */
pub struct Querier {
    pub subscriber: subscriber::Subscriber,

    // for tests
    params: super::TestParams,
    pub cur_stat: helpers::stats::QueryStat,
    pub stats: Vec<helpers::stats::QueryStat>,
}

impl Querier {
    pub fn new(params: &super::TestParams) -> Self {
        Querier {
            subscriber: subscriber::Subscriber::new(),

            params: params.clone(),
            cur_stat: helpers::stats::QueryStat::new(),
            stats: vec![],
        }
    }

    pub fn query<W: io::Write>(
        &mut self,
        writer: QueryResultWriter<W>,
        stmt: &Statement,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        warn!("issue db statement: {}", stmt);

        match stmt {
            Statement::Select(SelectStatement { query, .. }) => {
                helpers::answer_rows(writer, db.query_iter(query.to_string()))
            }
            _ => {
                self.query_drop(stmt, db)?;
                writer.completed(0, 0)?;
                Ok(())
            }
        }
    }

    pub fn query_drop(
        &mut self,
        stmt: &Statement,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        match stmt {
            Statement::Insert(InsertStatement { .. }) => {
                db.query_drop(stmt.to_string())?;
                self.cur_stat.nqueries += 1;
            }
            Statement::Update(UpdateStatement { .. }) => {
                db.query_drop(stmt.to_string())?;
                self.cur_stat.nqueries += 1;
            }
            Statement::Delete(DeleteStatement { .. }) => {
                db.query_drop(stmt.to_string())?;
                self.cur_stat.nqueries += 1;
            }
            Statement::CreateTable(CreateTableStatement {
                name,
                columns,
                constraints,
                indexes,
                with_options,
                if_not_exists,
                engine,
            }) => {
                let mut new_engine = engine.clone();
                if self.params.in_memory {
                    new_engine = Some(Engine::Memory);
                }

                let dtstmt = CreateTableStatement {
                    name: name.clone(),
                    columns: columns.clone(),
                    constraints: constraints.clone(),
                    indexes: indexes.clone(),
                    with_options: with_options.clone(),
                    if_not_exists: *if_not_exists,
                    engine: new_engine.clone(),
                };
                db.query_drop(dtstmt.to_string())?;
                self.cur_stat.nqueries += 1;
            }
            Statement::DropObjects(DropObjectsStatement { object_type, .. }) => {
                match object_type {
                    ObjectType::Table => {
                        // alter the data table
                        db.query_drop(stmt.to_string())?;
                        self.cur_stat.nqueries += 1;
                    }
                    _ => unimplemented!("Cannot drop object {}", stmt),
                }
            }
            _ => unimplemented!("stmt not supported: {}", stmt),
        }
        Ok(())
    }

    pub fn record_query_stats(&mut self, qtype: helpers::stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries += self.subscriber.get_nqueries();
        self.cur_stat.duration = dur;
        self.cur_stat.qtype = qtype;
        self.stats.push(self.cur_stat.clone());
        self.cur_stat.clear();
    }

    /*******************************************************
     ****************** UNSUBSCRIPTION *********************
     *******************************************************/
    pub fn unsubscribe<W: io::Write>(
        &mut self,
        uid: u64,
        db: &mut mysql::Conn,
        writer: QueryResultWriter<W>,
    ) -> Result<(), mysql::Error> {

        Ok(())
    }

    /*******************************************************
     ****************** RESUBSCRIPTION *********************
     *******************************************************/
    /*
     * Set all user_ids in the guises table to specified user
     * Refresh "materialized views"
     * TODO add back deleted content from shard
     */
    pub fn resubscribe(
        &mut self,
        uid: u64,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        // TODO check auth token?
        warn!("Resubscribing uid {}", uid);

        //let mut object_data = object_data.clone();
        //self.subscriber.check_and_sort_resubscribed_data(uid, &mut guise_oid_mappings, &mut object_data, db)?;
        Ok(())
    }
}
