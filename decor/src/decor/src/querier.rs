use log::warn;
use msql_srv::QueryResultWriter;
use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::*;

use crate::disguises;
use crate::types::*;
use crate::{helpers, policy, subscriber};

/*
 * The controller issues queries to the database
 */
pub struct Querier {
    schema: policy::SchemaConfig,
    pub subscriber: subscriber::Subscriber,

    // for tests
    params: super::TestParams,
    pub cur_stat: helpers::stats::QueryStat,
    pub stats: Vec<helpers::stats::QueryStat>,
}

impl Querier {
    pub fn new(schema_config: policy::SchemaConfig, params: &super::TestParams) -> Self {
        Querier {
            schema: schema_config,
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
        let mut removed_rows: Vec<Row> = vec![];
        let mut created_ids: Vec<ID> = vec![];

        /*
         * 1. Get all reachable objects, organized by type
         */
        let table_info = &self.schema.table_info.get(&self.schema.user_table).unwrap();
        let id = ID {
            id: uid,
            table: self.schema.user_table.clone(),
            id_col_index: table_info.id_col_info.col_index,
            id_col_name: table_info.id_col_info.col_name.clone(),
        };

        /*
         * For now, just create user guises and redirect referencers
         */
        let refs = id.get_referencers(&self.schema, db)?;
        for (rid, fk) in refs {
            // create a unique guise with the given modifications
            let new_guise = disguises::copy_guise_with_modifications(
                &id,
                &table_info.guise_modifications,
                &self.schema,
                db,
            )?;

            // redirect the referencer to this guise
            disguises::redirect_referencer(&fk, &rid.id, new_guise.id, &self.schema, db)?;

            created_ids.push(new_guise.clone());
        }

        // remove the user
        disguises::delete_guise(&id, &self.schema, db)?;
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
        mappings: &Vec<ID>,
        object_data: &Vec<Row>,
        db: &mut mysql::Conn,
    ) -> Result<(), mysql::Error> {
        // TODO check auth token?
        warn!("Resubscribing uid {}", uid);

        //let mut object_data = object_data.clone();
        //self.subscriber.check_and_sort_resubscribed_data(uid, &mut guise_oid_mappings, &mut object_data, db)?;
        Ok(())
    }
}
