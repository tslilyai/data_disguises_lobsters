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
use crate::policy::*;

/*
 * The controller issues queries to the database
 */
pub struct Querier {
    schema_config: SchemaConfig,
    pub subscriber: subscriber::Subscriber,

    // for tests
    params: super::TestParams,
    pub cur_stat: helpers::stats::QueryStat,
    pub stats: Vec<helpers::stats::QueryStat>,
}

impl Querier {
    pub fn new(schema_config: SchemaConfig, params: &super::TestParams) -> Self {
        Querier {
            schema_config: schema_config,
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
         * 0. Get target "global" object
         */
        let table_info = &self
            .schema_config
            .table_info
            .get(&self.schema_config.user_table)
            .unwrap();
        let target = ID {
            id: uid,
            table: self.schema_config.user_table.clone(),
            id_col_index: table_info.id_col_info.col_index,
            id_col_name: table_info.id_col_info.col_name.clone(),
        };

        /*
         * 1. Get all single objects satisfying predicates
         */
        for (table, polvec) in self.schema_config.single_policies.iter() {
            for pol in polvec {
                let pred = helpers::subst_target_values_in_expr(&target, &pol.predicate);
                let matching = self.get_objects_satisfying_pred(&table, &pred, db)?;
                for row in matching {
                    match pol.action {
                        Action::Delete => {
                            removed_rows.append(&mut disguises::delete_guise(&row.id, &self.schema_config, db)?);
                        }
                        Action::Modify => {
                            if let Some(m) = &pol.modifications {
                                row.id.update_row_with_modifications(m, db)?;
                            }
                        }
                        _ => continue
                    }
                }
            }
        }
        
        /*
         * 2. Get all pairs of objects satisfying predicates
         * NOTE: Some may no longer satisfy the predicate because of the single policies already
         * applied---is that ok?
         */
        for (pair, pol) in self.schema_config.pair_policies.iter(){
            // query both tables using policy single predicates
            // alter according to policy
        }

        /*let refs = id.get_referencers(&self.schema_config, db)?;
        for (rid, fk) in refs {
            let emptyv = vec![];
            let pair_policies = self.schema_config.pair_policies.get(&TableNamePair {
                type1: id.table.clone(),
                type2: rid.id.table.clone(),
            }).unwrap_or(&emptyv);

            for p in pair_policies {
                // do pair policies actions
                let new_guise = disguises::copy_guise_with_modifications(
                &id,
                &table_info.guise_modifications,
                db,
            )?;

            // redirect the referencer to this guise
            disguises::redirect_referencer(&fk, &rid.id, new_guise.id, db)?;
            }

            // create a unique guise with the given modifications
            

            created_ids.push(new_guise.clone());
        }*/

        // remove the user
        Ok(())
    }

    fn get_objects_satisfying_pred(&self, table: &str, pred: &Predicate, db: &mut mysql::Conn) -> Result<Vec<Row>, mysql::Error> {
        // create query using policy predicate
        let q = Select {
            distinct: true,
            projection: vec![],
            from: vec![TableWithJoins {
                relation: TableFactor:: Table {
                    name: helpers::string_to_objname(table),
                    alias: None,
                },
                joins: vec![],
            }],
            selection: Some(pred.clone()),
            group_by: vec![],
            having: None,
        }.to_string();

        let id_col_info = &self.schema_config.table_info.get(table).unwrap().id_col_info;
        let rows = helpers::get_rows_of_query(&q, table, &id_col_info.col_name, db)?;
        
        Ok(rows)
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
