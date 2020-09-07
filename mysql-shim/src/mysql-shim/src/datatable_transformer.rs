use mysql::prelude::*;
use sql_parser::ast::*;
use super::config;
use super::helpers;
use super::mv_transformer;

static mut LATEST_GID: u64 = super::GHOST_ID_START;

pub struct DataTableTransformer {
    cfg: config::Config,
    mv_trans: mv_transformer::MVTransformer,
}

impl DataTableTransformer {
    pub fn new(cfg: config::Config) -> Self {
        // better way than simply replicating?
        let mv_trans = mv_transformer::MVTransformer::new(cfg.clone());
        DataTableTransformer{cfg, mv_trans}
    }   
    
    fn get_user_cols_of_datatable(&self, table_name: &Vec<Ident>) -> Vec<String> {
        let mut res = vec![];
        for dt in &self.cfg.data_tables {
            if let Some(_p) = helpers::objname_subset_match_range(table_name, &dt.name) {
                res = dt.user_cols.clone();
                break;
            }
        }
        res
    }
    
    /* 
     * This issues the specified query to the MVs, and returns a VALUES query that
     * represents the values retrieved by the query to the MVs.
     * NOTE: queries are read-only operations (whereas statements may be writes)
     */
    fn query_to_datatable_query(&mut self, query: &Query, db: &mut mysql::Conn) -> Result<Query, mysql::Error> {
        let mv_q = self.mv_trans.query_to_mv_query(query);
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        let res = db.query_iter(&mv_q.to_string())?;
        for row in res {
            let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
            vals_vec.push(mysql_vals
                .iter()
                .map(|val| Expr::Value(helpers::mysql_val_to_parser_val(&val)))
                .collect());
        }
        Ok(Query {
            ctes: vec![],
            body: SetExpr::Values(Values(vals_vec)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        })
    }

    /*
     * This changes any nested queries to the corresponding VALUE 
     * (read from the MVs), if any exist.
     * If no nested queries are present, the expr is kept unchanged.
     *
     * If replace_uids is nonempty, the specified uids are swapped for their set of corresponding
     * gids
     */
    fn expr_to_datatable_expr(&mut self, expr: &Expr, db: &mut mysql::Conn, table_name: &ObjectName, ucols_to_replace: &Vec<String>) 
        -> Result<Expr, mysql::Error> 
    {
        let new_expr = match expr {
            Expr::FieldAccess {
                expr,
                field,
            } => {
                let new_expr = self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?;
                Expr::FieldAccess {
                    expr: Box::new(new_expr),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                subquery: Box::new(self.query_to_datatable_query(&subquery, db)?),
                negated: *negated,
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                negated: *negated,
                low: Box::new(self.expr_to_datatable_expr(&low, db, table_name, ucols_to_replace)?),
                high: Box::new(self.expr_to_datatable_expr(&high, db, table_name, ucols_to_replace)?),
            },
            Expr::BinaryOp{
                left,
                op,
                right
            } => Expr::BinaryOp{
                left: Box::new(self.expr_to_datatable_expr(&left, db, table_name, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.expr_to_datatable_expr(&right, db, table_name, ucols_to_replace)?),
            },
            Expr::UnaryOp{
                op,
                expr,
            } => Expr::UnaryOp{
                op: op.clone(),
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                }
                Expr::Row{
                    exprs: new_exprs,
                }
            }
            Expr::Function(f) => Expr::Function(Function{
                name: f.name.clone(),
                args: match &f.args {
                    FunctionArgs::Star => FunctionArgs::Star,
                    FunctionArgs::Args(exprs) => {
                        let mut new_exprs = vec![];
                        for e in exprs {
                            new_exprs.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_datatable_expr(&filt, db, table_name, ucols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_datatable_expr(&obe.expr, db, table_name, ucols_to_replace)?,
                                asc: obe.asc.clone(),
                            });
                        }
                        Some(WindowSpec{
                            partition_by: new_pb,
                            order_by: new_ob,
                            window_frame: ws.window_frame.clone(),
                        })
                    }
                    None => None,
                },
                distinct: f.distinct,
            }),
            Expr::Case{
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut new_cond = vec![];
                for e in conditions {
                    new_cond.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?)),
                        None => None,
                    },
                }
            }
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_datatable_query(&q, db)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_datatable_query(&q, db)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_datatable_expr(&left, db, table_name, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right, db)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_datatable_expr(&left, db, table_name, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right, db)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                subscript: Box::new(self.expr_to_datatable_expr(&subscript, db, table_name, ucols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_datatable_expr(&e, db, table_name, ucols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_datatable_expr(&expr, db, table_name, ucols_to_replace)?),
                    positions: new_pos,
                }
            }
            _ => expr.clone(),
        };
        Ok(new_expr)
    }

    fn vals_vec_to_datatable_vals(&mut self, vals_vec: &Vec<Vec<Expr>>, ucol_indices: &Vec<usize>, db: &mut mysql::Conn) 
        -> Option<Vec<Vec<Expr>>> 
    {
        if ucol_indices.is_empty() {
            return Some(vals_vec.to_vec());
        }         
        let mut parser_val_tuples = vec![];
        for row in vals_vec {
            let mut parser_vals : Vec<Expr> = vec![];
            for i in 0..row.len() {
                let mut val = row[i].clone();
                // add entry to ghosts table
                if ucol_indices.contains(&i) {
                    // NULL check: don't add ghosts entry if new UID value is NULL
                    if val != Expr::Value(Value::Null) {
                        // user ids are always ints
                        let res = db.query_iter(&format!("INSERT INTO `ghosts` ({});", row[i]));
                        match res {
                            Err(_) => return None,
                            Ok(res) => {
                                // we want to insert the GID in place
                                // of the UID
                                val = Expr::Value(Value::Number(res.last_insert_id()?.to_string()));
                            }
                        }
                    }
                }
                // add to vector of values for this row
                parser_vals.push(val);
            }
            parser_val_tuples.push(parser_vals);
        }
        Some(parser_val_tuples)
    }

    pub fn stmt_to_datatable_stmt(&mut self, stmt: &Statement, db: &mut mysql::Conn) -> Result<Option<Statement>, mysql::Error> {
        let mut dt_stmt = stmt.clone();

        match stmt {
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                /* note that if the table is the users table,
                 * we just want to insert like usual; we only care about
                 * adding ghost ids for data tables, but we don't add ghosts to
                 * the user table
                 */

                /* For all columns that are user columns, generate a new ghost_id and insert
                     into ghosts table with appropriate user_id value
                     those as the values instead for those columns.
                    This will be empty if the table is the user table, or not a datatable
                 */
                let ucols = self.get_user_cols_of_datatable(&table_name.0);
                let mut ucol_indices = vec![];
                // get indices of columns corresponding to user vals
                if !ucols.is_empty() {
                    for (i, c) in columns.into_iter().enumerate() {
                        if ucols.iter().any(|uc| *uc == c.to_string()) {
                            ucol_indices.push(i);
                        }
                    }
                }

                // update sources
                let mut dt_source = source.clone();
                /* if no user columns, change sources to use MV
                 * otherwise, we need to insert new GID->UID mappings 
                 * with the values of the usercol value as the UID
                 * and then set the GID as the new source value of the usercol 
                 * */
                match source {
                    InsertSource::Query(q) => {
                        match &q.body {
                            SetExpr::Values(Values(vals_vec)) => {
                                // NOTE: only need to modify values if we're dealing with a DT,
                                // could perform check here rather than calling vals_vec
                                if let Some(vv) = self.vals_vec_to_datatable_vals(&vals_vec, &ucol_indices, db) {
                                    let mut new_q = q.clone();
                                    new_q.body = SetExpr::Values(Values(vv));
                                    dt_source = InsertSource::Query(new_q);
                                } else {
                                    return Ok(None);
                                }
                            }
                            _ => {
                                // we need to issue q to MVs to get rows that will be set as values
                                // regardless of whether this is a DT or not (because query needs
                                // to read from MV, rather than initially specified tables)
                                let mv_q = self.mv_trans.query_to_mv_query(q);
                                let mut vals_vec : Vec<Vec<Expr>>= vec![];
                                let mut res = db.query_iter(&mv_q.to_string());
                                match res {
                                    Ok(ref mut rows) => {
                                        for row in rows {
                                            let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                                            vals_vec.push(mysql_vals
                                                .iter()
                                                .map(|val| Expr::Value(helpers::mysql_val_to_parser_val(&val)))
                                                .collect());
                                        }
                                    }
                                    _ => return Ok(None),
                                }
                                drop(res);

                                if let Some(vv) = self.vals_vec_to_datatable_vals(&vals_vec, &ucol_indices, db) {
                                    let mut new_q = q.clone();
                                    new_q.body = SetExpr::Values(Values(vv));
                                    dt_source = InsertSource::Query(new_q);
                                } else {
                                    return Ok(None);
                                }
                            }    
                        }
                    } 
                    InsertSource::DefaultValues => (), // TODO might have to get rid of this
                }
                dt_stmt = Statement::Insert(InsertStatement{
                    table_name: table_name.clone(),
                    columns : columns.clone(),
                    source : dt_source, 
                });
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                let ucols = self.get_user_cols_of_datatable(&table_name.0);
                let mut ucol_assigns = vec![];
                let mut ucol_selectitems = vec![];
                let mut dt_assn = vec![];
                for a in assignments {
                    // don't replace any UIDs when converting assignments to values
                    let new_val = self.expr_to_datatable_expr(&a.value, db, &table_name, &vec![])?;
                    
                    // we still want to perform the update
                    // BUT we need to make sure that the updated value, if a 
                    // expr with a query, reads from the MV rather than the datatables
                    // we also want to update any usercol value to NULL if the UID is being set to
                    // NULL. 
                    let is_ucol = ucols.iter().any(|uc| *uc == a.id.to_string());
                    if is_ucol || new_val == Expr::Value(Value::Null) {
                        dt_assn.push(Assignment{
                            id: a.id.clone(),
                            value: new_val,
                        });
                    } else if is_ucol {
                        // if we have an assignment to a UID, we need to update the GID->UID mapping
                        // instead of updating the actual data table record
                        // note that we still include NULL entries so we know to delete this GID
                        ucol_assigns.push(Assignment {
                            id: a.id.clone(),
                            value: new_val,
                        });
                        ucol_selectitems.push(SelectItem::Expr{
                            expr: Expr::Identifier(vec![a.id.clone()]),
                            alias: None,
                        });
                    }
                }
              
                let mut dt_selection = None;
                if let Some(s) = selection {
                    // update selection to use matching set of GIDs in place of any UIDs that
                    // might be used to perform the selection
                    let new_s = self.expr_to_datatable_expr(&s, db, &table_name, &ucols)?;
                    dt_selection = Some(new_s);
                } 

                // if usercols are being updated, query DT to get the relevant
                // GIDs and update these GID->UID mappings in the ghosts table
                if !ucol_assigns.is_empty() {
                    let get_gids_stmt = Statement::Select(SelectStatement {
                        query: Box::new(Query::select(Select{
                            distinct: true,
                            projection: ucol_selectitems,
                            from: vec![TableWithJoins{
                                relation: TableFactor::Table{
                                    name: table_name.clone(),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: dt_selection.clone(),
                            group_by: vec![],
                            having: None,
                        })),
                        as_of: None,
                    });
                    // get the user_col GIDs from the datatable
                    let res = db.query_iter(format!("{}", get_gids_stmt.to_string()))?;
                    let mut ghost_update_stmts = vec![];
                    for row in res {
                        let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                        for (i, uc_val) in ucol_assigns.iter().enumerate() {
                            let gid = helpers::mysql_val_to_parser_val(&mysql_vals[i]);
                            // delete the GID entry if it is being set to NULL
                            if uc_val.value == Expr::Value(Value::Null) {
                                ghost_update_stmts.push(Statement::Delete(DeleteStatement {
                                    table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                                    selection: Some(Expr::BinaryOp{
                                        left: Box::new(Expr::Identifier(vec![uc_val.id.clone()])),
                                        op: BinaryOperator::Eq,
                                        right: Box::new(Expr::Value(Value::Number(format!("{}", gid)))),
                                    }),
                                }));
                            } else {
                                // otherwise, update GID entry with new UID value
                                ghost_update_stmts.push(Statement::Update(UpdateStatement {
                                    table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                                    assignments: vec![uc_val.clone()],
                                    selection: Some(Expr::BinaryOp{
                                        left: Box::new(Expr::Identifier(vec![uc_val.id.clone()])),
                                        op: BinaryOperator::Eq,
                                        right: Box::new(Expr::Value(Value::Number(format!("{}", gid)))),
                                    }),
                                }));
                            }
                        }
                    }
                    for gstmt in ghost_update_stmts {
                        db.query_drop(format!("{}", gstmt.to_string()))?;
                    }
                }
                dt_stmt = Statement::Update(UpdateStatement{
                    table_name: table_name.clone(),
                    assignments : dt_assn,
                    selection : dt_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                let mut dt_selection = selection.clone();
                let ucols = self.get_user_cols_of_datatable(&table_name.0);

                // update selection 
                if let Some(s) = selection {
                    let new_s = self.expr_to_datatable_expr(&s, db, &table_name, &ucols)?;
                    dt_selection = Some(new_s);
                }

                // TODO delete from ghosts table if GIDs are removed
                dt_stmt = Statement::Delete(DeleteStatement{
                    table_name: table_name.clone(),
                    selection : dt_selection,
                });
            }
            Statement::CreateView(CreateViewStatement{
                name,
                columns,
                with_options,
                query,
                if_exists,
                temporary,
                materialized,
            }) => {
                let dt_query = self.query_to_datatable_query(&query, db)?;
                dt_stmt = Statement::CreateView(CreateViewStatement{
                    name: name.clone(),
                    columns: columns.clone(),
                    with_options: with_options.clone(),
                    query : Box::new(dt_query),
                    if_exists: if_exists.clone(),
                    temporary: temporary.clone(),
                    materialized: materialized.clone(),
                });
            }
            Statement::CreateTable(CreateTableStatement{
                name,
                columns,
                constraints,
                with_options,
                if_not_exists,
            }) => {
                let dt_constraints = constraints
                    .iter()
                    .map(|c| match c {
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                        } => {
                            TableConstraint::ForeignKey{
                                name: name.clone(),
                                columns: columns.clone(),
                                foreign_table: foreign_table.clone(),
                                referred_columns: referred_columns.clone(),
                            }
                        }
                        _ => c.clone(),
                    })
                    .collect(); 
                dt_stmt = Statement::CreateTable(CreateTableStatement{
                    name: name.clone(),
                    columns: columns.clone(),
                    constraints: dt_constraints,
                    with_options: with_options.clone(),
                    if_not_exists: if_not_exists.clone(),
                });
                // TODO might have to add auto_increment here 
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
                dt_stmt = Statement::CreateIndex(CreateIndexStatement{
                    name: name.clone(),
                    on_name: on_name.clone(),
                    key_parts: key_parts.clone(),
                    if_not_exists: if_not_exists.clone(),
                });
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
                dt_stmt = Statement::AlterObjectRename(AlterObjectRenameStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    name: name.clone(),
                    to_item_name: to_item_name.clone(),
                });
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
                dt_stmt = Statement::DropObjects(DropObjectsStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    names: names.clone(),
                    cascade: *cascade,
                });
            }
            /* TODO Handle Statement::Explain(stmt) => f.write_node(stmt)
             *
             * Don't handle CreateSink, CreateSource, Copy,
             *  ShowCreateSource, ShowCreateSink, Tail, Explain
             * 
             * Don't modify queries for CreateSchema, CreateDatabase, 
             * ShowDatabases, ShowCreateTable, DropDatabase, Transactions,
             * ShowColumns, SetVariable (mysql exprs in set var not supported yet)
             *
             * XXX: ShowVariable, ShowCreateView and ShowCreateIndex will return 
             *  queries that used the materialized views, rather than the 
             *  application-issued tables. This is probably not a big issue, 
             *  since these queries are used to create the table again?
             * */
            _ => ()
        }
        return Ok(Some(dt_stmt));
    }
}
