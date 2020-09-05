use mysql::prelude::*;
use sql_parser::ast::*;
use super::config;
use super::helpers;
use super::mv_transformer;

static mut LATEST_GID: u64 = super::GHOST_ID_START;

pub struct DataTableTransformer<'a> {
    cfg: config::Config,
    mv_trans: mv_transformer::MVTransformer,
    db: &'a mut mysql::Conn,
}

impl<'a> DataTableTransformer<'a> {
    pub fn new(cfg: config::Config, db: &'a mut mysql::Conn) -> Self {
        // better way than simply replicating?
        let mv_trans = mv_transformer::MVTransformer::new(cfg.clone());
        DataTableTransformer{cfg, mv_trans, db}
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
    
    /*fn setexpr_to_datatable_setexpr(&self, setexpr: &SetExpr) -> SetExpr {
        match setexpr {
            SetExpr::Select(s) => 
                SetExpr::Select(Box::new(Select{
                    distinct: s.distinct,
                    projection: s.projection
                        .iter()
                        .map(|si| match si {
                            SelectItem::Expr{
                                expr,
                                alias,
                            } => SelectItem::Expr{
                                expr: self.expr_to_datatable_expr(&expr),
                                alias: alias.clone(),
                            },
                            SelectItem::Wildcard => SelectItem::Wildcard
                        })
                        .collect(),
                    from: s.from
                        .iter()
                        .map(|twj| self.tablewithjoins_to_datatable_tablewithjoins(&twj))
                        .collect(),
                    selection: match &s.selection {
                        Some(e) => Some(self.expr_to_datatable_expr(&e)),
                        None => None,
                    },
                    group_by: s.group_by
                        .iter()
                        .map(|e| self.expr_to_datatable_expr(&e))
                        .collect(),
                    having: match &s.having {
                        Some(e) => Some(self.expr_to_datatable_expr(&e)),
                        None => None,
                    },
                })),
            SetExpr::Query(q) => SetExpr::Query(Box::new(self.query_to_datatable_query(&q))),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => SetExpr::SetOperation{
                    op: op.clone(),
                    all: *all,
                    left: Box::new(self.setexpr_to_datatable_setexpr(&left)),
                    right: Box::new(self.setexpr_to_datatable_setexpr(&right)),
                },
                SetExpr::Values(Values(v)) => SetExpr::Values(
                    Values(v
                        .iter()
                        .map(|exprs| exprs
                             .iter()
                             .map(|e| self.expr_to_datatable_expr(&e))
                             .collect())
                        .collect())),
        }
    }*/

    fn query_to_datatable_query(&self, query: &Query) -> Result<Query, mysql::Error> {
        let mv_q = self.mv_trans.query_to_mv_query(query);
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        let mut res = self.db.query_iter(&mv_q.to_string())?;
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
 
    fn expr_to_datatable_expr(&self, expr: &Expr) -> Result<Expr, mysql::Error> {
        let newExpr = match expr {
            Expr::FieldAccess {
                expr,
                field,
            } => Expr::FieldAccess {
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                field: field.clone(),
            },
            Expr::WildcardAccess(e) => Expr::WildcardAccess(Box::new(self.expr_to_datatable_expr(&e)?)),
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                list: list
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e)?)
                    .collect(),
                negated: *negated,
            },
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                subquery: Box::new(self.query_to_datatable_query(&subquery)?),
                negated: *negated,
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                negated: *negated,
                low: Box::new(self.expr_to_datatable_expr(&low)?),
                high: Box::new(self.expr_to_datatable_expr(&high)?),
            },
            Expr::BinaryOp{
                left,
                op,
                right
            } => Expr::BinaryOp{
                left: Box::new(self.expr_to_datatable_expr(&left)?),
                op: op.clone(),
                right: Box::new(self.expr_to_datatable_expr(&right)?),
            },
            Expr::UnaryOp{
                op,
                expr,
            } => Expr::UnaryOp{
                op: op.clone(),
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_datatable_expr(&expr)?)),
            Expr::Row{
                exprs,
            } => Expr::Row{
                exprs: exprs
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e)?)
                    .collect(),
            },
            Expr::Function(f) => Expr::Function(Function{
                name: f.name.clone(),
                args: match &f.args {
                    FunctionArgs::Star => FunctionArgs::Star,
                    FunctionArgs::Args(exprs) => FunctionArgs::Args(exprs
                        .iter()
                        .map(|e| self.expr_to_datatable_expr(&e)?)
                        .collect()),
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_datatable_expr(&filt)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => Some(WindowSpec{
                        partition_by: ws.partition_by
                            .iter()
                            .map(|e| self.expr_to_datatable_expr(&e)?)
                            .collect(),
                        order_by: ws.order_by
                            .iter()
                            .map(|obe| OrderByExpr {
                                expr: self.expr_to_datatable_expr(&obe.expr)?,
                                asc: obe.asc.clone(),
                            })
                            .collect(),
                        window_frame: ws.window_frame.clone(),
                    }),
                    None => None,
                },
                distinct: f.distinct,
            }),
            Expr::Case{
                operand,
                conditions,
                results,
                else_result,
            } => Expr::Case{
                operand: match operand {
                    Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e)?)),
                    None => None,
                },
                conditions: conditions
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e)?)
                    .collect(),
                results:results
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e)?)
                    .collect(),
                else_result: match else_result {
                    Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e)?)),
                    None => None,
                },
            },
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_datatable_query(&q)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_datatable_query(&q)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_datatable_expr(&left)?),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_datatable_expr(&left)?),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right)?),
            },
            Expr::List(exprs) => Expr::List(exprs
                .iter()
                .map(|e| self.expr_to_datatable_expr(&e)?)
                .collect()),
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                subscript: Box::new(self.expr_to_datatable_expr(&subscript)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => Expr::SubscriptSlice{
                expr: Box::new(self.expr_to_datatable_expr(&expr)?),
                positions: positions
                    .iter()
                    .map(|pos| SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_datatable_expr(&e)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_datatable_expr(&e)?),
                            None => None,
                        },
                    })
                    .collect(),
            },
            _ => expr.clone(),
        };
        Ok(newExpr)
    }

    fn vals_vec_to_datatable_vals(&self, vals_vec: &Vec<Vec<Expr>>, ucol_indices: &Vec<usize>) 
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
                    // user ids are always ints
                    let res = self.db.query_iter(&format!("INSERT INTO `ghosts` ({});", row[i]));
                    match res {
                        Err(_) => return None,
                        Ok(res) => {
                            // we want to insert the GID in place
                            // of the UID
                            val = Expr::Value(Value::Number(res.last_insert_id()?.to_string()));
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

    pub fn stmt_to_datatable_stmt(&mut self, stmt: &Statement) -> Result<Option<Statement>, mysql::Error> {
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
                                if let Some(vv) = self.vals_vec_to_datatable_vals(&vals_vec, &ucol_indices) {
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
                                let mut res = self.db.query_iter(&mv_q.to_string());
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

                                if let Some(vv) = self.vals_vec_to_datatable_vals(&vals_vec, &ucol_indices) {
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
                let mut dt_assn = Vec::<Assignment>::new();
                let mut dt_selection = selection.clone();
                // update assignment values
                // this may read from the MV table
                for a in assignments {
                    dt_assn.push(Assignment{
                        id : a.id.clone(),
                        value: self.expr_to_datatable_expr(&a.value)?,
                    });
                }
                // update selection 
                match selection {
                    None => (),
                    Some(s) => dt_selection = Some(self.expr_to_datatable_expr(&s)?),
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
                // update selection 
                match selection {
                    None => (),
                    Some(s) => dt_selection = Some(self.expr_to_datatable_expr(&s)?),
                }
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
                let dt_query = self.query_to_datatable_query(&query)?;
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
