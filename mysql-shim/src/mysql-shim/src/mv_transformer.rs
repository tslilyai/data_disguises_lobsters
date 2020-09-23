use mysql::prelude::*;
use sql_parser::ast::*;
use std::*;
use super::config;
use super::helpers;
use std::sync::atomic::{AtomicU64, Ordering};

static LATEST_UID : AtomicU64 = AtomicU64::new(1);

pub struct MVTransformer {
    user_table: config::UserTable,
    table_names: Vec<String>,
}

impl MVTransformer {
    pub fn new(cfg: &config::Config) -> Self {
        let mut table_names = Vec::<String>::new();
        for dt in &cfg.data_tables {
            table_names.push(dt.name.clone());
        }
        table_names.push(cfg.user_table.name.clone());

        MVTransformer{
            user_table: cfg.user_table.clone(),
            table_names: table_names, 
        }
    }   
   
    /* 
     * This issues the specified query to the MVs, and returns a VALUES query that
     * represents the values retrieved by the query to the MVs.
     * NOTE: queries are read-only operations (whereas statements may be writes)
     */
    fn query_to_value_query(&mut self, query: &Query, db: &mut mysql::Conn) -> Result<Query, mysql::Error> {
        let mv_q = self.query_to_mv_query(query);
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
     *
     * If ucols_to_replace is nonempty, the function sets whether 
     * any of these cols are contained within the query 
     */
    fn expr_to_value_expr(&mut self, expr: &Expr, db: &mut mysql::Conn, 
                              contains_ucol_id: &mut bool, ucols_to_replace: &Vec<String>) 
        -> Result<Expr, mysql::Error> 
    {
        let new_expr = match expr {
            Expr::Identifier(_ids) => {
                *contains_ucol_id |= ucols_to_replace.iter().any(|uc| uc.contains(&expr.to_string()));
                expr.clone()
            }
            Expr::QualifiedWildcard(_ids) => {
                *contains_ucol_id |= ucols_to_replace.iter().any(|uc| uc.contains(&expr.to_string()));
                expr.clone()
            }
            Expr::FieldAccess {
                expr,
                field,
            } => {
                // XXX we might be returning TRUE for contains_ucol_id if only the expr matches,
                // but not the field
                Expr::FieldAccess {
                    expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_query = self.query_to_value_query(&subquery, db)?;
                // otherwise just return table column IN subquery
                Expr::InSubquery {
                    expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    subquery: Box::new(new_query),
                    negated: *negated,
                }                
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let new_low = self.expr_to_value_expr(&low, db, contains_ucol_id, ucols_to_replace)?;
                let new_high = self.expr_to_value_expr(&high, db, contains_ucol_id, ucols_to_replace)?;
                Expr::Between {
                    expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    negated: *negated,
                    low: Box::new(new_low),
                    high: Box::new(new_high),
                }
            }
            Expr::BinaryOp{
                left,
                op,
                right
            } => {
                let new_left = self.expr_to_value_expr(&left, db, contains_ucol_id, ucols_to_replace)?;
                let new_right = self.expr_to_value_expr(&right, db, contains_ucol_id, ucols_to_replace)?;
                Expr::BinaryOp{
                    left: Box::new(new_left),
                    op: op.clone(),
                    right: Box::new(new_right),
                }
            }
            Expr::UnaryOp{
                op,
                expr,
            } => Expr::UnaryOp{
                op: op.clone(),
                expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
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
                            new_exprs.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_value_expr(&filt, db, contains_ucol_id, ucols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_value_expr(&obe.expr, db, contains_ucol_id, ucols_to_replace)?,
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
                    new_cond.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                }
            }
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_value_query(&q, db)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_value_query(&q, db)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_value_expr(&left, db, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right, db)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_value_expr(&left, db, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right, db)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                subscript: Box::new(self.expr_to_value_expr(&subscript, db, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_value_expr(&e, db, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_value_expr(&expr, db, contains_ucol_id, ucols_to_replace)?),
                    positions: new_pos,
                }
            }
            _ => expr.clone(),
        };
        Ok(new_expr)
    }

    fn insert_source_to_vals_vec(source: &InsertSource) -> Vec<Vec<Expr>> {
        let mut qt_source = source.clone();
        match source {
            InsertSource::Query(q) => {
                let mut contains_ucol_id = false;
                match &q.body {
                    SetExpr::Values(Values(expr_vals)) => {
                        // NOTE: only need to modify values if we're dealing with a DT,
                        // could perform check here rather than calling vals_vec
                        let mut vals_vec : Vec<Vec<Expr>>= vec![];
                        for row in expr_vals {
                            let mut vals_row : Vec<Expr>= vec![];
                            for val in row {
                                let query_val = match self.expr_to_value_expr(&val, db, &mut contains_ucol_id, &vec![])? {
                                    Expr::Subquery(q) => {
                                        match q.body {
                                            SetExpr::Values(Values(subq_exprs)) => {
                                                assert_eq!(subq_exprs.len(), 1);
                                                assert_eq!(subq_exprs[0].len(), 1);
                                                subq_exprs[0][0].clone()
                                            }
                                            _ => unimplemented!("query_to_data_query should only return a Value"),
                                        }
                                    }
                                    _ => val.clone(),
                                };
                                vals_row.push(self.expr_to_value_expr(&query_val, db, &mut contains_ucol_id, &vec![])?);
                            }
                            vals_vec.push(vals_row);
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
                            Err(_e) => {
                                return Ok(None);
                            }
                        }
                    }    
                }
                vals_vec
            }
            InsertSource::DefaultValues => vec![], // TODO might have to get rid of this
        }
    }


    /********************************************************
     * Processing statements to use materialized views      
     * ******************************************************/
    pub fn objname_to_mv_string(&self, obj: &ObjectName) -> String {
        let obj_mv = ObjectName(self.idents_to_mv_idents(&obj.0));
        obj_mv.to_string()
    }

    pub fn objname_to_mv_objname(&self, obj: &ObjectName) -> ObjectName {
        ObjectName(self.idents_to_mv_idents(&obj.0))
    }
 
    pub fn idents_to_mv_idents(&self, obj: &Vec<Ident>) -> Vec<Ident> {
        // note that we assume that the name specified in the config
        // is the minimum needed to identify the data table.
        // if there are duplicates, the database/schema would also
        // need to be present as well. however, we allow for overspecifying
        // in the query (so the data table name in the config may be a 
        // subset of the query name).
        
        let mut objs_mv = obj.clone();
        for dt in &self.table_names {
            if let Some((_start, end)) = helpers::str_subset_of_idents(dt, obj) {
                objs_mv.clear();
                for (index, ident) in obj.iter().enumerate() {
                    if index == end-1 {
                        // we found a match
                        objs_mv.push(Ident::new(&format!("{}{}", ident, super::MV_SUFFIX)));
                    } else {
                        objs_mv.push(ident.clone());
                    }
                } 
                break;
            }
        }
        objs_mv
    }

    fn tablefactor_to_mv_tablefactor(&self, tf: &TableFactor) -> TableFactor {
        match tf {
            TableFactor::Table {
                name,
                alias,
            } => {
                let mv_table_name = self.objname_to_mv_string(&name);
                TableFactor::Table{
                    name: helpers::string_to_objname(&mv_table_name),
                    alias: alias.clone(),
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => TableFactor::Derived {
                    lateral: *lateral,
                    subquery: Box::new(self.query_to_mv_query(&subquery)),
                    alias: alias.clone(),
                },
            TableFactor::NestedJoin {
                join,
                alias,
            } => TableFactor::NestedJoin{
                    join: Box::new(self.tablewithjoins_to_mv_tablewithjoins(&join)),
                    alias: alias.clone(),
                },
            _ => tf.clone(),
        }
    }

    fn joinoperator_to_mv_joinoperator(&self, jo: &JoinOperator) -> JoinOperator {
        let jo_mv : JoinOperator;
        match jo {
            JoinOperator::Inner(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::Inner(JoinConstraint::On(self.expr_to_mv_expr(e))),
            JoinOperator::LeftOuter(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::LeftOuter(JoinConstraint::On(self.expr_to_mv_expr(e))),
            JoinOperator::RightOuter(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::RightOuter(JoinConstraint::On(self.expr_to_mv_expr(e))),
            JoinOperator::FullOuter(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::FullOuter(JoinConstraint::On(self.expr_to_mv_expr(e))),
            _ => jo_mv = jo.clone(),
        }
        jo_mv
    }

    fn tablewithjoins_to_mv_tablewithjoins(&self, twj: &TableWithJoins) -> TableWithJoins {
        TableWithJoins {
            relation: self.tablefactor_to_mv_tablefactor(&twj.relation),
            joins: twj.joins
                .iter()
                .map(|j| Join {
                    relation: self.tablefactor_to_mv_tablefactor(&j.relation),
                    join_operator: self.joinoperator_to_mv_joinoperator(&j.join_operator),
                })
                .collect(),
        }
    }

    fn setexpr_to_mv_setexpr(&self, setexpr: &SetExpr) -> SetExpr {
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
                                expr: self.expr_to_mv_expr(&expr),
                                alias: alias.clone(),
                            },
                            SelectItem::Wildcard => SelectItem::Wildcard
                        })
                        .collect(),
                    from: s.from
                        .iter()
                        .map(|twj| self.tablewithjoins_to_mv_tablewithjoins(&twj))
                        .collect(),
                    selection: match &s.selection {
                        Some(e) => Some(self.expr_to_mv_expr(&e)),
                        None => None,
                    },
                    group_by: s.group_by
                        .iter()
                        .map(|e| self.expr_to_mv_expr(&e))
                        .collect(),
                    having: match &s.having {
                        Some(e) => Some(self.expr_to_mv_expr(&e)),
                        None => None,
                    },
                })),
            SetExpr::Query(q) => SetExpr::Query(Box::new(self.query_to_mv_query(&q))),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => SetExpr::SetOperation{
                    op: op.clone(),
                    all: *all,
                    left: Box::new(self.setexpr_to_mv_setexpr(&left)),
                    right: Box::new(self.setexpr_to_mv_setexpr(&right)),
                },
                SetExpr::Values(Values(v)) => SetExpr::Values(
                    Values(v
                        .iter()
                        .map(|exprs| exprs
                             .iter()
                             .map(|e| self.expr_to_mv_expr(&e))
                             .collect())
                        .collect())),
        }
    }

    pub fn query_to_mv_query(&self, query: &Query) -> Query {
        let mut mv_query = query.clone(); 

        let mut cte_mv_query : Query;
        for cte in &mut mv_query.ctes {
            cte_mv_query = self.query_to_mv_query(&cte.query);
            cte.query = cte_mv_query;
        }

        mv_query.body = self.setexpr_to_mv_setexpr(&query.body);

        let mut mv_oexpr : Expr;
        for orderby in &mut mv_query.order_by {
            mv_oexpr = self.expr_to_mv_expr(&orderby.expr);
            orderby.expr = mv_oexpr;
        }

        if let Some(e) = &query.limit {
            mv_query.limit = Some(self.expr_to_mv_expr(&e));
        }

        if let Some(e) = &query.offset {
            mv_query.offset = Some(self.expr_to_mv_expr(&e));
        }       

        if let Some(f) = &mut mv_query.fetch {
            if let Some(e) = &f.quantity {
                let new_quantity = Some(self.expr_to_mv_expr(&e));
                f.quantity = new_quantity;
            }
        }

        mv_query
    }
 
    pub fn expr_to_mv_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::Identifier(ids) => Expr::Identifier(self.idents_to_mv_idents(&ids)),
            Expr::QualifiedWildcard(ids) => Expr::QualifiedWildcard(self.idents_to_mv_idents(&ids)),
            Expr::FieldAccess {
                expr,
                field,
            } => Expr::FieldAccess {
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                field: field.clone(),
            },
            Expr::WildcardAccess(e) => Expr::WildcardAccess(Box::new(self.expr_to_mv_expr(&e))),
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                list: list
                    .iter()
                    .map(|e| self.expr_to_mv_expr(&e))
                    .collect(),
                negated: *negated,
            },
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                subquery: Box::new(self.query_to_mv_query(&subquery)),
                negated: *negated,
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                negated: *negated,
                low: Box::new(self.expr_to_mv_expr(&low)),
                high: Box::new(self.expr_to_mv_expr(&high)),
            },
            Expr::BinaryOp{
                left,
                op,
                right
            } => Expr::BinaryOp{
                left: Box::new(self.expr_to_mv_expr(&left)),
                op: op.clone(),
                right: Box::new(self.expr_to_mv_expr(&right)),
            },
            Expr::UnaryOp{
                op,
                expr,
            } => Expr::UnaryOp{
                op: op.clone(),
                expr: Box::new(self.expr_to_mv_expr(&expr)),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                collation: self.objname_to_mv_objname(&collation),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_mv_expr(&expr))),
            Expr::Row{
                exprs,
            } => Expr::Row{
                exprs: exprs
                    .iter()
                    .map(|e| self.expr_to_mv_expr(&e))
                    .collect(),
            },
            Expr::Function(f) => Expr::Function(Function{
                name: self.objname_to_mv_objname(&f.name),
                args: match &f.args {
                    FunctionArgs::Star => FunctionArgs::Star,
                    FunctionArgs::Args(exprs) => FunctionArgs::Args(exprs
                        .iter()
                        .map(|e| self.expr_to_mv_expr(&e))
                        .collect()),
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_mv_expr(&filt))),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => Some(WindowSpec{
                        partition_by: ws.partition_by
                            .iter()
                            .map(|e| self.expr_to_mv_expr(&e))
                            .collect(),
                        order_by: ws.order_by
                            .iter()
                            .map(|obe| OrderByExpr {
                                expr: self.expr_to_mv_expr(&obe.expr),
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
                    Some(e) => Some(Box::new(self.expr_to_mv_expr(&e))),
                    None => None,
                },
                conditions: conditions
                    .iter()
                    .map(|e| self.expr_to_mv_expr(&e))
                    .collect(),
                results:results
                    .iter()
                    .map(|e| self.expr_to_mv_expr(&e))
                    .collect(),
                else_result: match else_result {
                    Some(e) => Some(Box::new(self.expr_to_mv_expr(&e))),
                    None => None,
                },
            },
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_mv_query(&q))),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_mv_query(&q))),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_mv_expr(&left)),
                op: op.clone(),
                right: Box::new(self.query_to_mv_query(&right)),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_mv_expr(&left)),
                op: op.clone(),
                right: Box::new(self.query_to_mv_query(&right)),
            },
            Expr::List(exprs) => Expr::List(exprs
                .iter()
                .map(|e| self.expr_to_mv_expr(&e))
                .collect()),
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                subscript: Box::new(self.expr_to_mv_expr(&subscript)),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => Expr::SubscriptSlice{
                expr: Box::new(self.expr_to_mv_expr(&expr)),
                positions: positions
                    .iter()
                    .map(|pos| SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_mv_expr(&e)),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_mv_expr(&e)),
                                None => None,
                            },
                        })
                    .collect(),
            },
            _ => expr.clone(),
        }
    }

    pub fn stmt_to_mv_stmt(&mut self, stmt: &Statement, db: &mut mysql::Conn) -> Result<(Statement, bool /*is_write*/), mysql::Error> {
        let mv_stmt : Statement;
        let mut is_dt_write = false;
        let mv_table_name : String;

        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                let new_q = self.query_to_mv_query(&query);
                mv_stmt = Statement::Select(SelectStatement{
                    query: Box::new(new_q), 
                    as_of: as_of.clone(),
                });
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();
                let mut new_source = source.clone();
                
                // update sources if is a datatable
                if is_dt_write {
                    let vals_vec = self.insert_source_to_vals_vec(&source);
                    let mut new_q = q.clone();
                    new_q.body = SetExpr::Values(Values(vals_vec));
                    new_source = InsertSource::Query(new_q);
                   
                    // if the user table has an autoincrement column, we should 
                    // (1) see if the table is actually inserting a value for that column and
                    // (2) update the latest_uid appropriately and insert the value for that column
                    if table_name.to_string() == self.user_table.name && self.user_table.is_autoinc {
                        let inserting_uid_col = columns.iter().any(|c| c.to_string() == self.user_table.id_col);
                        if inserting_uid_col {
                            // TODO we need to get the values of the uid col being inserted and update
                            // appropriately 
                        } else {
                            // we need to get the number of rows being updated and put latest_uid + N
                            // as the id col values 
                        }
                    }
                }
                mv_stmt = Statement::Insert(InsertStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    columns : columns.clone(),
                    source : new_source, 
                });
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();
                let mut mv_assn = Vec::<Assignment>::new();
                let mut mv_selection = selection.clone();
                // update assignments
                for a in assignments {
                    mv_assn.push(Assignment{
                        id : a.id.clone(),
                        value: self.expr_to_mv_expr(&a.value),
                    });
                }
                // update selection 
                match selection {
                    None => (),
                    Some(s) => mv_selection = Some(self.expr_to_mv_expr(&s)),
                }
                mv_stmt = Statement::Update(UpdateStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    assignments : mv_assn,
                    selection : mv_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();
                let mut mv_selection = selection.clone();
                // update selection 
                match selection {
                    None => (),
                    Some(s) => mv_selection = Some(self.expr_to_mv_expr(&s)),
                }
                mv_stmt = Statement::Delete(DeleteStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    selection : mv_selection,
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
                let mv_query = self.query_to_mv_query(&query);
                mv_stmt = Statement::CreateView(CreateViewStatement{
                    name: name.clone(),
                    columns: columns.clone(),
                    with_options: with_options.clone(),
                    query : Box::new(mv_query),
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
                mv_table_name = self.objname_to_mv_string(&name);
                is_dt_write = mv_table_name != name.to_string();
                let mv_constraints : Vec<TableConstraint> = constraints
                    .iter()
                    .map(|c| match c {
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                        } => {
                            let foreign_table = self.objname_to_mv_string(foreign_table);
                            TableConstraint::ForeignKey{
                                name: name.clone(),
                                columns: columns.clone(),
                                foreign_table: helpers::string_to_objname(&foreign_table),
                                referred_columns: referred_columns.clone(),
                            }
                        }
                        _ => c.clone(),
                    })
                    .collect(); 

                let mut mv_cols = columns.clone();
                // if we're creating the user table, remove any autoinc column
                if name.to_string() == self.user_table.name {
                    for col in &mut mv_cols{
                        if col.name.to_string() != self.user_table.id_col {
                            continue;
                        }

                        // if this is the user id column and it is autoincremented,
                        // remove autoincrement in materialized view
                        // TODO add test to make sure autoincrement removed
                        if col.options.iter().any(|cod| cod.option == ColumnOption::AutoIncrement) {
                            self.user_table.is_autoinc = true;
                            col.options.retain(|x| x.option != ColumnOption::AutoIncrement);
                        }
                        break;
                    }
                }
                mv_stmt = Statement::CreateTable(CreateTableStatement{
                    name: helpers::string_to_objname(&mv_table_name),
                    columns: mv_cols,
                    constraints: mv_constraints,
                    with_options: with_options.clone(),
                    if_not_exists: if_not_exists.clone(),
                });
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
                mv_table_name = self.objname_to_mv_string(&on_name);
                is_dt_write = mv_table_name != on_name.to_string();
                mv_stmt = Statement::CreateIndex(CreateIndexStatement{
                    name: name.clone(),
                    on_name: helpers::string_to_objname(&mv_table_name),
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
                let mut to_item_mv_name = to_item_name.to_string();
                mv_table_name= self.objname_to_mv_string(&name);
                is_dt_write = mv_table_name != name.to_string();
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        if mv_table_name != name.to_string() {
                            // change config to reflect new table name
                            self.table_names.push(to_item_name.to_string());
                            self.table_names.retain(|x| *x != *name.to_string());
                            // TODO change config as table names are updated
                            /*if self.cfg.user_table.name == name.to_string() {
                                self.cfg.user_table.name = to_item_name.to_string();
                            } else {
                                for tab in &mut self.cfg.data_tables {
                                    if tab.name == name.to_string() {
                                        tab.name = to_item_name.to_string();
                                    }
                                }
                            }*/
                            to_item_mv_name = format!("{}{}", to_item_name, super::MV_SUFFIX);
                        }
                    }
                    _ => (),
                }
                mv_stmt = Statement::AlterObjectRename(AlterObjectRenameStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    name: helpers::string_to_objname(&mv_table_name),
                    to_item_name: Ident::new(to_item_mv_name),
                });
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
                let mut mv_names = names.clone();
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        for name in &mut mv_names {
                            let newname = self.objname_to_mv_string(&name);
                            is_dt_write |= newname != name.to_string();
                            *name = helpers::string_to_objname(&newname);
                        }
                    }
                    _ => (),
                }
                mv_stmt = Statement::DropObjects(DropObjectsStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    names: mv_names,
                    cascade: *cascade,
                });
            }
            Statement::ShowObjects(ShowObjectsStatement{
                object_type,
                from,
                extended,
                full,
                materialized,
                filter,
            }) => {
                let mut mv_from = from.clone();
                if let Some(f) = from {
                    mv_from = Some(helpers::string_to_objname(&self.objname_to_mv_string(&f)));
                }

                let mut mv_filter = filter.clone();
                if let Some(f) = filter {
                    match f {
                        ShowStatementFilter::Like(_s) => (),
                        ShowStatementFilter::Where(expr) => {
                            mv_filter = Some(ShowStatementFilter::Where(self.expr_to_mv_expr(&expr)));
                        }
                    }
                }
                mv_stmt = Statement::ShowObjects(ShowObjectsStatement{
                    object_type: object_type.clone(),
                    from: mv_from,
                    extended: *extended,
                    full: *full,
                    materialized: *materialized,
                    filter: mv_filter,
                })
            }
            Statement::ShowIndexes(ShowIndexesStatement{
                table_name,
                extended,
                filter,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                let mut mv_filter = filter.clone();
                if let Some(f) = filter {
                    match f {
                        ShowStatementFilter::Like(_s) => (),
                        ShowStatementFilter::Where(expr) => {
                            mv_filter = Some(ShowStatementFilter::Where(self.expr_to_mv_expr(&expr)));
                        }
                    }
                }
                mv_stmt = Statement::ShowIndexes(ShowIndexesStatement {
                    table_name: helpers::string_to_objname(&mv_table_name),
                    extended: *extended,
                    filter: mv_filter,
                });
            }
            /* TODO Handle Statement::Explain(stmt) => f.write_node(stmt)
             *
             * TODO Currently don't support alterations that reset autoincrement counters
             * Assume that deletions leave autoincrement counters as monotonically increasing
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
             *
             * XXX: SHOW * from users will not return any ghost users in ghostusersMV
             * */
            _ => {
                mv_stmt = stmt.clone();
            }
        }
        Ok((mv_stmt, is_dt_write))
    }
}
