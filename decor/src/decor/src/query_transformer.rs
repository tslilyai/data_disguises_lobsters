use mysql::prelude::*;
use sql_parser::ast::*;
use super::config;
use super::helpers;
use std::sync::atomic::Ordering;
use std::*;
use log::warn;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::collections::HashMap;

pub struct QTStats {
    pub nqueries : usize,
}

pub struct QueryTransformer {
    pub uid2gids: HashMap<u64, Vec<u64>>,
    pub gid2uid: HashMap<u64, u64>,

    table_names: Vec<String>,
    latest_gid: AtomicU64,
    latest_uid: AtomicUsize,

    pub cfg: config::Config,
    pub params: super::TestParams,
    pub stats: QTStats,
}

impl QueryTransformer {
    pub fn new(cfg: &config::Config, params: &super::TestParams) -> Self {
        let mut table_names = Vec::<String>::new();
        for dt in &cfg.data_tables {
            table_names.push(dt.name.clone());
        }
        
        QueryTransformer{
            cfg: cfg.clone(),
            params: params.clone(),
            table_names: table_names, 
            latest_gid: AtomicU64::new(super::GHOST_ID_START),
            latest_uid: AtomicUsize::new(0),
            stats: QTStats{nqueries:0},
            uid2gids: HashMap::new(),
            gid2uid: HashMap::new(),
        }
    }   

    /**************************************** 
     **** Converts Queries/Exprs to Values **
     ****************************************/
    fn insert_gid_into_caches(&mut self, uid:u64, gid:u64) {
        match self.uid2gids.get_mut(&uid) {
            Some(gids) => (*gids).push(gid),
            None => {
                self.uid2gids.insert(uid, vec![gid]);
            }
        }
        self.gid2uid.insert(gid, uid);
    }

    pub fn get_gids_for(&mut self, uid:u64, txn:&mut mysql::Transaction) -> Result<Vec<u64>, mysql::Error> {
        match self.uid2gids.get_mut(&uid) {
            Some(gids) => Ok(gids.to_vec()),
            None => {
                self.cache_uid2gids_for_uid(uid, txn)?;
                let gids = self.uid2gids.get(&uid).ok_or(
                    mysql::Error::IoError(io::Error::new(
                        io::ErrorKind::Other, "get_gids: uid not present in cache?")))?;
                Ok(gids.to_vec())
            }
        }
    }

    /* 
     * Add uid->gid mapping to cache if mapping not yet present
     * by querying the ghosts mapping table
     */
    pub fn cache_uid2gids_for_uid(&mut self, uid: u64, txn:&mut mysql::Transaction) -> Result<(), mysql::Error>
    {
        if self.uid2gids.get_mut(&uid) == None {
            let get_gids_of_uid_stmt = Query::select(Select{
                distinct: true,
                projection: vec![
                    SelectItem::Expr{
                        expr: Expr::Identifier(helpers::string_to_objname(&super::GHOST_USER_COL).0),
                        alias: None,
                    },
                    SelectItem::Expr{
                        expr: Expr::Identifier(helpers::string_to_objname(&super::GHOST_ID_COL).0),
                        alias: None,
                    }
                ],
                from: vec![TableWithJoins{
                    relation: TableFactor::Table{
                        name: helpers::string_to_objname(&super::GHOST_TABLE_NAME),
                        alias: None,
                    },
                    joins: vec![],
                }],
                selection: Some(Expr::BinaryOp{
                    left: Box::new(Expr::Identifier(helpers::string_to_idents(&super::GHOST_USER_COL))),
                    op: BinaryOperator::Eq, 
                    right: Box::new(Expr::Value(Value::Number(uid.to_string()))),
                }),
                group_by: vec![],
                having: None,
            });

            warn!("cache_uid2gids: {}", get_gids_of_uid_stmt);
            let res = txn.query_iter(format!("{}", get_gids_of_uid_stmt.to_string()))?;
            self.stats.nqueries+=1;
            for row in res {
                let mut vals = vec![];
                for v in row.unwrap().unwrap() {
                    vals.push(helpers::mysql_val_to_u64(&v)?);
                }
                self.insert_gid_into_caches(vals[0], vals[1]);
            }
        }
        Ok(())
    }

    /* 
     * This issues the specified query to the MVs, and returns a VALUES query that
     * represents the values retrieved by the query to the MVs.
     * NOTE: queries are read-only operations (whereas statements may be writes)
     */
    fn query_to_value_query(&mut self, query: &Query, txn: &mut mysql::Transaction) -> Result<Query, mysql::Error> {
        let mv_q = self.query_to_mv_query(query);
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        
        warn!("query_to_value_query: {}", mv_q);
        let res = txn.query_iter(&mv_q.to_string())?;
        self.stats.nqueries+=1;

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
    fn expr_to_value_expr(&mut self, expr: &Expr, txn: &mut mysql::Transaction, 
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
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                    field: field.clone(),
                }
            }
            Expr::WildcardAccess(e) => {
                Expr::WildcardAccess(Box::new(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?))
            }
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut new_list = vec![];
                for e in list {
                    new_list.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::InList {
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                    list: new_list,
                    negated: *negated,
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_query = self.query_to_value_query(&subquery, txn)?;
                // otherwise just return table column IN subquery
                Expr::InSubquery {
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
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
                let new_low = self.expr_to_value_expr(&low, txn, contains_ucol_id, ucols_to_replace)?;
                let new_high = self.expr_to_value_expr(&high, txn, contains_ucol_id, ucols_to_replace)?;
                Expr::Between {
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
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
                let new_left = self.expr_to_value_expr(&left, txn, contains_ucol_id, ucols_to_replace)?;
                let new_right = self.expr_to_value_expr(&right, txn, contains_ucol_id, ucols_to_replace)?;
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
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                collation: collation.clone(),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?)),
            Expr::Row{
                exprs,
            } => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
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
                            new_exprs.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                        }
                        FunctionArgs::Args(new_exprs)
                    }                
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_value_expr(&filt, txn, contains_ucol_id, ucols_to_replace)?)),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => {
                        let mut new_pb = vec![];
                        for e in &ws.partition_by {
                            new_pb.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                        }
                        let mut new_ob = vec![];
                        for obe in &ws.order_by {
                            new_ob.push(OrderByExpr {
                                expr: self.expr_to_value_expr(&obe.expr, txn, contains_ucol_id, ucols_to_replace)?,
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
                    new_cond.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                let mut new_res= vec![];
                for e in results {
                    new_res.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::Case{
                    operand: match operand {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                    conditions: new_cond ,
                    results: new_res, 
                    else_result: match else_result {
                        Some(e) => Some(Box::new(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?)),
                        None => None,
                    },
                }
            }
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_value_query(&q, txn)?)),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_value_query(&q, txn)?)),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_value_expr(&left, txn, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right, txn)?),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_value_expr(&left, txn, contains_ucol_id, ucols_to_replace)?),
                op: op.clone(),
                right: Box::new(self.query_to_value_query(&right, txn)?),
            },
            Expr::List(exprs) => {
                let mut new_exprs = vec![];
                for e in exprs {
                    new_exprs.push(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?);
                }
                Expr::List(new_exprs)
            }
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                subscript: Box::new(self.expr_to_value_expr(&subscript, txn, contains_ucol_id, ucols_to_replace)?),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => {
                let mut new_pos = vec![];
                for pos in positions {
                    new_pos.push(SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_value_expr(&e, txn, contains_ucol_id, ucols_to_replace)?),
                            None => None,
                        },                
                    });
                }
                Expr::SubscriptSlice{
                    expr: Box::new(self.expr_to_value_expr(&expr, txn, contains_ucol_id, ucols_to_replace)?),
                    positions: new_pos,
                }
            }
            _ => expr.clone(),
        };
        Ok(new_expr)
    }

    fn insert_source_query_to_values(&mut self, q: &Query, txn: &mut mysql::Transaction) -> Result<Vec<Vec<Expr>>, mysql::Error> {
        let mut contains_ucol_id = false;
        let mut vals_vec : Vec<Vec<Expr>>= vec![];
        match &q.body {
            SetExpr::Values(Values(expr_vals)) => {
                // NOTE: only need to modify values if we're dealing with a DT,
                // could perform check here rather than calling vals_vec
                for row in expr_vals {
                    let mut vals_row : Vec<Expr> = vec![];
                    for val in row {
                        let query_val = match self.expr_to_value_expr(&val, txn, &mut contains_ucol_id, &vec![])? {
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
                        vals_row.push(self.expr_to_value_expr(&query_val, txn, &mut contains_ucol_id, &vec![])?);
                    }
                    vals_vec.push(vals_row);
                }
            }
            _ => {
                // we need to issue q to MVs to get rows that will be set as values
                // regardless of whether this is a DT or not (because query needs
                // to read from MV, rather than initially specified tables)
                let mv_q = self.query_to_mv_query(q);
                warn!("insert_source_q_to_values: {}", mv_q);
                let rows = txn.query_iter(&mv_q.to_string())?;
                self.stats.nqueries+=1;
                for row in rows {
                    let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                    vals_vec.push(mysql_vals
                        .iter()
                        .map(|val| Expr::Value(helpers::mysql_val_to_parser_val(&val)))
                        .collect());
                }
            }
        }    
        Ok(vals_vec)
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

    pub fn get_mv_stmt(&mut self, stmt: &Statement, txn: &mut mysql::Transaction) -> mysql::Result<Statement> {
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
                })
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                is_dt_write = mv_table_name != table_name.to_string();
                let mut new_source = source.clone();
                let mut mv_cols = columns.clone();
                let mut new_q = None;
                
                // update sources if is a datatable
                let mut values = vec![];
                if is_dt_write || table_name.to_string() == self.cfg.user_table.name {
                    match source {
                        InsertSource::Query(q) => {
                            values = self.insert_source_query_to_values(&q, txn)?;
                            new_q = Some(q.clone());
                        }
                        InsertSource::DefaultValues => (),
                    }
                }

                // issue to datatable with vals_vec BEFORE we modify vals_vec to include the
                // user_id column
                if is_dt_write {
                    self.issue_insert_datatable_stmt(&mut values.clone(), InsertStatement{
                        table_name: table_name.clone(), 
                        columns: columns.clone(), 
                        source: source.clone(),
                    }, txn)?;
                }
                                       
                // if the user table has an autoincrement column, we should 
                // (1) see if the table is actually inserting a value for that column (found) 
                // (2) update the self.latest_uid appropriately and insert the value for that column
                if table_name.to_string() == self.cfg.user_table.name && self.cfg.user_table.is_autoinc {
                    let mut found = false;
                    for (i, col) in columns.iter().enumerate() {
                        if col.to_string() == self.cfg.user_table.id_col {
                            // get the values of the uid col being inserted and update as
                            // appropriate
                            let mut max = self.latest_uid.load(Ordering::SeqCst);
                            for vv in &values{
                                match &vv[i] {
                                    Expr::Value(Value::Number(n)) => {
                                        let n = n.parse::<usize>().map_err(|e| mysql::Error::IoError(io::Error::new(
                                                        io::ErrorKind::Other, format!("{}", e))))?;
                                        max = cmp::max(max, n);
                                    }
                                    _ => (),
                                }
                            }
                            // TODO ensure self.latest_uid never goes above GID_START
                            self.latest_uid.fetch_max(max, Ordering::SeqCst);
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        // put self.latest_uid + N as the id col values 
                        let cur_uid = self.latest_uid.fetch_add(values.len(), Ordering::SeqCst);
                        for i in 0..values.len() {
                            values[i].push(Expr::Value(Value::Number(format!("{}", cur_uid + i + 1))));
                        }
                        // add id column to update
                        mv_cols.push(Ident::new(self.cfg.user_table.id_col.clone()));
                    }
                    // update source with new vals_vec
                    if let Some(mut nq) = new_q {
                        nq.body = SetExpr::Values(Values(values.clone()));
                        new_source = InsertSource::Query(nq);
                    }
                }
                
                mv_stmt = Statement::Insert(InsertStatement{
                    table_name: helpers::string_to_objname(&mv_table_name),
                    columns : mv_cols,
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

                let mut assign_vals = vec![];
                if is_dt_write || table_name.to_string() == self.cfg.user_table.name {
                    let mut contains_ucol_id = false;
                    for a in assignments {
                        assign_vals.push(self.expr_to_value_expr(&a.value, txn, &mut contains_ucol_id, &vec![])?);
                    }
                }
                    
                if is_dt_write {
                    self.issue_update_datatable_stmt(
                        &assign_vals,
                        UpdateStatement{
                            table_name: table_name.clone(), 
                            assignments: assignments.clone(), 
                            selection: selection.clone()
                        }, 
                        txn)?;
                }
 
                // if the user table has an autoincrement column, we should 
                // (1) see if the table is actually updating a value for that column and
                // (2) update the self.latest_uid appropriately 
                if table_name.to_string() == self.cfg.user_table.name && self.cfg.user_table.is_autoinc {
                    for i in 0..assignments.len() {
                        if assignments[i].id.to_string() == self.cfg.user_table.id_col {
                            match &assign_vals[i] {
                                Expr::Value(Value::Number(n)) => {
                                    let n = n.parse::<usize>().map_err(|e| mysql::Error::IoError(io::Error::new(
                                                    io::ErrorKind::Other, format!("{}", e))))?;
                                    self.latest_uid.fetch_max(n, Ordering::SeqCst);
                                }
                                _ => (),
                            }
                        }
                    }
                }

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

                if is_dt_write {
                    self.issue_delete_datatable_stmt(DeleteStatement{
                        table_name: table_name.clone(), 
                        selection: selection.clone(),
                    }, txn)?;
                }

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
                indexes,
                with_options,
                if_not_exists,
                engine,
            }) => {
                mv_table_name = self.objname_to_mv_string(&name);
                is_dt_write = mv_table_name != name.to_string();
                let mut new_engine = engine.clone();
                if self.params.in_memory {
                    new_engine = Some(Engine::Memory);
                }

                if is_dt_write {
                    // create the original table as well if we're going to
                    // create a MV for this table
                    let dtstmt = CreateTableStatement {
                        name: name.clone(),
                        columns: columns.clone(),
                        constraints: constraints.clone(),
                        indexes: indexes.clone(),
                        with_options: with_options.clone(),
                        if_not_exists: *if_not_exists,
                        engine: new_engine.clone(),
                    };

                    warn!("get_mv stmt: {}", dtstmt);
                    txn.query_drop(dtstmt.to_string())?;
                    self.stats.nqueries+=1;
                }

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
                if name.to_string() == self.cfg.user_table.name {
                    for col in &mut mv_cols{
                        if col.name.to_string() != self.cfg.user_table.id_col {
                            continue;
                        }

                        // if this is the user id column and it is autoincremented,
                        // remove autoincrement in materialized view
                        if col.options.iter().any(|cod| cod.option == ColumnOption::AutoIncrement) {
                            self.cfg.user_table.is_autoinc = true;
                            col.options.retain(|x| x.option != ColumnOption::AutoIncrement);
                        }
                        break;
                    }
                }
                
                mv_stmt = Statement::CreateTable(CreateTableStatement{
                    name: helpers::string_to_objname(&mv_table_name),
                    columns: mv_cols,
                    constraints: mv_constraints,
                    indexes: indexes.clone(),
                    with_options: with_options.clone(),
                    if_not_exists: *if_not_exists,
                    engine: new_engine,
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

                if is_dt_write {
                    // create the original index as well if we're going to
                    // create a MV index 
                    warn!("get_mv: {}", stmt);
                    txn.query_drop(stmt.to_string())?;
                    self.stats.nqueries+=1;
                }

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

                if is_dt_write {
                    // alter the original table as well if we're going to
                    // alter a MV table
                    warn!("get_mv: {}", stmt);
                    txn.query_drop(stmt.to_string())?;
                    self.stats.nqueries+=1;
                }
                
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

                            if is_dt_write {
                                // alter the original table as well if we're going to
                                // alter a MV table
                                warn!("get_mv: {}", stmt);
                                txn.query_drop(stmt.to_string())?;
                                self.stats.nqueries+=1;
                            }

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
                });
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
                mv_stmt = stmt.clone()
            }
        }
        Ok(mv_stmt)
    }
      
    /*
     * DATATABLE QUERY TRANSFORMER FUNCTIONS
     */
    fn insert_gid_for_uid(&mut self, uid: u64, txn: &mut mysql::Transaction) -> Result<u64, mysql::Error> {
        // user ids are always ints
        let insert_query = &format!("INSERT INTO {} ({}) VALUES ({});", 
                            super::GHOST_TABLE_NAME, super::GHOST_USER_COL, uid);
        warn!("insert_gid_for_uid: {}", insert_query);
        let res = txn.query_iter(insert_query)?;
        self.stats.nqueries+=1;
        
        // we want to insert the GID in place of the UID
        let gid = res.last_insert_id().ok_or_else(|| 
            mysql::Error::IoError(io::Error::new(
                io::ErrorKind::Other, "Last GID inserted could not be retrieved")))?;
      
        // insert into cache
        self.insert_gid_into_caches(uid, gid);

        // update the last known GID
        self.latest_gid.fetch_max(gid, Ordering::SeqCst);
        Ok(gid)
    }

    fn insert_uid2gids_for_values(&mut self, values: &mut Vec<Vec<Expr>>, ucol_indices: &Vec<usize>, txn: &mut mysql::Transaction) 
        -> Result<(), mysql::Error>
    {
        if ucol_indices.is_empty() {
            return Ok(());
        }         
        for row in 0..values.len() {
            for col in 0..values[row].len() {
                // add entry to ghosts table
                if ucol_indices.contains(&col) {
                    // NULL check: don't add ghosts entry if new UID value is NULL
                    if values[row][col] != Expr::Value(Value::Null) {
                        let uid = helpers::parser_expr_to_u64(&values[row][col])?;
                        let gid = self.insert_gid_for_uid(uid, txn)?;
                        values[row][col] = Expr::Value(Value::Number(gid.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    fn selection_to_datatable_selection(
        &mut self, 
        selection: &Option<Expr>, 
        txn: &mut mysql::Transaction, 
        table_name: &ObjectName, 
        ucols: &Vec<String>) 
        -> Result<Option<Expr>, mysql::Error>
    {
        let mut qt_selection = None;
        let mut contains_ucol_id = false;
        if let Some(s) = selection {
            // check if the expr contains any conditions on user columns
            qt_selection = Some(self.expr_to_value_expr(&s, txn, &mut contains_ucol_id, &ucols)?);

            // if a user column is being used as a selection criteria, first perform a 
            // select of all UIDs of matching rows in the MVs
            if contains_ucol_id {
                // get the matching rows from the MVs 
                let mv_select_stmt = Statement::Select(SelectStatement {
                        query: Box::new(Query::select(Select{
                        distinct: true,
                        projection: vec![SelectItem::Wildcard],
                        from: vec![TableWithJoins{
                            relation: TableFactor::Table{
                                name: self.objname_to_mv_objname(table_name),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: Some(self.expr_to_mv_expr(&s)),
                        group_by: vec![],
                        having: None,
                    })),
                    as_of: None,
                });

                // collect row results from MV
                let mut uids = vec![];
                let mut rows : Vec<Vec<mysql::Value>> = vec![];
                let mut cols = vec![];
                warn!("selection_to_dt_selection: {}", mv_select_stmt);
                let res = txn.query_iter(format!("{}", mv_select_stmt.to_string()))?;
                self.stats.nqueries+=1;
                for row in res {
                    let row = row.unwrap();
                    cols = row.columns_ref().to_vec();
                    
                    let mut row_vals = vec![];
                    for i in 0..cols.len() {
                        // if it's a user column, add restriction on GID
                        let colname = cols[i].name_str().to_string();
 
                        // Add condition on user column to be within relevant GIDs mapped
                        // to by the UID value
                        // However, only update with GIDs if UID value is NOT NULL
                        if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) 
                            && row[i] != mysql::Value::NULL 
                        {
                            uids.push(helpers::mysql_val_to_u64(&row[i])?);
                        }
                        row_vals.push(row[i].clone());
                    }
                    rows.push(row_vals);
                }

                // get all the gid rows corresponding to uids
                // TODO deal with potential GIDs in user_cols due to
                // unsubscriptions/resubscriptions
                for uid in uids {
                    self.cache_uid2gids_for_uid(uid, txn)?;
                }

                // expr to constrain to select a particular row
                let mut or_row_constraint_expr = Expr::Value(Value::Boolean(false));
                for row in rows {
                    let mut and_col_constraint_expr = Expr::Value(Value::Boolean(true));
                    for i in 0..cols.len() {
                        // if it's a user column, add restriction on GID
                        let colname = cols[i].name_str().to_string();
                        // Add condition on user column to be within relevant GIDs mapped
                        // to by the UID value
                        // However, only update with GIDs if UID value is NOT NULL
                        if ucols.iter().any(|uc| helpers::str_ident_match(&colname, uc)) 
                            && row[i] != mysql::Value::NULL 
                        {
                            let uid = helpers::mysql_val_to_u64(&row[i])?;
                            // add condition on user column to be within the relevant GIDs
                            and_col_constraint_expr = Expr::BinaryOp {
                                left: Box::new(and_col_constraint_expr),
                                op: BinaryOperator::And,
                                right: Box::new(Expr::InList {
                                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                    list: match self.uid2gids.get(&uid) {
                                        Some(gids) => gids.iter()
                                            .map(|g| Expr::Value(Value::Number(g.to_string())))
                                            .collect(),
                                        None => vec![],
                                    },
                                    negated: false,
                                }),
                            };

                        } else {

                            // otherwise, we just want to constrain the row value to the
                            // actual value returned
                            and_col_constraint_expr = Expr::BinaryOp {
                                left: Box::new(and_col_constraint_expr),
                                op: BinaryOperator::And,
                                right: Box::new(Expr::BinaryOp{
                                    left: Box::new(Expr::Identifier(helpers::string_to_idents(&colname))),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(Expr::Value(helpers::mysql_val_to_parser_val(&row[i]))),
                                }),             
                            };
                        }
                    }

                    // we allow the selection to match ANY returned row
                    or_row_constraint_expr = Expr::BinaryOp {
                        left: Box::new(or_row_constraint_expr),
                        op: BinaryOperator::Or,
                        right: Box::new(and_col_constraint_expr),
                    };
                }
                qt_selection = Some(or_row_constraint_expr);
            } 
        } 
        return Ok(qt_selection);
    }
    
    fn issue_insert_datatable_stmt(&mut self, values: &mut Vec<Vec<Expr>>, stmt: InsertStatement, txn: &mut mysql::Transaction) 
        -> Result<(), mysql::Error> 
    {
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
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let mut ucol_indices = vec![];
        // get indices of columns corresponding to user vals
        if !ucols.is_empty() {
            for (i, c) in (&stmt.columns).into_iter().enumerate() {
                // XXX this may overcount if a non-user column is a suffix of a user
                // column
                if ucols.iter().any(|uc| helpers::str_ident_match(&c.to_string(), uc)) {
                    ucol_indices.push(i);
                }
            }
        }

        // update sources
        let mut qt_source = stmt.source.clone();
        /* if no user columns, change sources to use MV
         * otherwise, we need to insert new GID->UID mappings 
         * with the values of the usercol value as the UID
         * and then set the GID as the new source value of the usercol 
         * */
        match stmt.source {
            InsertSource::Query(q) => {
                self.insert_uid2gids_for_values(values, &ucol_indices, txn)?;
                let mut new_q = q.clone();
                new_q.body = SetExpr::Values(Values(values.to_vec()));
                qt_source = InsertSource::Query(new_q);
            }
            InsertSource::DefaultValues => (),
        }
       
        let dt_stmt = Statement::Insert(InsertStatement{
            table_name: stmt.table_name.clone(),
            columns : stmt.columns.clone(),
            source : qt_source, 
        });
 
        warn!("issue_insert_dt_stmt: {}", dt_stmt);
        txn.query_drop(dt_stmt.to_string())?;
        self.stats.nqueries+=1;

        Ok(())
    }
    
    fn update_uid2gids_with(&mut self, pairs: &Vec<(Option<u64>, u64)>)
        -> Result<(), mysql::Error> 
    {
        for (uid, gid) in pairs {
            // delete current mapping
            if let Some(olduid) = self.gid2uid.get(gid) {
                if let Some(gids) = self.uid2gids.get_mut(&olduid) {
                    gids.retain(|x| *x != *gid);
                }
                self.gid2uid.remove(gid);
            }

            // update if there is a new mapping
            if let Some(newuid) = uid {
                self.insert_gid_into_caches(*newuid, *gid);
            }
        }
        Ok(())
    }
    
    fn issue_update_datatable_stmt(&mut self, assign_vals: &Vec<Expr>, stmt: UpdateStatement, txn: &mut mysql::Transaction)
        -> Result<(), mysql::Error> 
    {
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let mut ucol_assigns = vec![];
        let mut ucol_selectitems_assn = vec![];
        let mut qt_assn = vec![];

        for (i, a) in stmt.assignments.iter().enumerate() {
            // we still want to perform the update BUT we need to make sure that the updated value, if a 
            // expr with a query, reads from the MV rather than the datatables
                                
            // we won't replace any UIDs when converting assignments to values, but
            // we also want to update any usercol value to NULL if the UID is being set to NULL, so we put it
            // in qt_assn too (rather than only updating the GID)
            let is_ucol = ucols.iter().any(|uc| helpers::str_ident_match(&a.id.to_string(), uc));
            if !is_ucol || assign_vals[i] == Expr::Value(Value::Null) {
                qt_assn.push(Assignment{
                    id: a.id.clone(),
                    value: assign_vals[i].clone(),
                });
            } 
            // if we have an assignment to a UID, we need to update the GID->UID mapping
            // instead of updating the actual data table record
            // note that we still include NULL entries so we know to delete this GID
            if is_ucol {
                ucol_assigns.push(Assignment {
                    id: a.id.clone(),
                    value: assign_vals[i].clone(),
                });
                ucol_selectitems_assn.push(SelectItem::Expr{
                    expr: Expr::Identifier(vec![a.id.clone()]),
                    alias: None,
                });
            }
        }

        let qt_selection = self.selection_to_datatable_selection(
            &stmt.selection, txn, &stmt.table_name, &ucols)?;
     
        // if usercols are being updated, query DT to get the relevant
        // GIDs and update these GID->UID mappings in the ghosts table
        if !ucol_assigns.is_empty() {
            let get_gids_stmt_from_dt = Statement::Select(SelectStatement {
                query: Box::new(Query::select(Select{
                    distinct: true,
                    projection: ucol_selectitems_assn,
                    from: vec![TableWithJoins{
                        relation: TableFactor::Table{
                            name: stmt.table_name.clone(),
                            alias: None,
                        },
                        joins: vec![],
                    }],
                    selection: qt_selection.clone(),
                    group_by: vec![],
                    having: None,
                })),
                as_of: None,
            });
            // get the user_col GIDs from the datatable
            warn!("issue_update_datatable_stmt: {}", get_gids_stmt_from_dt);
            let res = txn.query_iter(format!("{}", get_gids_stmt_from_dt.to_string()))?;
            self.stats.nqueries+=1;

            let mut ghost_update_stmts = vec![];
            let mut ghost_update_pairs = vec![];
            for row in res {
                let mysql_vals : Vec<mysql::Value> = row.unwrap().unwrap();
                for (i, uc_val) in ucol_assigns.iter().enumerate() {
                    let gid = helpers::mysql_val_to_parser_val(&mysql_vals[i]);
                    // delete the GID entry if it is being set to NULL
                    if uc_val.value == Expr::Value(Value::Null) {
                        ghost_update_stmts.push(Statement::Delete(DeleteStatement {
                            table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                            selection: Some(Expr::BinaryOp{
                                left: Box::new(Expr::Identifier(
                                              helpers::string_to_idents(super::GHOST_ID_COL))),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(gid)),
                            }),
                        }));
                        ghost_update_pairs.push((None, helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    } else {
                        // otherwise, update GID entry with new UID value
                        // XXX what if the value IS a GID??? should we just remove this GID?
                        ghost_update_stmts.push(Statement::Update(UpdateStatement {
                            table_name: helpers::string_to_objname(super::GHOST_TABLE_NAME),
                            assignments: vec![Assignment{
                                id: Ident::new(super::GHOST_USER_COL),
                                value: uc_val.value.clone(),
                            }],
                            selection: Some(Expr::BinaryOp{
                                left: Box::new(Expr::Identifier(
                                              helpers::string_to_idents(super::GHOST_ID_COL))),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(gid)),
                            }),
                        }));
                        ghost_update_pairs.push(
                            (Some(helpers::parser_expr_to_u64(&uc_val.value)?), 
                             helpers::mysql_val_to_u64(&mysql_vals[i])?));
                    }
                }
            }
            for gstmt in ghost_update_stmts {
                warn!("issue_update_dt_stmt: {}", gstmt);
                txn.query_drop(format!("{}", gstmt.to_string()))?;
                self.stats.nqueries+=1;
            }
            self.update_uid2gids_with(&ghost_update_pairs)?;
        }
        let update_stmt = Statement::Update(UpdateStatement{
            table_name: stmt.table_name.clone(),
            assignments : qt_assn,
            selection : qt_selection,
        });
        warn!("issue_update_dt_stmt: {}", update_stmt);
        txn.query_drop(update_stmt.to_string())?;
        self.stats.nqueries+=1;
        Ok(())
    }
    
    fn issue_delete_datatable_stmt(&mut self, stmt: DeleteStatement, txn: &mut mysql::Transaction)
        -> Result<(), mysql::Error> 
    {        
        let ucols = helpers::get_user_cols_of_datatable(&self.cfg, &stmt.table_name);
        let qt_selection = self.selection_to_datatable_selection(&stmt.selection, txn, &stmt.table_name, &ucols)?;

        let ucol_selectitems = ucols.iter()
            .map(|uc| SelectItem::Expr{
                expr: Expr::Identifier(helpers::string_to_idents(uc)),
                alias: None,
            })
            .collect();
       
        // get the list of GIDs to delete from the ghosts table 
        let select_gids_stmt = Statement::Select(SelectStatement {
                query: Box::new(Query::select(Select{
                        distinct: true,
                        projection: ucol_selectitems,
                        from: vec![TableWithJoins{
                            relation: TableFactor::Table{
                                name: stmt.table_name.clone(),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: qt_selection.clone(),
                        group_by: vec![],
                        having: None,
                    })),        
                as_of: None,
        });
        warn!("issue_delete_dt_stmt: {}", select_gids_stmt);
        let res = txn.query_iter(format!("{}", select_gids_stmt.to_string()))?;
        self.stats.nqueries+=1;

        let mut gids_list : Vec<Expr>= vec![];
        let mut ghost_update_pairs: Vec<(Option<u64>, u64)>= vec![];
        for row in res {
            for val in row.unwrap().unwrap() {
                gids_list.push(Expr::Value(helpers::mysql_val_to_parser_val(&val)));
                ghost_update_pairs.push((None, helpers::mysql_val_to_u64(&val)?));
            }
        }

        // delete from ghosts table if GIDs are removed
        // TODO we might want to keep this around if data is "restorable"
        if !ucols.is_empty() {
            let ghosts_delete_statement = Statement::Delete(DeleteStatement{
                table_name: helpers::string_to_objname(&super::GHOST_TABLE_NAME),
                selection: Some(Expr::InList{
                    expr: Box::new(Expr::Identifier(helpers::string_to_idents(&super::GHOST_ID_COL))),
                    list: gids_list,
                    negated: false,
                }),
            });
            warn!("issue_delete_dt_stmt: {}", ghosts_delete_statement);
            txn.query_drop(&ghosts_delete_statement.to_string())?;
            self.stats.nqueries+=1;
        }
        self.update_uid2gids_with(&ghost_update_pairs)?;

        let delete_stmt = Statement::Delete(DeleteStatement{
            table_name: stmt.table_name.clone(),
            selection : qt_selection,
        });
        warn!("issue_delete_dt_stmt: {}", delete_stmt);
        txn.query_drop(delete_stmt.to_string())?;
        self.stats.nqueries+=1;
        Ok(())
    }
}
