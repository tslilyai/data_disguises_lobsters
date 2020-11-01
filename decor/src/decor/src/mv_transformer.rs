use sql_parser::ast::*;
use super::{helpers, config};
use std::*;
//use lru::LruCache;
use std::collections::HashMap;

const CACHE_SZ : usize = 64;

pub struct MVTransformer {
    pub cfg: config::Config,
    q2mvq: HashMap<Query, Query>,
    //q2mvq: LruCache<Query, Query>,
}

/********************************************************
 * Processing statements to use materialized views      
 * ******************************************************/
impl MVTransformer {
    pub fn new(cfg: &config::Config) -> Self {
        MVTransformer{
            cfg: cfg.clone(),
            q2mvq: HashMap::new(),
            //q2mvq: LruCache::new(CACHE_SZ),
        }
    }   
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
        for dt in &self.cfg.data_tables {
            if let Some((_start, end)) = helpers::str_subset_of_idents(&dt.name, obj) {
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

    fn tablefactor_to_mv_tablefactor(&mut self, tf: &TableFactor) -> TableFactor {
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

    fn joinoperator_to_mv_joinoperator(&mut self, jo: &JoinOperator) -> JoinOperator {
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

    fn tablewithjoins_to_mv_tablewithjoins(&mut self, twj: &TableWithJoins) -> TableWithJoins {
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

    fn setexpr_to_mv_setexpr(&mut self, setexpr: &SetExpr) -> SetExpr {
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

    pub fn query_to_mv_query(&mut self, query: &Query) -> Query {
        if let Some(mvq) = self.q2mvq.get(query) {
            return mvq.clone();
        }

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
        self.q2mvq.insert(query.clone(), mv_query.clone());
        //self.q2mvq.put(query.clone(), mv_query.clone());

        mv_query
    }
 
    pub fn expr_to_mv_expr(&mut self, expr: &Expr) -> Expr {
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

    pub fn try_get_simple_mv_stmt (
        &mut self, 
        in_memory: bool, 
        stmt: &Statement)
        -> Result<Option<Statement>, mysql::Error>
    {
        let mv_stmt : Statement;
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
                ..
            }) => {
                // if it's a datatable or the user table , we need to perform more complex transformations
                if self.objname_to_mv_string(&table_name) != table_name.to_string() 
                    || table_name.to_string() == self.cfg.user_table.name 
                {
                    return Ok(None);
                }
                mv_stmt = stmt.clone();
            }
            Statement::Update(UpdateStatement{
                table_name,
                ..
            }) => {
                if self.objname_to_mv_string(&table_name) != table_name.to_string() 
                    || table_name.to_string() == self.cfg.user_table.name 
                {                
                    return Ok(None);
                }
                mv_stmt = stmt.clone();
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                if mv_table_name != table_name.to_string() 
                    || table_name.to_string() == self.cfg.user_table.name 
                {
                    return Ok(None);
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
                if self.objname_to_mv_string(&name) != name.to_string() 
                    || name.to_string() == self.cfg.user_table.name 
                {
                    return Ok(None);
                }

                let mut new_engine = engine.clone();
                if in_memory {
                    new_engine = Some(Engine::Memory);
                }

                mv_stmt = Statement::CreateTable(CreateTableStatement{
                    name: name.clone(),
                    columns: columns.clone(),
                    constraints: constraints.clone(),
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
                if self.objname_to_mv_string(&on_name) != on_name.to_string() {
                    return Ok(None);
                }

                mv_stmt = stmt.clone();
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
                let mut to_item_mv_name = to_item_name.to_string();
                mv_table_name = self.objname_to_mv_string(&name);
                if mv_table_name != name.to_string() {
                    return Ok(None);
                }
                
                match object_type {
                    ObjectType::Table => {
                        to_item_mv_name = format!("{}{}", to_item_name, super::MV_SUFFIX);
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
                let mut is_dt_write = false;
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        for name in &mut mv_names {
                            let newname = self.objname_to_mv_string(&name);
                            is_dt_write |= newname != name.to_string();

                            if is_dt_write {
                                return Ok(None);
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
        Ok(Some(mv_stmt))
    }
}
