use sql_parser::ast::*;
use std::*;
use super::config;
use super::helpers;

const dt_SUFFIX : &'static str = "mv"; 
pub struct DataTableTransformer {
    table_names: Vec<String>,
    cfg: config::Config,
}

impl DataTableTransformer {
    pub fn new(cfg: config::Config) -> Self {
        let mut table_names = Vec::<String>::new();
        table_names.push(cfg.user_table.name.clone());
        for dt in &cfg.data_tables {
            table_names.push(dt.name.clone());
        }
        DataTableTransformer{table_names, cfg}
    }   
    
    fn get_user_cols_of_table(&self, table_name: String) -> Option<&Vec<String>> {
         for dt in &self.cfg.data_tables {
             if table_name == dt.name {
                 return Some(&dt.user_cols);
             }
         }
         None
    }
   
    fn objname_to_datatable_string(&self, obj: &ObjectName) -> String {
        let obj_mv = ObjectName(self.idents_to_datatable_idents(&obj.0));
        obj_mv.to_string()
    }

    fn objname_to_datatable_objname(&self, obj: &ObjectName) -> ObjectName {
        ObjectName(self.idents_to_datatable_idents(&obj.0))
    }
    
    fn idents_to_datatable_idents(&self, obj: &Vec<Ident>) -> Vec<Ident> {
        // note that we assume that the name specified in the config
        // is the minimum needed to identify the data table.
        // if there are duplicates, the database/schema would also
        // need to be present as well. however, we allow for overspecifying
        // in the query (so the data table name in the config may be a 
        // subset of the query name).
        
        let mut objs_mv = obj.clone();
        for dt in &self.table_names {
            let dt_split : Vec<&str> = dt.split(".").collect();
          
            let mut i = 0;
            let mut j = 0;
            while j < obj.len() {
                if i < dt_split.len() {
                    if dt_split[i] == obj[j].to_string() {
                        i+=1;
                    } else {
                        // reset comparison from beginning of dt
                        i = 0; 
                    }
                    j+=1;
                } else {
                    break;
                }
            }
            if i == dt_split.len() {
                objs_mv.clear();
                for (index, ident) in obj.iter().enumerate() {
                    if index == j-1 && i == dt_split.len() {
                        // we found a match
                        objs_mv.push(Ident::new(&format!("{}{}", ident, dt_SUFFIX)));
                    } else {
                        objs_mv.push(ident.clone());
                    }
                } 
                break;
            }
        }
        objs_mv
    }

    /********************************************************
     * Processing statements to use materialized views      
     * ******************************************************/
    fn tablefactor_to_datatable_tablefactor(&self, tf: &TableFactor) -> TableFactor {
        match tf {
            TableFactor::Table {
                name,
                alias,
            } => {
                let dt_table_name = self.objname_to_datatable_string(&name);
                TableFactor::Table{
                    name: helpers::string_to_objname(&dt_table_name),
                    alias: alias.clone(),
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => TableFactor::Derived {
                    lateral: *lateral,
                    subquery: Box::new(self.query_to_datatable_query(&subquery)),
                    alias: alias.clone(),
                },
            TableFactor::NestedJoin {
                join,
                alias,
            } => TableFactor::NestedJoin{
                    join: Box::new(self.tablewithjoins_to_datatable_tablewithjoins(&join)),
                    alias: alias.clone(),
                },
            _ => tf.clone(),
        }
    }

    fn joinoperator_to_datatable_joinoperator(&self, jo: &JoinOperator) -> JoinOperator {
        let jo_mv : JoinOperator;
        match jo {
            JoinOperator::Inner(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::Inner(JoinConstraint::On(self.expr_to_datatable_expr(e))),
            JoinOperator::LeftOuter(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::LeftOuter(JoinConstraint::On(self.expr_to_datatable_expr(e))),
            JoinOperator::RightOuter(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::RightOuter(JoinConstraint::On(self.expr_to_datatable_expr(e))),
            JoinOperator::FullOuter(JoinConstraint::On(e)) => 
                jo_mv = JoinOperator::FullOuter(JoinConstraint::On(self.expr_to_datatable_expr(e))),
            _ => jo_mv = jo.clone(),
        }
        jo_mv
    }

    fn tablewithjoins_to_datatable_tablewithjoins(&self, twj: &TableWithJoins) -> TableWithJoins {
        TableWithJoins {
            relation: self.tablefactor_to_datatable_tablefactor(&twj.relation),
            joins: twj.joins
                .iter()
                .map(|j| Join {
                    relation: self.tablefactor_to_datatable_tablefactor(&j.relation),
                    join_operator: self.joinoperator_to_datatable_joinoperator(&j.join_operator),
                })
                .collect(),
        }
    }

    fn setexpr_to_datatable_setexpr(&self, setexpr: &SetExpr) -> SetExpr {
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
    }

    fn query_to_datatable_query(&self, query: &Query) -> Query {
        //TODO inefficient to clone and then replace?
        let mut dt_query = query.clone(); 

        let mut cte_dt_query : Query;
        for cte in &mut dt_query.ctes {
            cte_dt_query = self.query_to_datatable_query(&cte.query);
            cte.query = cte_dt_query;
        }

        dt_query.body = self.setexpr_to_datatable_setexpr(&query.body);

        let mut dt_oexpr : Expr;
        for orderby in &mut dt_query.order_by {
            dt_oexpr = self.expr_to_datatable_expr(&orderby.expr);
            orderby.expr = dt_oexpr;
        }

        if let Some(e) = &query.limit {
            dt_query.limit = Some(self.expr_to_datatable_expr(&e));
        }

        if let Some(e) = &query.offset {
            dt_query.offset = Some(self.expr_to_datatable_expr(&e));
        }       

        if let Some(f) = &mut dt_query.fetch {
            if let Some(e) = &f.quantity {
                let new_quantity = Some(self.expr_to_datatable_expr(&e));
                f.quantity = new_quantity;
            }
        }

        dt_query
    }
 
    fn expr_to_datatable_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::Identifier(ids) => Expr::Identifier(self.idents_to_datatable_idents(&ids)),
            Expr::QualifiedWildcard(ids) => Expr::QualifiedWildcard(self.idents_to_datatable_idents(&ids)),
            Expr::FieldAccess {
                expr,
                field,
            } => Expr::FieldAccess {
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                field: field.clone(),
            },
            Expr::WildcardAccess(e) => Expr::WildcardAccess(Box::new(self.expr_to_datatable_expr(&e))),
            Expr::IsNull{
                expr,
                negated,
            } => Expr::IsNull {
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                negated: *negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                list: list
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e))
                    .collect(),
                negated: *negated,
            },
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                subquery: Box::new(self.query_to_datatable_query(&subquery)),
                negated: *negated,
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                negated: *negated,
                low: Box::new(self.expr_to_datatable_expr(&low)),
                high: Box::new(self.expr_to_datatable_expr(&high)),
            },
            Expr::BinaryOp{
                left,
                op,
                right
            } => Expr::BinaryOp{
                left: Box::new(self.expr_to_datatable_expr(&left)),
                op: op.clone(),
                right: Box::new(self.expr_to_datatable_expr(&right)),
            },
            Expr::UnaryOp{
                op,
                expr,
            } => Expr::UnaryOp{
                op: op.clone(),
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
            },
            Expr::Cast{
                expr,
                data_type,
            } => Expr::Cast{
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                data_type: data_type.clone(),
            },
            Expr::Collate {
                expr,
                collation,
            } => Expr::Collate{
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                collation: self.objname_to_datatable_objname(&collation),
            },
            Expr::Nested(expr) => Expr::Nested(Box::new(self.expr_to_datatable_expr(&expr))),
            Expr::Row{
                exprs,
            } => Expr::Row{
                exprs: exprs
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e))
                    .collect(),
            },
            Expr::Function(f) => Expr::Function(Function{
                name: self.objname_to_datatable_objname(&f.name),
                args: match &f.args {
                    FunctionArgs::Star => FunctionArgs::Star,
                    FunctionArgs::Args(exprs) => FunctionArgs::Args(exprs
                        .iter()
                        .map(|e| self.expr_to_datatable_expr(&e))
                        .collect()),
                },
                filter: match &f.filter {
                    Some(filt) => Some(Box::new(self.expr_to_datatable_expr(&filt))),
                    None => None,
                },
                over: match &f.over {
                    Some(ws) => Some(WindowSpec{
                        partition_by: ws.partition_by
                            .iter()
                            .map(|e| self.expr_to_datatable_expr(&e))
                            .collect(),
                        order_by: ws.order_by
                            .iter()
                            .map(|obe| OrderByExpr {
                                expr: self.expr_to_datatable_expr(&obe.expr),
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
                    Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e))),
                    None => None,
                },
                conditions: conditions
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e))
                    .collect(),
                results:results
                    .iter()
                    .map(|e| self.expr_to_datatable_expr(&e))
                    .collect(),
                else_result: match else_result {
                    Some(e) => Some(Box::new(self.expr_to_datatable_expr(&e))),
                    None => None,
                },
            },
            Expr::Exists(q) => Expr::Exists(Box::new(self.query_to_datatable_query(&q))),
            Expr::Subquery(q) => Expr::Subquery(Box::new(self.query_to_datatable_query(&q))),
            Expr::Any {
                left,
                op,
                right,
            } => Expr::Any {
                left: Box::new(self.expr_to_datatable_expr(&left)),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right)),
            },
            Expr::All{
                left,
                op,
                right,
            } => Expr::All{
                left: Box::new(self.expr_to_datatable_expr(&left)),
                op: op.clone(),
                right: Box::new(self.query_to_datatable_query(&right)),
            },
            Expr::List(exprs) => Expr::List(exprs
                .iter()
                .map(|e| self.expr_to_datatable_expr(&e))
                .collect()),
            Expr::SubscriptIndex {
                expr,
                subscript,
            } => Expr::SubscriptIndex{
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                subscript: Box::new(self.expr_to_datatable_expr(&subscript)),
            },
            Expr::SubscriptSlice{
                expr,
                positions,
            } => Expr::SubscriptSlice{
                expr: Box::new(self.expr_to_datatable_expr(&expr)),
                positions: positions
                    .iter()
                    .map(|pos| SubscriptPosition {
                        start: match &pos.start {
                            Some(e) => Some(self.expr_to_datatable_expr(&e)),
                            None => None,
                        },
                        end: match &pos.end {
                            Some(e) => Some(self.expr_to_datatable_expr(&e)),
                                None => None,
                            },
                        })
                    .collect(),
            },
            _ => expr.clone(),
        }
    }
    
    pub fn stmt_to_datatable_stmt(&mut self, stmt: &Statement) -> Statement {
        let dt_stmt : Statement;
        let dt_table_name : String;

        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                let new_q = self.query_to_datatable_query(&query);
                dt_stmt = Statement::Select(SelectStatement{
                    query: Box::new(new_q), 
                    as_of: as_of.clone(),
                });
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                dt_table_name = self.objname_to_datatable_string(&table_name);
                let mut dt_source = source.clone();
                // update sources
                match source {
                    InsertSource::Query(q) => {
                        dt_source = InsertSource::Query(Box::new(self.query_to_datatable_query(&q)));
                    } 
                    InsertSource::DefaultValues => (), // TODO might have to get rid of this
                }
                dt_stmt = Statement::Insert(InsertStatement{
                    table_name: helpers::string_to_objname(&dt_table_name),
                    columns : columns.clone(),
                    source : dt_source, 
                });
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                dt_table_name = self.objname_to_datatable_string(&table_name);
                let mut dt_assn = Vec::<Assignment>::new();
                let mut dt_selection = selection.clone();
                // update assignments
                for a in assignments {
                    dt_assn.push(Assignment{
                        id : a.id.clone(),
                        value: self.expr_to_datatable_expr(&a.value),
                    });
                }
                // update selection 
                match selection {
                    None => (),
                    Some(s) => dt_selection = Some(self.expr_to_datatable_expr(&s)),
                }
                dt_stmt = Statement::Update(UpdateStatement{
                    table_name: helpers::string_to_objname(&dt_table_name),
                    assignments : dt_assn,
                    selection : dt_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                dt_table_name = self.objname_to_datatable_string(&table_name);
                let mut dt_selection = selection.clone();
                // update selection 
                match selection {
                    None => (),
                    Some(s) => dt_selection = Some(self.expr_to_datatable_expr(&s)),
                }
                dt_stmt = Statement::Delete(DeleteStatement{
                    table_name: helpers::string_to_objname(&dt_table_name),
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
                let dt_query = self.query_to_datatable_query(&query);
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
                dt_table_name = self.objname_to_datatable_string(&name);
                let dt_constraints = constraints
                    .iter()
                    .map(|c| match c {
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                        } => {
                            let mut foreign_table = self.objname_to_datatable_string(foreign_table);
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
                dt_stmt = Statement::CreateTable(CreateTableStatement{
                    name: helpers::string_to_objname(&dt_table_name),
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
                dt_table_name = self.objname_to_datatable_string(&on_name);
                dt_stmt = Statement::CreateIndex(CreateIndexStatement{
                    name: name.clone(),
                    on_name: helpers::string_to_objname(&dt_table_name),
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
                let mut to_item_dt_name = to_item_name.to_string();
                dt_table_name= self.objname_to_datatable_string(&name);
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        if dt_table_name != name.to_string() {
                            // change config to reflect new table name
                            self.table_names.push(to_item_name.to_string());
                            self.table_names.retain(|x| *x != *name.to_string());
                            if self.cfg.user_table.name == name.to_string() {
                                self.cfg.user_table.name = to_item_name.to_string();
                            } else {
                                for tab in &mut self.cfg.data_tables {
                                    if tab.name == name.to_string() {
                                        tab.name = to_item_name.to_string();
                                    }
                                }
                            }
                            to_item_dt_name = format!("{}{}", to_item_name, dt_SUFFIX);
                        }
                    }
                    _ => (),
                }
                dt_stmt = Statement::AlterObjectRename(AlterObjectRenameStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    name: helpers::string_to_objname(&dt_table_name),
                    to_item_name: Ident::new(to_item_dt_name),
                });
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
                let mut dt_names = names.clone();
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        for name in &mut dt_names {
                            let newname = self.objname_to_datatable_string(&name);
                            *name = helpers::string_to_objname(&newname);
                        }
                    }
                    _ => (),
                }
                dt_stmt = Statement::DropObjects(DropObjectsStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    names: dt_names,
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
                let mut dt_from = from.clone();
                if let Some(f) = from {
                    dt_from = Some(helpers::string_to_objname(&self.objname_to_datatable_string(&f)));
                }

                let mut dt_filter = filter.clone();
                if let Some(f) = filter {
                    match f {
                        ShowStatementFilter::Like(_s) => (),
                        ShowStatementFilter::Where(expr) => {
                            dt_filter = Some(ShowStatementFilter::Where(self.expr_to_datatable_expr(&expr)));
                        }
                    }
                }
                dt_stmt = Statement::ShowObjects(ShowObjectsStatement{
                    object_type: object_type.clone(),
                    from: dt_from,
                    extended: *extended,
                    full: *full,
                    materialized: *materialized,
                    filter: dt_filter,
                })
            }
            // XXX TODO should indexes be created in both the 
            // MV and the data table? (if data is only ever read from MV)
            Statement::ShowIndexes(ShowIndexesStatement{
                table_name,
                extended,
                filter,
            }) => {
                dt_table_name = self.objname_to_datatable_string(&table_name);
                let mut dt_filter = filter.clone();
                if let Some(f) = filter {
                    match f {
                        ShowStatementFilter::Like(_s) => (),
                        ShowStatementFilter::Where(expr) => {
                            dt_filter = Some(ShowStatementFilter::Where(self.expr_to_datatable_expr(&expr)));
                        }
                    }
                }
                dt_stmt = Statement::ShowIndexes(ShowIndexesStatement {
                    table_name: helpers::string_to_objname(&dt_table_name),
                    extended: *extended,
                    filter: dt_filter,
                });
            }
            /* TODO Handle Statement::Explain(stmt) => f.write_node(stmt)
             *
             * Don't handle CreateSink, CreateSource, Copy,
             *  ShowCreateSource, ShowCreateSink, Tail, Explain
             * 
             * Don't modify queries for CreateSchema, CreateDatabase, 
             * ShowDatabases, ShowCreateTable, DropDatabase, Transactions,
             * ShowColumns, SetVariable
             *
             * XXX: ShowVariable, ShowCreateView and ShowCreateIndex will return 
             *  queries that used the materialized views, rather than the 
             *  application-issued tables. This is probably not a big issue, 
             *  since these queries are used to create the table again?
             * */
            _ => {
                dt_stmt = stmt.clone();
            }
        }
        dt_stmt
    }
}
