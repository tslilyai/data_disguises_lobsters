use sql_parser::ast::*;
use super::config;
use super::helpers;

static mut LATEST_GID: u64 = super::GHOST_ID_START;

pub struct DataTableTransformer {
    cfg: config::Config,
}

impl DataTableTransformer {
    pub fn new(cfg: config::Config) -> Self {
        DataTableTransformer{cfg}
    }   
    
    fn get_user_cols_of_table_stmt(&self, table_name: &Vec<Ident>, cols: &Vec<Ident>) -> Vec<Ident> {
        let mut users = cols.clone();
        for dt in &self.cfg.data_tables {
            if let Some(_p) = helpers::objname_subset_match_range(table_name, &dt.name) {
                users.retain(|c| dt.user_cols.iter().any(|uc| c.to_string() == *uc));
                break;
            }
        }
        users
    }
    
    fn tablefactor_to_datatable_tablefactor(&self, tf: &TableFactor) -> TableFactor {
        match tf {
            TableFactor::Table {
                name,
                alias,
            } => {
                TableFactor::Table{
                    name: name.clone(),
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
                collation: collation.clone(),
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
                name: f.name.clone(),
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
    
    pub fn stmt_to_datatable_stmt(&mut self, stmt: &Statement) -> Option<Statement> {
        let mut is_write = false;
        let mut dt_stmt = stmt.clone();

        match stmt {
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                is_write = true;

                /* note that if the table is the users table,
                 * we just want to insert like usual; we only care about
                 * adding ghost ids for data tables, but we don't add ghosts to
                 * the user table
                 */

                // for all columns that are user columns, generate a new ghost_id and insert
                // into ghosts table with appropriate user_id value
                // those as the values instead for those columns.
                let ucols = self.get_user_cols_of_table_stmt(&table_name.0, columns);
                for uc in ucols {
                    
                }
                // TODO need to issue multiple statements 
                // to update values of user cols to ghost
                // ids

                let mut dt_source = source.clone();
                // update sources
                match source {
                    InsertSource::Query(q) => {
                        dt_source = InsertSource::Query(Box::new(self.query_to_datatable_query(&q)));
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
                is_write = true;
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
                    table_name: table_name.clone(),
                    assignments : dt_assn,
                    selection : dt_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                is_write = true;
                let mut dt_selection = selection.clone();
                // update selection 
                match selection {
                    None => (),
                    Some(s) => dt_selection = Some(self.expr_to_datatable_expr(&s)),
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
                is_write = true;
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
                is_write = true;
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
                is_write = true;
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
                is_write = true;
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
                is_write = true;
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
        if is_write {
            return Some(dt_stmt);
        } else {
            return None;
        }
    }
}
