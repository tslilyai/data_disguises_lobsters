extern crate mysql;
use msql_srv::*;
use mysql::prelude::*;
use sql_parser::parser::parse_statements;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::*;
mod config;

const MV_SUFFIX : &'static str = "mv"; 
const GHOST_ID_START : i64 = 1<<20;

fn create_ghosts_query() -> String {
    return format!(
        r"CREATE TABLE IF NOT EXISTS `ghosts` (
            `ghost_id` int unsigned NOT NULL AUTO_INCREMENT = {},
            `user_id` int unsigned);", 
        GHOST_ID_START);
}

fn trim_quotes(s: &str) -> &str {
    let mut s = s;
    if s.ends_with('"') && s.starts_with('"') {
        s = &s[1..s.len() - 1]
    } 
    s
}

fn string_to_objname(s: &str) -> ObjectName {
    let idents = s
        .split(".")
        .into_iter()
        .map(|i| Ident::new(trim_quotes(i)))
        .collect();
    let obj = ObjectName(idents);
    obj
}

struct Prepared {
    stmt: mysql::Statement,
    params: Vec<Column>,
}

pub struct Shim { 
    db: mysql::Conn,
    prepared: HashMap<u32, Prepared>,
    
    cfg: config::Config,
    table_names: Vec<String>,
    // NOTE: not *actually* static, but tied to our connection's lifetime.
    schema: &'static str,
}

impl Drop for Shim {
    fn drop(&mut self) {
        self.prepared.clear();
        // drop the connection (implicitly done).
    }
}

impl Shim {
    pub fn new(db: mysql::Conn, cfg_json: &str, schema: &'static str) -> Self {
        let cfg = config::parse_config(cfg_json).unwrap();
        let mut table_names = Vec::<String>::new();
        table_names.push(cfg.user_table.name.clone());
        for dt in &cfg.data_tables {
            table_names.push(dt.name.clone());
        }
        let prepared = HashMap::new();
        Shim{db, cfg, table_names, prepared, schema}
    }   

    /* 
     * Set all user_ids in the ghosts table to NULL
     * refresh "materialized views"
     */
    pub fn unsubscribe() -> bool {
        false
    }

    /* 
     * Set all user_ids in the ghosts table to specified user 
     * refresh "materialized views"
     * TODO add back deleted content from shard
     */
    pub fn resubscribe() -> bool {
        false
    }

    /* 
     * Given schema in sql, issue queries to set up database.
     * Must be issued after select_db statement is issued.
     * */
    fn create_schema(&mut self) -> Result<(), mysql::Error> {
        let mut current_q = String::new();
        for line in self.schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !current_q.is_empty() {
                current_q.push_str(" ");
            }
            current_q.push_str(line);
            if current_q.ends_with(';') {
                self.db.query_drop(&current_q).unwrap();
                println!("Query executed: {}", current_q);
                current_q.clear();
            }
        }
        Ok(())
    }

    fn objname_to_mv_string(&self, obj: &ObjectName) -> String {
        let obj_mv = ObjectName(self.idents_to_mv_idents(&obj.0));
        obj_mv.to_string()
    }

    fn objname_to_mv_objname(&self, obj: &ObjectName) -> ObjectName {
        ObjectName(self.idents_to_mv_idents(&obj.0))
    }
    
    fn idents_to_mv_idents(&self, obj: &Vec<Ident>) -> Vec<Ident> {
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
                        objs_mv.push(Ident::new(&format!("{}{}", ident, MV_SUFFIX)));
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
    fn tablefactor_to_mv_tablefactor(&self, tf: &TableFactor) -> TableFactor {
        match tf {
            TableFactor::Table {
                name,
                alias,
            } => {
                let mv_table_name = self.objname_to_mv_string(&name);
                TableFactor::Table{
                    name: string_to_objname(&mv_table_name),
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

    fn query_to_mv_query(&self, query: &Query) -> Query {
        //TODO inefficient to clone and then replace?
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
 
    fn expr_to_mv_expr(&self, expr: &Expr) -> Expr {
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
    
    fn stmt_to_mv_stmt(&mut self, stmt: &Statement) -> Statement {
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
                });
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                let mut mv_source = source.clone();
                // update sources
                match source {
                    InsertSource::Query(q) => {
                        mv_source = InsertSource::Query(Box::new(self.query_to_mv_query(&q)));
                    } 
                    InsertSource::DefaultValues => (), // TODO might have to get rid of this
                }
                mv_stmt = Statement::Insert(InsertStatement{
                    table_name: string_to_objname(&mv_table_name),
                    columns : columns.clone(),
                    source : mv_source, 
                });
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
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
                    table_name: string_to_objname(&mv_table_name),
                    assignments : mv_assn,
                    selection : mv_selection,
                });
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                mv_table_name = self.objname_to_mv_string(&table_name);
                let mut mv_selection = selection.clone();
                // update selection 
                match selection {
                    None => (),
                    Some(s) => mv_selection = Some(self.expr_to_mv_expr(&s)),
                }
                mv_stmt = Statement::Delete(DeleteStatement{
                    table_name: string_to_objname(&mv_table_name),
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
                let mv_constraints = constraints
                    .iter()
                    .map(|c| match c {
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                        } => {
                            let mut foreign_table = self.objname_to_mv_string(foreign_table);
                            TableConstraint::ForeignKey{
                                name: name.clone(),
                                columns: columns.clone(),
                                foreign_table: string_to_objname(&foreign_table),
                                referred_columns: referred_columns.clone(),
                            }
                        }
                        _ => c.clone(),
                    })
                    .collect(); 
                mv_stmt = Statement::CreateTable(CreateTableStatement{
                    name: string_to_objname(&mv_table_name),
                    columns: columns.clone(),
                    constraints: mv_constraints,
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
                mv_table_name = self.objname_to_mv_string(&on_name);
                mv_stmt = Statement::CreateIndex(CreateIndexStatement{
                    name: name.clone(),
                    on_name: string_to_objname(&mv_table_name),
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
                match object_type {
                    ObjectType::Table => {
                        // update name(s)
                        if mv_table_name != name.to_string() {
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
                            to_item_mv_name = format!("{}{}", to_item_name, MV_SUFFIX);
                        }
                    }
                    _ => (),
                }
                mv_stmt = Statement::AlterObjectRename(AlterObjectRenameStatement{
                    object_type: object_type.clone(),
                    if_exists: *if_exists,
                    name: string_to_objname(&mv_table_name),
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
                            *name = string_to_objname(&newname);
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
                    mv_from = Some(string_to_objname(&self.objname_to_mv_string(&f)));
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
            // XXX TODO should indexes be created in both the 
            // MV and the data table? (if data is only ever read from MV)
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
                    table_name: string_to_objname(&mv_table_name),
                    extended: *extended,
                    filter: mv_filter,
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
                mv_stmt = stmt.clone();
            }
        }
        mv_stmt
    }

    fn query_to_datatable_query(&self, query: &Query) -> Query {
        query.clone()
    }
 
    fn expr_to_datatable_expr(&self, expr: &Expr) -> Expr {
        expr.clone()
    }

    fn stmt_to_datatable_stmt(&mut self, stmt: &Statement) -> Statement
    {
        match stmt {
            Statement::Insert {..} => {
                // we want to insert into both the MV and the data table
                // and to insert a unique ghost_id in place of the user_id 
                // 1. check if this table even has user_ids that we need to replace
                let user_cols : Vec<String> = Vec::<String>::new();
                /*match self.get_user_cols_of(table_name.to_string()) {
                    Some(uc) => user_cols = uc.clone(),
                    None => (),
                }*/
                //for (i, c) in columns.iter().enumerate() {
                    // 2. get param value of user_id columns
                    // 3. insert user_id val into the ghosts table
                    //    as a new ghost_id entry
                    //let user_id_q = format!("{:?}", value_exprs[i]);
                    //self.db.query_drop(format!("INSERT INTO ghosts (user_id) VALUES ({});", 
                    //self.query_using_mv_tables(&user_id_q)))?;
                    // 4. get the ghost_id field of the new entry 
                    //let res = self.db.query_iter("SELECT LAST_INSERT_ID()")?;
                    //match res.last_insert_id() {
                        //None => Ok(()), // TODO return error 
                        /*return Ok(results.error(
                                ErrorKind::ER_INTERNAL_ERROR, 
                                b"call to last insert ID failed")?),*/
                        //Some(ghost_id) => {
                            // 5. replace user_id value in query with ghost_id
                            // XXX this seems brittle (could replace more than once? would that be problematic?)
                        //}
                    //}
                //}
                // 5. issue the MODIFIED query to the data table (err if error)
            }
            Statement::Update{..} => {
            }
            Statement::Delete{..} => {
            }
            _ => ()
        }
        stmt.clone()
    }

    fn get_user_cols_of_table(&self, table_name: String) -> Option<&Vec<String>> {
         for dt in &self.cfg.data_tables {
             if table_name == dt.name {
                 return Some(&dt.user_cols);
             }
         }
         None
    }

    // TODO factor out conversions to make tests more organized #[cfg(test)]
    pub fn stmt_to_mv_stmt_test(&mut self, stmt: &Statement) -> Statement {
        self.stmt_to_mv_stmt(stmt)
    }
}

impl<W: io::Write> MysqlShim<W> for Shim {
    type Error = mysql::Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> Result<(), Self::Error> {
        // TODO save prepared stmts modified for MVs and ghosts table
        /*match self.db.prep(self.query_using_mv_tables(query)) {
            Ok(stmt) => {
                let params: Vec<_> = stmt
                    .params()
                    .into_iter()
                    .map(|p| {
                        Column {
                            table: p.table_str().to_string(),
                            column: p.name_str().to_string(),
                            coltype: get_coltype(&p.column_type()),
                            colflags: ColumnFlags::from_bits(p.flags().bits()).unwrap(),
                        }
                    })
                    .collect();
                let columns: Vec<_> = stmt
                    .columns()
                    .into_iter()
                    .map(|c| {
                        Column {
                            table: c.table_str().to_string(),
                            column: c.name_str().to_string(),
                            coltype: get_coltype(&c.column_type()),
                            colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                        }
                    })
                    .collect();
                info.reply(stmt.id(), &params, &columns)?;
                self.prepared.insert(stmt.id(), Prepared{stmt: stmt.clone(), params});
            },
            Err(e) => {
                match e {
                    mysql::Error::MySqlError(merr) => {
                        info.error(ErrorKind::ER_NO, merr.message.as_bytes())?;
                    },
                    _ => return Err(e),
                }
            }
        }*/
        Ok(())
    }
    
    fn on_execute(
        &mut self,
        id: u32,
        ps: ParamParser,
        results: QueryResultWriter<W>,
    ) -> Result<(), Self::Error> {
        match self.prepared.get(&id) {
            None => return Ok(results.error(ErrorKind::ER_NO, b"no such prepared statement")?),
            Some(prepped) => {
                // parse params
                let args : Vec<mysql::Value> = ps
                    .into_iter()
                    .map(|p| match p.value.into_inner() {
                        msql_srv::ValueInner::NULL => {
                            mysql::Value::NULL
                        }
                        ValueInner::Bytes(bs) => {
                            mysql::Value::Bytes(bs.to_vec())
                        }
                        ValueInner::Int(v) => {
                            mysql::Value::Int(v)
                        }
                        ValueInner::UInt(v) => {
                            mysql::Value::UInt(v)
                        }
                        ValueInner::Double(v) => {
                            mysql::Value::Float(v)
                        }
                        ValueInner::Date(bs) => {
                            assert!(bs.len() == 7);
                            mysql::Value::Date(bs[0].into(), bs[1].into(), bs[2], bs[3], bs[4], bs[5], bs[6].into())
                        }
                        ValueInner::Time(bs) => {
                            assert!(bs.len() == 6);
                            mysql::Value::Time(bs[0] == 0, bs[1].into(), bs[2], bs[3], bs[4], bs[5].into())
                        }
                        ct => unimplemented!("no translation for param type {:?}", ct)
                    }).collect();

                let res = self.db.exec_iter(
                    prepped.stmt.clone(), 
                    mysql::params::Params::Positional(args),
                );

                // TODO get response
                return Ok(());
                //answer_rows(results, self.db.query_iter(self.query_using_mv_tables("")))
            }
        }
    }
    
    fn on_close(&mut self, id: u32) {
        match self.prepared.get(&id) {
            None => return,
            Some(prepped) => {
                if let Err(e) = self.db.close(prepped.stmt.clone()){
                    eprintln!("{}", e);
                };
                self.prepared.remove(&id); 
            }
        }
    }

    fn on_init(&mut self, schema: &str, w: InitWriter<W>) -> Result<(), Self::Error> {
        println!("On init called!");
        let res = self.db.select_db(schema);
        if !res {
            w.error(ErrorKind::ER_BAD_DB_ERROR, b"select db failed")?;
            return Ok(());
        }   
        
        self.create_schema().unwrap();

        /* create ghost metadata table with boolean cols for each user id */
        let create_ghost_table_q = create_ghosts_query();
        // XXX temp: create a new ghost metadata table
        self.db.query_drop("DROP TABLE IF EXISTS ghosts;").unwrap();
        self.db.query_drop(create_ghost_table_q).unwrap();
        
        /* issue schema statements */
        let mut sql = String::new();
        let mut mv_stmt : Statement;
        let mut dt_stmt : Statement;
        for line in self.schema.lines() {
            if line.starts_with("--") || line.is_empty() {
                continue;
            }
            if !sql.is_empty() {
                sql.push_str(" ");
            }
            sql.push_str(line);
            if sql.ends_with(';') {
                sql.push_str("\n");
            }
        }

        let stmts = parse_statements(sql);
        match stmts {
            Err(e) => 
                Ok(w.error(ErrorKind::ER_BAD_DB_ERROR, &format!("{}", e).as_bytes())?),
            Ok(stmts) => {
                for stmt in stmts {
                    // issue actual statement to datatables (potentially creating ghost ID 
                    // entries as well)
                    dt_stmt = self.stmt_to_datatable_stmt(&stmt);
                    self.db.query_drop(dt_stmt.to_string())?;
                    
                    // issue statement to materialized views
                    mv_stmt = self.stmt_to_mv_stmt(&stmt);
                    self.db.query_drop(mv_stmt.to_string())?;
                }
                Ok(w.ok()?)
            }
        }
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<(), Self::Error> {
        let stmts_res = parse_statements(query.to_string());
        match stmts_res {
            Err(e) => {
                results.error(ErrorKind::ER_PARSE_ERROR, format!("{:?}", e).as_bytes())?;
                return Ok(());
            }
            Ok(stmts) => {
                assert!(stmts.len()==1);
                let datatable_stmt = self.stmt_to_datatable_stmt(&stmts[0]);
                let mv_stmt = self.stmt_to_mv_stmt(&stmts[0]);
                self.db.query_drop(format!("{}", datatable_stmt))?; 
                return answer_rows(results, self.db.query_iter(format!("{}", mv_stmt)));
            }
        }
    }
}

fn answer_rows<W: io::Write>(
    results: QueryResultWriter<W>,
    rows: mysql::Result<mysql::QueryResult<mysql::Text>>,
) -> Result<(), mysql::Error> 
{
    match rows {
        Ok(rows) => {
            let cols : Vec<_> = rows
                .columns()
                .as_ref()
                .into_iter()
                .map(|c| {
                    Column {
                    table : c.table_str().to_string(),
                    column : c.name_str().to_string(),
                    coltype : get_coltype(&c.column_type()),
                    colflags: ColumnFlags::from_bits(c.flags().bits()).unwrap(),
                }
            })
            .collect();
        let mut writer = results.start(&cols)?;
        for row in rows {
            let vals = row.unwrap();
            for (c, col) in cols.iter().enumerate() {
                match col.coltype {
                    ColumnType::MYSQL_TYPE_DECIMAL => writer.write_col(vals.get::<f64, _>(c))?,
                    ColumnType::MYSQL_TYPE_TINY => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_SHORT => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_LONG => writer.write_col(vals.get::<i32, _>(c))?,
                    ColumnType::MYSQL_TYPE_FLOAT => writer.write_col(vals.get::<f32, _>(c))?,
                    ColumnType::MYSQL_TYPE_DOUBLE => writer.write_col(vals.get::<f64, _>(c))?,
                    ColumnType::MYSQL_TYPE_NULL => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_LONGLONG => writer.write_col(vals.get::<i64, _>(c))?,
                    ColumnType::MYSQL_TYPE_INT24 => writer.write_col(vals.get::<i32, _>(c))?,
                    ColumnType::MYSQL_TYPE_VARCHAR => writer.write_col(vals.get::<String, _>(c))?,
                    ColumnType::MYSQL_TYPE_BIT => writer.write_col(vals.get::<i16, _>(c))?,
                    ColumnType::MYSQL_TYPE_TINY_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_MEDIUM_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_LONG_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_BLOB => writer.write_col(vals.get::<Vec<u8>, _>(c))?,
                    ColumnType::MYSQL_TYPE_VAR_STRING => writer.write_col(vals.get::<String, _>(c))?,
                    ColumnType::MYSQL_TYPE_STRING => writer.write_col(vals.get::<String, _>(c))?,
                    ColumnType::MYSQL_TYPE_GEOMETRY => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIMESTAMP => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_DATE => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIME => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_DATETIME => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_YEAR => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_NEWDATE => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIMESTAMP2 => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_DATETIME2 => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_TIME2 => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_JSON => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_NEWDECIMAL => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_ENUM => writer.write_col(vals.get::<i16, _>(c))?,
                    //ColumnType::MYSQL_TYPE_SET => writer.write_col(vals.get::<i16, _>(c))?,
                    ct => unimplemented!("Cannot translate row type {:?} into value", ct),
                }
            }
            writer.end_row()?;
        }
        writer.finish()?;
    }
    Err(e) => {
        results.error(ErrorKind::ER_BAD_SLAVE, format!("{:?}", e).as_bytes())?;
    }
}
Ok(())
}

/// Convert a MySQL type to MySQL_svr type 
fn get_coltype(t: &mysql::consts::ColumnType) -> ColumnType {
    match t {
        mysql::consts::ColumnType::MYSQL_TYPE_DECIMAL => ColumnType::MYSQL_TYPE_DECIMAL,
        mysql::consts::ColumnType::MYSQL_TYPE_TINY => ColumnType::MYSQL_TYPE_TINY,
        mysql::consts::ColumnType::MYSQL_TYPE_SHORT => ColumnType::MYSQL_TYPE_SHORT,
        mysql::consts::ColumnType::MYSQL_TYPE_LONG => ColumnType::MYSQL_TYPE_LONG,
        mysql::consts::ColumnType::MYSQL_TYPE_FLOAT => ColumnType::MYSQL_TYPE_FLOAT,
        mysql::consts::ColumnType::MYSQL_TYPE_DOUBLE => ColumnType::MYSQL_TYPE_DOUBLE,
        mysql::consts::ColumnType::MYSQL_TYPE_NULL => ColumnType::MYSQL_TYPE_NULL,
        mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP => ColumnType::MYSQL_TYPE_TIMESTAMP,
        mysql::consts::ColumnType::MYSQL_TYPE_LONGLONG => ColumnType::MYSQL_TYPE_LONGLONG,
        mysql::consts::ColumnType::MYSQL_TYPE_INT24 => ColumnType::MYSQL_TYPE_INT24,
        mysql::consts::ColumnType::MYSQL_TYPE_DATE => ColumnType::MYSQL_TYPE_DATE,
        mysql::consts::ColumnType::MYSQL_TYPE_TIME => ColumnType::MYSQL_TYPE_TIME,
        mysql::consts::ColumnType::MYSQL_TYPE_DATETIME => ColumnType::MYSQL_TYPE_DATETIME,
        mysql::consts::ColumnType::MYSQL_TYPE_YEAR => ColumnType::MYSQL_TYPE_YEAR,
        mysql::consts::ColumnType::MYSQL_TYPE_NEWDATE => ColumnType::MYSQL_TYPE_NEWDATE,
        mysql::consts::ColumnType::MYSQL_TYPE_VARCHAR => ColumnType::MYSQL_TYPE_VARCHAR,
        mysql::consts::ColumnType::MYSQL_TYPE_BIT => ColumnType::MYSQL_TYPE_BIT,
        mysql::consts::ColumnType::MYSQL_TYPE_TIMESTAMP2 => ColumnType::MYSQL_TYPE_TIMESTAMP2,
        mysql::consts::ColumnType::MYSQL_TYPE_DATETIME2 => ColumnType::MYSQL_TYPE_DATETIME2,
        mysql::consts::ColumnType::MYSQL_TYPE_TIME2 => ColumnType::MYSQL_TYPE_TIME2,
        mysql::consts::ColumnType::MYSQL_TYPE_JSON => ColumnType::MYSQL_TYPE_JSON,
        mysql::consts::ColumnType::MYSQL_TYPE_NEWDECIMAL => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        mysql::consts::ColumnType::MYSQL_TYPE_ENUM => ColumnType::MYSQL_TYPE_ENUM,
        mysql::consts::ColumnType::MYSQL_TYPE_SET => ColumnType::MYSQL_TYPE_SET,
        mysql::consts::ColumnType::MYSQL_TYPE_TINY_BLOB => ColumnType::MYSQL_TYPE_TINY_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_MEDIUM_BLOB => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_LONG_BLOB => ColumnType::MYSQL_TYPE_LONG_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_BLOB => ColumnType::MYSQL_TYPE_BLOB,
        mysql::consts::ColumnType::MYSQL_TYPE_VAR_STRING => ColumnType::MYSQL_TYPE_VAR_STRING,
        mysql::consts::ColumnType::MYSQL_TYPE_STRING => ColumnType::MYSQL_TYPE_STRING,
        mysql::consts::ColumnType::MYSQL_TYPE_GEOMETRY => ColumnType::MYSQL_TYPE_GEOMETRY,
    }
}

