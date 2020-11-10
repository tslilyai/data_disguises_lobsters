use sql_parser::ast::*;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};

#[derive(Debug, Clone)]
pub struct View {
    name: String,
    // schema column definitions
    columns: Vec<ColumnDef>,
    // values stored in table
    rows: Vec<Vec<Value>>,
    // List of indices (by column) INDEX of column (only INT type for now) to row
    indices: HashMap<String, HashMap<String, Vec<usize>>>,
}

impl View {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        View {
            name: String::new(),
            columns: columns,
            rows: vec![],
            indices: HashMap::new(),
        }
    }
    pub fn contains_row(&self, r: &Vec<Value>) -> bool {
        self.rows.iter().any(|row| {
            let mut eq = true;
            for i in 0..row.len() {
                eq = eq && (row[i] == r[i]);
            }
            eq
        })
    }
    pub fn get_rows_of_col(&self, col_index: usize, val: &Value) -> Vec<Vec<Value>> {
        let mut rows = vec![];
        if let Some(index) = self.indices.get(&self.columns[col_index].name.to_string()) {
            if let Some(row_indices) = index.get(&val.to_string()) {
                for i in row_indices {
                    rows.push(self.rows[*i].clone());
                }
            }
        } else {
            for row in &self.rows {
                match &row[col_index] {
                    Value::Number(v) => if *v == val.to_string() {
                        rows.push(row.clone());
                    }
                    _ => unimplemented!("Must be a number!")
                } 
            }
        }
        rows
    }
}

pub struct Views {
    views: HashMap<String, View>,
}

impl Views {
    pub fn new() -> Self {
        Views {
            views: HashMap::new(),
        }
    }

    /*
     * Convert table name (with optional alias) to current view
     */
    fn tablefactor_to_view(&self, tf: &TableFactor) -> Result<View, Error> {
        match tf {
            TableFactor::Table {
                name,
                alias,
            } => {
                let tab = self.views.get(&name.to_string());
                match tab {
                    None => Err(Error::new(ErrorKind::Other, format!("table {:?} does not exist", tf))),
                    Some(t) => {
                        let mut view = t.clone();
                        if let Some(a) = alias {
                            // only alias table name..
                            assert!(a.columns.is_empty());
                            view.name = a.name.to_string();
                        }
                        Ok(view)
                    }
                }
            }
            _ => unimplemented!("no derived joins {:?}", tf),
        }
    }

    /*
     * Only handle join constraints of form "table.col = table'.col'"
     */
    fn get_join_on_col_indices(&self, e: &Expr, v1: &View, v2: &View) -> Result<(usize, usize), Error> {
        let err = Error::new(ErrorKind::Other, format!("joins constraint not supported: {:?}", e));
        let i1: Option<usize>; 
        let i2 : Option<usize>;
        if let Expr::BinaryOp {left, op, right} = e {
            if let BinaryOperator::Eq = op {
                if let (Expr::Identifier(ids1), Expr::Identifier(ids2)) = (&(**left), &(**right)) {
                    if let (Some(tab1), Some(tab2)) = (ids1.get(ids1.len()-2), ids2.get(ids2.len()-2)) {
                        if let (Some(col1), Some(col2)) = (ids1.get(ids1.len()-1), ids2.get(ids2.len()-1)) {
                            if v1.name == tab1.to_string() && v2.name == tab2.to_string() {
                                i1 = v1.columns.iter().position(|c| c.to_string() == col1.to_string());
                                i2 = v2.columns.iter().position(|c| c.to_string() == col2.to_string());
                            } else if v2.name == tab1.to_string() && v1.name == tab2.to_string() {
                                i1 = v1.columns.iter().position(|c| c.to_string() == col1.to_string());
                                i2 = v2.columns.iter().position(|c| c.to_string() == col2.to_string());
                            } else {
                                return Err(err);
                            }
                            if i1 == None || i2 == None {
                                return Err(err);
                            }
                            return Ok((i1.unwrap(), i2.unwrap()));
                        }
                    }
                }
            }
        }
        unimplemented!("join_on {}", e)
    }

    fn join_views(&self, jo: &JoinOperator, v1: &View, v2: &View) -> Result<View, Error> {
        let mut new_cols : Vec<ColumnDef> = v1.columns
            .iter()
            .map(|c| ColumnDef{
                name: Ident::new(format!("{}.{}", v1.name, c.name)),
                data_type: c.data_type.clone(),
                collation: c.collation.clone(),
                options: c.options.clone(),
            })
            .collect();
        let mut new_cols_2 = v2.columns
            .iter()
            .map(|c| ColumnDef{
                name: Ident::new(format!("{}.{}", v2.name, c.name)),
                data_type: c.data_type.clone(),
                collation: c.collation.clone(),
                options: c.options.clone(),
            })
            .collect();
        new_cols.append(&mut new_cols_2);
        let mut new_view = View::new(new_cols);
        match jo {
            JoinOperator::Inner(JoinConstraint::On(e)) => {
                let (i1, i2) = self.get_join_on_col_indices(&e, v1, v2)?;
                // this seems very very inefficient
                for row1 in &v1.rows {
                    new_view.rows.append(&mut v2.get_rows_of_col(i2, &row1[i1]));
                }
            }
            JoinOperator::LeftOuter(JoinConstraint::On(e)) => {
                let (i1, i2) = self.get_join_on_col_indices(&e, v1, v2)?;
                for row1 in &v1.rows {
                    let mut found = false;
                    let mut rows2 = v2.get_rows_of_col(i2, &row1[i1]);
                    if !rows2.is_empty() {
                        new_view.rows.append(&mut rows2);
                        found = true;
                    }
                    if !found {
                        let mut new_row = row1.clone();
                        new_row.append(&mut vec![Value::Null; v2.columns.len()]);
                        new_view.rows.push(new_row);
                    }
                }
            }
            JoinOperator::RightOuter(JoinConstraint::On(e)) => {
                let (i1, i2) = self.get_join_on_col_indices(&e, v1, v2)?;
                for row2 in &v2.rows {
                    let mut found = false;
                    let mut rows1 = v1.get_rows_of_col(i1, &row2[i2]);
                    if !rows1.is_empty() {
                        new_view.rows.append(&mut rows1);
                        found = true;
                    }
                    if !found {
                        let mut new_row = vec![Value::Null; v1.columns.len()];
                        new_row.append(&mut row2.clone());
                        new_view.rows.push(new_row);
                    }
                }            
            }
            JoinOperator::FullOuter(JoinConstraint::On(e)) => {
                let (i1, i2) = self.get_join_on_col_indices(&e, v1, v2)?;
                for row1 in &v1.rows {
                    let mut found = false;
                    let mut rows2 = v2.get_rows_of_col(i2, &row1[i1]);
                    if !rows2.is_empty() {
                        new_view.rows.append(&mut rows2);
                        found = true;
                    } 
                    if !found {
                        let mut new_row = row1.clone();
                        new_row.append(&mut vec![Value::Null; v2.columns.len()]);
                        new_view.rows.push(new_row);
                    }
                }
                // only add null rows for rows that weren't matched
                for row2 in &v2.rows {
                    let mut found = false;
                    let rows1 = v1.get_rows_of_col(i1, &row2[i2]);
                    if !rows1.is_empty() {
                        found = true;
                    } 
                    if !found {
                        let mut new_row = vec![Value::Null; v1.columns.len()];
                        new_row.append(&mut row2.clone());
                        new_view.rows.push(new_row);
                    }
                }            
            }
            _ => unimplemented!("No support for join type {:?}", jo),
        }
        Ok(new_view)
    }

    fn tablewithjoins_to_view(&self, twj: &TableWithJoins) -> Result<View, Error> {
        let mut view1 = self.tablefactor_to_view(&twj.relation)?;
        for j in &twj.joins {
            let view2 = self.tablefactor_to_view(&j.relation)?;
            view1 = self.join_views(&j.join_operator, &view1, &view2)?;
        }
        Ok(view1)
    }

    fn get_setexpr_results(&self, se: &SetExpr) -> Result<View, Error> {
        match se {
            SetExpr::Select(s) => {
                let mut new_view = View::new(vec![]);
                if s.having != None {
                    unimplemented!("No support for having queries");
                }
                let mut from_views = vec![];
                for twj in &s.from {
                    from_views.push(self.tablewithjoins_to_view(&twj)?);
                }
                for proj in &s.projection {
                    match proj {
                        SelectItem::Wildcard => {
                            // take all the columns from all the views
                            for v in &from_views {
                                new_view.columns.append(&mut v.columns.clone());
                            }
                            // only support wildcards if there are no other projections...
                            assert!(s.projection.len() == 1);
                        },
                        SelectItem::Expr {expr, alias} => {
                            ()
                        }
                    }
                }
                Ok(new_view)
            }
            SetExpr::Query(q) => {
                return self.get_query_results(&q);
            }
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let left_view = self.get_setexpr_results(&left)?;
                let right_view = self.get_setexpr_results(&right)?;
                let mut view = left_view.clone();
                match op {
                    // TODO primary keys / unique keys 
                    SetOperator::Union => {
                        view.rows.append(&mut right_view.rows.clone());
                        return Ok(view);
                    }
                    SetOperator::Except => {
                        let mut view = left_view.clone();
                        view.rows.retain(|r| !right_view.contains_row(&r));
                        return Ok(view);
                    },
                    SetOperator::Intersect => {
                        let mut view = left_view.clone();
                        view.rows.retain(|r| right_view.contains_row(&r));
                        return Ok(view);
                    }
                }
            }
            SetExpr::Values(vals) => {
                unimplemented!("Shouldn't be getting values when looking up results: {}", se); 
            }
        }
    }
    
    fn get_query_results(&self, q: &Query) -> Result<View, Error> {
        self.get_setexpr_results(&q.body)
    }

    /*pub fn expr_to_results(&self, expr: &Expr) -> Expr {
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
    }*/

    pub fn query_view(&self, stmt: &Statement) -> Result<(View, bool), Error> {
        let mut results : View = View::new(vec![]);
        let mut is_write = false;
        match stmt {
            // Note: mysql doesn't support "as_of"
            Statement::Select(SelectStatement{
                query, 
                as_of,
            }) => {
                // ignore ctes for now
                results = self.get_query_results(&query)?;
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                is_write = true;
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                is_write = true;
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                is_write = true;
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
                is_write = true;
            }
            Statement::CreateIndex(CreateIndexStatement{
                name,
                on_name,
                key_parts,
                if_not_exists,
            }) => {
                is_write = true;
            }
            Statement::AlterObjectRename(AlterObjectRenameStatement{
                object_type,
                if_exists,
                name,
                to_item_name,
            }) => {
                is_write = true;
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                if_exists,
                names,
                cascade,
            }) => {
                is_write = true;
            }
            /* TODO Handle Statement::Explain(stmt) => f.write_node(stmt), ShowObjects
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
            }
        }
        Ok((results, is_write))
    }
}
