use decor::helpers::*;
use sql_parser::ast::*;

pub fn insert_paper_comment (
    comment_id: usize,
    paper_id: usize,
    author_id: usize,
    db: &mut mysql::Conn,
) -> Result<(), mysql::Error> {
    let comment_cols = 
    vec![
        "commentId",
        "paperId",
        "contactId",
    ];
    let comment_vals = vec![vec![
        Expr::Value(Value::Number(comment_id.to_string())),
        Expr::Value(Value::Number(paper_id.to_string())),
        Expr::Value(Value::Number(author_id.to_string())),
    ]];
    get_query_rows_db(
        &Statement::Insert(InsertStatement {
            table_name: string_to_objname("PaperComment"),
            columns: comment_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
            source: InsertSource::Query(Box::new(values_query(comment_vals))),
        }),
        db,
    )?;
    Ok(())
}
