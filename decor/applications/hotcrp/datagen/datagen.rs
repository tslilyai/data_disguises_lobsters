use crate::datagen::*;
use decor::types;
use decor::*;
use log::*;
use rand::{distributions::Alphanumeric, Rng};
use sql_parser::ast::*;

const SCHEMA: &'static str = include_str!("../schema.sql");

// Generates NUSERS_NONPC+NUSERS_PC users
pub const NUSERS_NONPC: usize = 20;
pub const NUSERS_PC: usize = 10;
// Generates NPAPERS_REJ+NPAPER_ACCEPT papers.
const NPAPERS_REJ: usize = 10;
const NPAPERS_ACCEPT: usize = 5;

/*shepherd_val
 * Paper metadata:
 * - Each paper is assigned 1 leadContactId
 * - Each paper is assigned 1 managerContactId
 * - Accepted papers are assigned a shepherdId that is one of the reviewers
 * - Reviews and paper conflicts per paper
 */
const NREVIEWS: usize = 4;
const NCONFLICT_REVIEWER: usize = 2; // from PC
const NCONFLICT_AUTHOR: usize = 2; // from pool of other users
const NPAPER_COMMENTS: usize = 3; // made by reviewers, users w/authorship conflicts

pub fn get_random_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

pub fn get_schema_tables() -> Vec<types::TableColumns> {
    let mut table_cols = vec![];
    let stmts = helpers::get_create_schema_statements(SCHEMA, true);
    for stmt in stmts {
        match stmt {
            Statement::CreateTable(CreateTableStatement { name, columns, .. }) => {
                table_cols.push(types::TableColumns {
                    name: name.to_string(),
                    cols: columns.iter().map(|c| c.name.to_string()).collect(),
                    colformats: columns.iter().map(|c| helpers::get_parser_colformat(&c.data_type)).collect(),
                });
            }
            _ => unimplemented!("Not a create table statement?"),
        }
    }
    table_cols
}

pub fn populate_database(db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    create_schema(SCHEMA, true, db).unwrap();

    let total_users = NUSERS_NONPC + NUSERS_PC;
    let other_uids: Vec<usize> = (1..NUSERS_NONPC + 1).collect();
    let pc_uids: Vec<usize> = (NUSERS_NONPC + 1..total_users + 1).collect();
    let papers_rej: Vec<usize> = (1..NPAPERS_REJ + 1).collect();
    let papers_acc: Vec<usize> = (NPAPERS_REJ + 1..(NPAPERS_REJ + NPAPERS_ACCEPT + 1)).collect();

    // insert users
    warn!("INSERTING USERS");
    users::insert_users(NUSERS_NONPC + NUSERS_PC, db)?;

    // insert papers, author comments on papers, coauthorship conflicts
    warn!("INSERTING PAPERS");
    papers::insert_papers(
        &other_uids,
        &pc_uids,
        &papers_rej,
        &papers_acc,
        NPAPER_COMMENTS,
        NCONFLICT_AUTHOR,
        db,
    )?;

    // insert reviews, reviewer comments on papers, reviewer conflicts
    warn!("INSERTING REVIEWS");
    reviews::insert_reviews(
        &pc_uids,
        NPAPERS_REJ + NPAPERS_ACCEPT,
        NREVIEWS,
        NPAPER_COMMENTS,
        NCONFLICT_REVIEWER,
        db,
    )?;

    Ok(())
}
