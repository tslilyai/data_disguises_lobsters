use crate::datagen::*;
use rand::{distributions::Alphanumeric, Rng};
use mysql::prelude::*;
use decor::*;
use log::*;

const SCHEMA : &'static str = include_str!("../schema.sql");

// Generates NUSERS_NONPC+NUSERS_PC users
pub const NUSERS_NONPC: usize = 500;
pub const NUSERS_PC: usize = 50;
// Generates NPAPERS_REJ+NPAPER_ACCEPT papers.
const NPAPERS_REJ: usize = 450;
const NPAPERS_ACCEPT: usize = 50;

/*
 * Paper metadata:
 * - Each paper is assigned 1 leadContactId
 * - Each paper is assigned 1 managerContactId
 * - Accepted papers are assigned a shepherdID that is one of the reviewers
 * - Reviews and paper conflicts per paper
 */
const NREVIEWS: usize = 4;
const NCONFLICT_REVIEWER: usize = 3; // from PC
const NCONFLICT_AUTHOR: usize = 3; // from pool of other users
const NPAPER_COMMENTS: usize = 5; // made by reviewers, users w/authorship conflicts

pub fn get_random_string() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect()
}

fn get_table_names() -> Vec<&'static str> {
    vec![
        "ContactInfo", 
        "PaperReviewPreference",
        "PaperWatch",
        "Capability",
        "PaperConflict",
        "TopicInterest",
        "PaperTag",
        "PaperTagAnno",
        "PaperReviewRefused",
        "ActionLog",
        "ReviewRating",
        "PaperComment",
        "PaperReview",
    ]
}

pub fn populate_database(db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    create_schema(SCHEMA, true, db).unwrap();

    let total_users = NUSERS_NONPC + NUSERS_PC;
    let other_uids: Vec<usize> = (1..NUSERS_NONPC + 1).collect();
    let pc_uids: Vec<usize> = (NUSERS_NONPC + 1..total_users+ 1).collect();
    let papers_rej : Vec<usize> = (1..NPAPERS_REJ + 1).collect();
    let papers_acc : Vec<usize> = (NPAPERS_REJ + 1..NPAPERS_ACCEPT + 1).collect();

    // insert users 
    warn!("INSERTING USERS");
    users::insert_users(NUSERS_NONPC + NUSERS_PC, db)?;

    // insert papers, author comments on papers, coauthorship conflicts
    warn!("INSERTING PAPERS");
    papers::insert_papers(&other_uids, &pc_uids, &papers_rej, &papers_acc, NPAPER_COMMENTS, NCONFLICT_AUTHOR, db)?;

    // insert reviews, reviewer comments on papers, reviewer conflicts
    warn!("INSERTING REVIEWS");
    reviews::insert_reviews(&pc_uids, NPAPERS_REJ + NPAPERS_ACCEPT, NREVIEWS, NPAPER_COMMENTS, NCONFLICT_REVIEWER, db)?;

    Ok(())
}
