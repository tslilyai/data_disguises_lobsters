extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn vote_on_story(db: &mut mysql::Conn, acting_as: Option<u64>, story_id: u64, pos: bool) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();
    let (author, score, story) : (u64, f64, u64) = db.query(format!(
                "SELECT `stories`.user_id, stories.hotness, stories.id \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = {}",
                 story_id
            ))?[0];
    db.query_drop(format!(
        "SELECT  `votes`.* \
         FROM `votes` \
         WHERE `votes`.`user_id` = {} \
         AND `votes`.`story_id` = {} \
         AND `votes`.`comment_id` IS NULL",
        user, story),
    )?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    db.query_drop(format!(
        "INSERT INTO `votes` \
         (`user_id`, `story_id`, `vote`) \
         VALUES \
         ({}, {}, {})",
        user,
        story,
        match pos {
            true => 1,
            false => 0,
        },
    ))?;

    db.query_drop(format!(
        "UPDATE `users` \
         SET `karma` = `users`.`karma` {} \
         WHERE `users`.`id` = {}",
        match pos {
            true => "+ 1",
            false=> "- 1",
        },
        author),
    )?;

    // get all the stuff needed to compute updated hotness
    db.query_drop(format!(
        "SELECT `tags`.* \
         FROM `tags` \
         INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
         WHERE `taggings`.`story_id` = {}",
        story,),
    )?;

    db.query_drop(format!(
         "SELECT \
         `comments`.`upvotes`, \
         `comments`.`downvotes` \
         FROM `comments` \
         JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
         WHERE `comments`.`story_id` = {} \
         AND `comments`.`user_id` <> `stories`.`user_id`",
        story,),
    )?;

    db.query_drop(format!(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = {}",
        story,),
    )?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    db.query_drop(format!(
        "UPDATE stories SET \
         upvotes = stories.upvotes {}, \
         downvotes = stories.downvotes {}, \
         hotness = {} \
         WHERE stories.id = {}",
        match pos {
            true => "+ 1",
            false => "+ 0",
        },
        match pos {
            true => "+ 0",
            false => "+ 1",
        },
        score
            - match pos {
                true => 1.0,
                false => -1.0,
            },
        story,
    ))?;
    Ok(())
}