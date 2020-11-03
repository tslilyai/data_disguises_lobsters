extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn vote_on_story(db: &mut mysql::Conn, acting_as: Option<u32>, story_id: u32, pos: bool) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();
    let (author, score, story) : (u32, f64, u32) = db.query(format!(
                "SELECT `stories`.user_id, stories.hotness, stories.id \
                 FROM `stories` \
                 WHERE `stories`.`short_id` = {}",
                 story_id
            ))?[0];
    db.exec_drop(
        "SELECT  `votes`.* \
         FROM `votes` \
         WHERE `votes`.`user_id` = ? \
         AND `votes`.`story_id` = ? \
         AND `votes`.`comment_id` IS NULL",
        (user, story),
    )?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load story under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    db.exec_drop(
        "INSERT INTO `votes` \
         (`user_id`, `story_id`, `vote`) \
         VALUES \
         (?, ?, ?)",
        (
            user,
            story,
            match pos {
                true => 1,
                false => 0,
            },
        ),
    )?;

    db.exec_drop(
        &format!(
            "UPDATE `users` \
             SET `users`.`karma` = `users`.`karma` {} \
             WHERE `users`.`id` = ?",
            match pos {
                true => "+ 1",
                false=> "- 1",
            }
        ),
        (author,),
    )?;

    // get all the stuff needed to compute updated hotness
    db.exec_drop(
        "SELECT `tags`.* \
         FROM `tags` \
         INNER JOIN `taggings` ON `tags`.`id` = `taggings`.`tag_id` \
         WHERE `taggings`.`story_id` = ?",
        (story,),
    )?;

    db.exec_drop(
         "SELECT \
         `comments`.`upvotes`, \
         `comments`.`downvotes` \
         FROM `comments` \
         JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
         WHERE `comments`.`story_id` = ? \
         AND `comments`.`user_id` <> `stories`.`user_id`",
        (story,),
    )?;

    db.exec_drop(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = ?",
        (story,),
    )?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    db.exec_drop(
        &format!(
            "UPDATE stories SET \
             stories.upvotes = stories.upvotes {}, \
             stories.downvotes = stories.downvotes {}, \
             stories.hotness = ? \
             WHERE stories.id = ?",
            match pos {
                true => "+ 1",
                false => "+ 0",
            },
            match pos {
                true => "+ 0",
                false => "+ 1",
            },
        ),
        (
            score
                - match pos {
                    true => 1.0,
                    false => -1.0,
                },
            story,
        ),
    )?;
    Ok(())
}
