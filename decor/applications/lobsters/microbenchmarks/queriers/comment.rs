extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn post_comment(db: &mut mysql::Conn, 
                    acting_as: Option<u64>, 
                    id: u64,
                    story: u64,
                    parent: Option<u64>
    ) -> Result<(), mysql::Error> 
{
    let user = acting_as.unwrap();
    let (author, hotness, story) : (u64, f64, u64) = db.query_first(format!(
            "SELECT `stories`.`user_id`, `stories`.`hotness`, stories.id \
             FROM `stories` \
             WHERE `stories`.`short_id` = {}",
             story
        ))?.unwrap();

    db.exec_drop(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` = ?",
        (author,),
    )?;

    let parent : Option<(u64, u64)> = if let Some(parent) = parent {
        // check that parent exists
        if let Some(p) = db.exec_first(
                "SELECT  `comments`.id, comments.thread_id FROM `comments` \
                 WHERE `comments`.`story_id` = ? \
                 AND `comments`.`short_id` = ?",
                 (story, parent,))? 
        {
            Some(p)
        } else {
            eprintln!(
                "failed to find parent comment {} in story {}",
                parent, story
            );
            None
        }
    } else {
        None
    };

    // TODO: real site checks for recent comments by same author with same
    // parent to ensure we don't double-post accidentally

    // check that short id is available
    db.exec_drop(
        "SELECT  1 AS one FROM `comments` \
         WHERE `comments`.`short_id` = ?",
         (id,)
    )?;

    // TODO: real impl checks *new* short_id *again*

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let now = chrono::Local::now().naive_local();
    let q = if let Some((parent, thread)) = parent {
        db.exec_iter(
            "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `parent_comment_id`, `thread_id`, \
             `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                now,
                id,
                story,
                user,
                parent,
                thread,
                "moar benchmarking", // lorem ipsum?
                1,
                0.1828847834138887,
                "<p>moar benchmarking</p>\n",
            ),
        )?
    } else {
        db.exec_iter(
            "INSERT INTO `comments` \
             (`created_at`, `updated_at`, `short_id`, `story_id`, \
             `user_id`, `comment`, `upvotes`, `confidence`, \
             `markeddown_comment`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                now,
                now,
                id,
                story,
                user,
                "moar benchmarking", // lorem ipsum?
                1,
                0.1828847834138887,
                "<p>moar benchmarking</p>\n",
            ),
        )?
    };
    let comment = q.last_insert_id().unwrap();
    drop(q);

    db.exec_drop(
        "SELECT  `votes`.* FROM `votes` \
         WHERE `votes`.`user_id` = ? \
         AND `votes`.`story_id` = ? \
         AND `votes`.`comment_id` = ?",
        (user, story, comment),
    )?;

    db.exec_drop(
        "INSERT INTO `votes` \
         (`user_id`, `story_id`, `comment_id`, `vote`) \
         VALUES (?, ?, ?, ?)",
        (user, story, comment, 1),
    )?;
    
    db.exec_drop(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = ?",
        (story,),
    )?;

    // why are these ordered?
    let res : Vec<(u64, u64)> = db.exec(
        "SELECT `comments`.id, \
         `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
         FROM `comments` \
         WHERE `comments`.`story_id` = ? \
         ORDER BY \
         saldo ASC, \
         confidence DESC",
        (story,),
    )?;
    let count = res.len() + 1;

    db.exec_drop(
        "UPDATE `stories` \
        SET `comments_count` = ?
        WHERE `stories`.`id` = ?",
        (count, story),
    )?;

    db.exec_drop(
        "SELECT `tags`.* \
         FROM `tags` \
         INNER JOIN `taggings` \
         ON `tags`.`id` = `taggings`.`tag_id` \
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

    // why oh why is story hotness *updated* here?!
    db.exec_drop(
        "UPDATE `stories` \
         SET `hotness` = ? \
         WHERE `stories`.`id` = ?",
        (hotness - 1.0, story),
    )?;

    let key = format!("user:{}:comments_posted", user);
    db.exec_drop(
        "INSERT INTO keystores (`key`, `value`) \
         VALUES (?, ?) \
         ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        (key, 1),
    )?;
    Ok(())
}
