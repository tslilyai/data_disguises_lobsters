extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn post_story(db: &mut mysql::Conn, acting_as: Option<u32>, id: usize,  title: String) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();

    db.exec_drop(
        "SELECT  1 AS one FROM `stories` \
         WHERE `stories`.`short_id` = ?",
        (id,)
    )?;

    // TODO: check for similar stories if there's a url
    // SELECT  `stories`.*
    // FROM `stories`
    // WHERE `stories`.`url` IN (
    //  'https://google.com/test',
    //  'http://google.com/test',
    //  'https://google.com/test/',
    //  'http://google.com/test/',
    //  ... etc
    // )
    // AND (is_expired = 0 OR is_moderated = 1)

    // TODO
    // real impl queries `tags` and `users` again here..?

    // TODO: real impl checks *new* short_id and duplicate urls *again*
    // TODO: sometimes submit url

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    let q = db
        .exec_iter(
            "INSERT INTO `stories` \
             (`created_at`, `user_id`, `title`, \
             `description`, `short_id`, `upvotes`, `hotness`, \
             `markeddown_description`) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                chrono::Local::now().naive_local(),
                user,
                title,
                "to infinity", // lorem ipsum?
                id,
                1,
                -19216.2884921,
                "<p>to infinity</p>\n",
            ),
        )?;
    let story = q.last_insert_id().unwrap();
    drop(q);

    /*db.exec_drop(
        "INSERT INTO `taggings` (`story_id`, `tag_id`) \
         VALUES (?, ?)",
        (story, tag),
    )?;*/

    let key = format!("user:{}:stories_submitted", user);
    db.exec_drop(
        "INSERT INTO keystores (`key`, `value`) \
         VALUES (?, ?) \
         ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        (key, 1),
    )?;

    // "priming"
    let key = format!("user:{}:stories_submitted", user);
    db.exec_drop(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = ?",
        (key,),
    )?;

    db.exec_drop(
        "SELECT  `votes`.* FROM `votes` \
         WHERE `votes`.`user_id` = ? \
         AND `votes`.`story_id` = ? \
         AND `votes`.`comment_id` IS NULL",
        (user, story),
    )?;
    
    db.exec_drop(
        "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
         VALUES (?, ?, ?)",
        (user, story, 1),
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
        "UPDATE `stories` \
         SET `hotness` = ? \
         WHERE `stories`.`id` = ?",
        (-19216.5479744, story),
    )?;

    Ok(())
}
