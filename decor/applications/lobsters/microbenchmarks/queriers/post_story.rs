extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn post_story(db: &mut mysql::Conn, acting_as: Option<u64>, id: u64,  title: String) -> Result<(), mysql::Error> {
    let user = acting_as.unwrap();

    db.query_drop(format!(
        "SELECT  1 AS one FROM `stories` \
         WHERE `stories`.`short_id` = {}",
        id,)
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
        .query_iter(format!(
            "INSERT INTO `stories` \
             (`created_at`, `user_id`, `title`, \
             `description`, `short_id`, `upvotes`, `hotness`, \
             `markeddown_description`) \
             VALUES (\"{}\", {}, \"{}\", \"{}\", {}, {}, {}, \"{}\")",
                chrono::Local::now().naive_local(),
                user,
                title,
                "to infinity", // lorem ipsum?
                id,
                1,
                -19216.2884921,
                "<p>to infinity</p>\\n",
            ),
        )?;
    // TODO this returned none?
    //let story = q.last_insert_id().unwrap();
    drop(q);

    /*db.query_drop(format!(
        "INSERT INTO `taggings` (`story_id`, `tag_id`) \
         VALUES ({}, {})",
        (story, tag),
    )?;*/

    let key = format!("\"user:{}:stories_submitted\"", user);
    db.query_drop(format!(
        "INSERT INTO keystores (`key`, `value`) \
         VALUES ({}, {}) \
         ON DUPLICATE KEY UPDATE `keystores`.`value` = `keystores`.`value` + 1",
        key, 1),
    )?;

    // "priming"
    /*let key = format!("user:{}:stories_submitted", user);
    db.query_drop(format!(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = {}",
        key,),
    )?;

    db.query_drop(format!(
        "SELECT  `votes`.* FROM `votes` \
         WHERE `votes`.`user_id` = {} \
         AND `votes`.`story_id` = {} \
         AND `votes`.`comment_id` IS NULL",
        user, story),
    )?;
    
    db.query_drop(format!(
        "INSERT INTO `votes` (`user_id`, `story_id`, `vote`) \
         VALUES ({}, {}, {})",
        user, story, 1),
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
        "UPDATE `stories` \
         SET `hotness` = {} \
         WHERE `stories`.`id` = {}",
        -19216.5479744, story),
    )?;*/

    Ok(())
}
