extern crate mysql;
extern crate log;
use std::collections::HashSet;
use rand;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn insert(db: &mut mysql::Conn, 
                    acting_as: Option<u64>, 
                    nstories: u64,
    ) -> Result<(), mysql::Error> 
{
    let user = acting_as.unwrap();

    db.query_drop(format!(
            "INSERT INTO `stories` (`created_at`, `user_id`, `title`, `description`, `short_id`, `upvotes`, `hotness`, `markeddown_description`) VALUES ('2020-11-24 12:39:27.838542370', {}, 'Dummy title', 'to infinity', {}, 1, -19216.2884921, '<p>to infinity</p>\n')", user, nstories))?;
    Ok(())
}
    

pub fn update(db: &mut mysql::Conn, 
                    acting_as: Option<u64>, 
                    story: u64,
    ) -> Result<(), mysql::Error> 
{
    let user = acting_as.unwrap();
    let count = rand::random::<u8>();
    db.query_drop(format!("UPDATE `users` SET `karma` = `users`.`karma` + 1 WHERE `users`.`id` = {}", user))?;
    db.query_drop(format!(
        "UPDATE `stories` \
        SET `comments_count` = {} 
        WHERE `stories`.`id` = {}",
        count, story)
    )?;
    Ok(())
}


pub fn select(db: &mut mysql::Conn, 
                    acting_as: Option<u64>, 
                    story: u64,
    ) -> Result<(), mysql::Error> 
{
    db.query_iter(format!("SELECT  `stories`.`user_id`, `stories`.`id` FROM `stories` WHERE `stories`.`merged_story_id` IS NULL AND `stories`.`is_expired` = 0 AND `stories`.`upvotes` - `stories`.`downvotes` >= 0 ORDER BY hotness LIMIT 51"))?;

    db.query_drop(format!(
        "SELECT \
         `comments`.`upvotes`, \
         `comments`.`downvotes` \
         FROM `comments` \
         JOIN `stories` ON (`stories`.`id` = `comments`.`story_id`) \
         WHERE `comments`.`story_id` = {} \
         AND `comments`.`user_id` <> `stories`.`user_id`",
        story)
    )?;
    Ok(())
}
