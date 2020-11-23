extern crate mysql;
extern crate log;
use std::collections::HashSet;

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
    let mut users : HashSet<u64> = HashSet::new(); 
    let mut stories : HashSet<u64> = HashSet::new();

    //db.query_drop("SELECT `users`.* FROM `users` WHERE `users`.`id` IN (6,7,8,4,1,0,9,5,2,3)");
    /*let (author, hotness, story) : (u64, f64, u64) = db.query_first(format!(
            "SELECT `stories`.`user_id`, `stories`.`hotness`, stories.id \
             FROM `stories` \
             WHERE `stories`.`short_id` = {}",
             story
        ))?.unwrap();

    db.query_drop(format!(
        "SELECT `users`.* FROM `users` WHERE `users`.`id` = {}",
        author
    ))?;*/

    // check that short id is available
    /*db.query_drop(format!(
        "SELECT  1 AS one FROM `comments` \
         WHERE `comments`.`short_id` = {}",
         id
    ))?;*/

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    //in the lobsters source for details.
    /*db.query_drop(format!(
        "UPDATE stories SET \
         upvotes = stories.upvotes {}, \
         downvotes = stories.downvotes {}, \
         hotness = {} \
         WHERE stories.id = {}",
         "+ 1",
         "+ 1",
        1.0,
        story,
    ))?;*/
    db.query_map(
        "SELECT  `stories`.`user_id`, `stories`.`id` FROM `stories` \
         WHERE `stories`.`merged_story_id` IS NULL \
         AND `stories`.`is_expired` = 0 \
         AND `stories`.`upvotes` - `stories`.`downvotes` >= 0 \
         ORDER BY hotness LIMIT 51",
         // OFFSET 0" parser can't handle this for some reason,
         |(user_id, id)| {
            users.insert(user_id);
            stories.insert(id);
         }
    )?;
    assert!(!stories.is_empty(), "got no stories from /frontpage");
 
    /*let res : Vec<(u64, u64)> = db.query(format!(
        "SELECT `comments`.id, \
         `comments`.`upvotes` - `comments`.`downvotes` AS saldo \
         FROM `comments` \
         WHERE `comments`.`story_id` = {} \
         ORDER BY \
         saldo ASC, \
         confidence DESC",
        story,),
    )?;
    let count = res.len() + 1;*/

    /*db.query_drop(format!(
        "UPDATE `stories` \
        SET `comments_count` = {} 
        WHERE `stories`.`id` = {}",
        count, story)
    )?;

    db.query_drop(format!(
        "SELECT `tags`.* \
         FROM `tags` \
         INNER JOIN `taggings` \
         ON `tags`.`id` = `taggings`.`tag_id` \
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
        story)
    )?;
    
    db.query_drop(format!(
        "SELECT `stories`.`id` \
         FROM `stories` \
         WHERE `stories`.`merged_story_id` = {}",
        story)
    )?;*/

    Ok(())
}
