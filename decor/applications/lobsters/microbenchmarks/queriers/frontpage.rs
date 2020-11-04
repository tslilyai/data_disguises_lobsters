extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
use std::collections::HashSet;
//use log::{warn, debug};

pub fn query_frontpage(db: &mut mysql::Conn, acting_as: Option<u32>) -> Result<(), mysql::Error> {
    let mut users : HashSet<u32> = HashSet::new(); 
    let mut stories : HashSet<u32> = HashSet::new();
    db.query_map(
        "SELECT  `stories`.`user_id`, `stories`.`id` FROM `stories` \
         WHERE `stories`.`merged_story_id` IS NULL \
         AND `stories`.`is_expired` = 0 \
         AND `stories`.`upvotes` - `stories`.`downvotes` >= 0 \
         ORDER BY hotness LIMIT 51 OFFSET 0",
         |(user_id, id)| {
            users.insert(user_id);
            stories.insert(id);
         }
    )?;
    assert!(!stories.is_empty(), "got no stories from /frontpage");
    
    let stories_in = stories
        .iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    if let Some(uid) = acting_as {
        db.query_drop(format!(
            "SELECT `hidden_stories`.`story_id` \
             FROM `hidden_stories` \
             WHERE `hidden_stories`.`user_id` = {}",
            uid
        ))?;

        db.query_drop(format!(
            "SELECT `tag_filters`.* FROM `tag_filters` \
             WHERE `tag_filters`.`user_id` = {}",
            uid
        ))?;

        db.query_drop(format!(
            "SELECT `taggings`.`story_id` \
             FROM `taggings` \
             WHERE `taggings`.`story_id` IN ({})",
            // AND `taggings`.`tag_id` IN ({})",
            stories_in,
            //tags
        ))?;
    }

   let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    
    db.query_drop(&format!(
            "SELECT `users`.* FROM `users` WHERE `users`.`id` IN ({})",
            users,
    ))?;
    
    db.query_drop(&format!(
        "SELECT `suggested_titles`.* \
         FROM `suggested_titles` \
         WHERE `suggested_titles`.`story_id` IN ({})",
        stories_in
    ))?;

    db.query_drop(&format!(
        "SELECT `suggested_taggings`.* \
         FROM `suggested_taggings` \
         WHERE `suggested_taggings`.`story_id` IN ({})",
        stories_in
    ))?;

    let mut tags : HashSet<u32> = HashSet::new();
    db.query_map(&format!(
            "SELECT `taggings`.`tag_id` FROM `taggings` \
             WHERE `taggings`.`story_id` IN ({})",
            stories_in
        ),
        |tag_id| tags.insert(tag_id)
    )?;

    let tags = tags
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    if tags.len() > 0 {
        db.query_drop(&format!(
                "SELECT `tags`.* FROM `tags` WHERE `tags`.`id` IN ({})",
                tags
            ))?;
    }
    
    // also load things that we need to highlight
    if let Some(uid) = acting_as {
        let story_params = stories.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(",");
        db.query_drop(
                &format!(
                    "SELECT `votes`.* FROM `votes` \
                     WHERE `votes`.`user_id` = {} \
                     AND `votes`.`story_id` IN ({}) \
                     AND `votes`.`comment_id` IS NULL",
                     uid,
                    story_params
                ),
            )?;

        db.query_drop(
                &format!(
                    "SELECT `hidden_stories`.* \
                     FROM `hidden_stories` \
                     WHERE `hidden_stories`.`user_id` = {} \
                     AND `hidden_stories`.`story_id` IN ({})",
                     uid,
                    story_params
                ),
            )?;

        db.query_drop(
                &format!(
                    "SELECT `saved_stories`.* \
                     FROM `saved_stories` \
                     WHERE `saved_stories`.`user_id` = {} \
                     AND `saved_stories`.`story_id` IN ({})",
                    uid,
                    story_params
                ),
            )?;
    }
    Ok(())
}
