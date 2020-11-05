extern crate mysql;
extern crate log;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn get_profile(db: &mut mysql::Conn, uid: u64) -> Result<(), mysql::Error> {
    let uid : u64 = db.query(format!(
            "SELECT `users`.id FROM `users` \
             WHERE `users`.`username` = {}",
            (format!("\'user{}\'", uid))
        ))?[0];

    let rows : Vec<(u64, u64)> = db.query(format!(
            "SELECT  `tags`.`id`, COUNT(*) AS `count` FROM `taggings` \
             INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` \
             INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` \
             WHERE `tags`.`inactive` = 0 \
             AND `stories`.`user_id` = {} \
             GROUP BY `tags`.`id` \
             ORDER BY `count` desc LIMIT 1",
            uid)
        )?;

    if !rows.is_empty() {
        let tag : u64 = rows[0].0;
        db.query_drop(format!(
            "SELECT  `tags`.* \
             FROM `tags` \
             WHERE `tags`.`id` = {}",
             tag,)
        )?;
    }
    db.query_drop(format!(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = {}",
        (format!("\'user:{}:stories_submitted\'", uid))),
    )?;
    db.query_drop(format!(
        "SELECT  `keystores`.* \
         FROM `keystores` \
         WHERE `keystores`.`key` = {}",
        (format!("\'user:{}:comments_posted\'", uid))),
    )?;
    db.query_drop(format!(
        "SELECT  1 AS one FROM `hats` \
         WHERE `hats`.`user_id` = {} LIMIT 1",
        uid),
    )?;
    Ok(())
} 


pub fn resubscribe(db: &mut mysql::Conn, uid: u64) -> Result<(), mysql::Error> {
    db.query_drop(format!("RESUBSCRIBE UID {}", uid))?;
    Ok(())
}

pub fn unsubscribe(db: &mut mysql::Conn, uid: u64) -> Result<(), mysql::Error> {
    db.query_drop(format!("UNSUBSCRIBE UID {}", uid))?;
    Ok(())
}
