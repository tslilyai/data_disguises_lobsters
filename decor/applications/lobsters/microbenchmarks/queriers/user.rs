extern crate mysql;
extern crate log;
use decor::*;

use mysql::prelude::*;
use std::*;
//use log::{warn, debug};

pub fn get_profile(db: &mut mysql::Conn, uid: u64) -> Result<(), mysql::Error> {
    let uids : Vec<u64> = db.query(format!(
            "SELECT `users`.id FROM `users` \
             WHERE `users`.`username` = {}",
            (format!("\'user{}\'", uid))
        ))?;
    if uids.is_empty() {
        return Ok(());
    }
    let uid = uids[0];

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

pub fn unsubscribe_user(user: u64, db: &mut mysql::Conn) -> Vec<u64> {
    let mut results = vec![];
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", user)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 1);
        let gid = helpers::mysql_val_to_u64(&vals[0]).unwrap();
        results.push(gid);
    }
    results
}

pub fn resubscribe_user(user: u64, gids: Vec<u64>, db: &mut mysql::Conn) {
    let mut gid_str = String::new();
    for i in 0..gids.len() {
        gid_str.push_str(&gids[i].to_string());
        if i+1 < gids.len() {
            gid_str.push_str(",");
        }
    }
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS ({});", user, gid_str)).unwrap();
}

