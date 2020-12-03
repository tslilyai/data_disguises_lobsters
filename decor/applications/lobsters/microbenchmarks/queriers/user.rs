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

pub fn unsubscribe_user(user: u64, db: &mut mysql::Conn) -> Vec<(String, String, String)> {
    let mut results = vec![];
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", user)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 3);
        let name = format!("{}", helpers::mysql_val_to_parser_val(&vals[0])).trim().trim_matches('\'').to_string();
        let eid = format!("{}", helpers::mysql_val_to_parser_val(&vals[1])).trim().trim_matches('\'').to_string();
        let gid = format!("{}", helpers::mysql_val_to_parser_val(&vals[2])).trim().trim_matches('\'').to_string();
        results.push((name, eid, gid));
    }
    results
}

pub fn resubscribe_user(user: u64, gids: Vec<(String, String, String)>, db: &mut mysql::Conn) {
    let mut gid_strs = vec![];
    for (table, eid, gid) in &gids{
        gid_strs.push(format!("({}, {}, {})", table, eid, gid));
    }
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS {};", user, gid_strs.join(", "))).unwrap();
}

