extern crate mysql;
extern crate log;
use log::{warn};
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

pub fn unsubscribe_user(user: u64, db: &mut mysql::Conn) -> (GhostMappingShard, EntityDataShard) {
    let res = db.query_iter(format!("UNSUBSCRIBE UID {};", user)).unwrap();
    for row in res {
        let vals = row.unwrap().unwrap();
        assert_eq!(vals.len(), 2);
        let s1 = helpers::mysql_val_to_string(&vals[0]);
        let s2 = helpers::mysql_val_to_string(&vals[1]);
        let s1 = s1.trim_end_matches('\'').trim_start_matches('\'');
        let s2 = s2.trim_end_matches('\'').trim_start_matches('\'');
        //warn!("Serialized values are {}, {}", s1, s2);
        return (serde_json::from_str(s1).unwrap(), serde_json::from_str(s2).unwrap())
    }
    (vec![], vec![])
}

pub fn resubscribe_user(user: u64, data: &(GhostMappingShard, EntityDataShard), db: &mut mysql::Conn) {
    let mut gid_strs = vec![];
    for (table, eid, gid) in &data.0{
        match eid {
            None => gid_strs.push(format!("({}, {}, {})", table, "NULL", gid)),
            Some(i) => gid_strs.push(format!("({}, {}, {})", table, i, gid)),
        }
    }
    let mut data_strs = vec![];
    for (table, vals) in &data.1{
        let vals_str = vals.join(",");
        data_strs.push(format!("({}, {{{}}})", table, vals_str));
    }
    db.query_drop(format!("RESUBSCRIBE UID {} WITH GIDS {} WITH DATA {};", user, gid_strs.join(", "), data_strs.join(","))).unwrap();
}

