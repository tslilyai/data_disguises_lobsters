use crate::*;
use crate::disguises::*;
use mysql::*;
use mysql::from_value;

pub fn insert_new_user(db: &mut mysql::PooledConn) -> String {
    let cols = get_insert_guise_cols();
    let vals : Vec<String> = get_insert_guise_vals().iter().map(|v| v.to_string()).collect();
    db.query_drop(&format!(
        "INSERT INTO {} ({}) VALUES ({});",
        "users",
        cols.join(","),
        vals.join(","),
    )).unwrap();
    return vals[0].clone();
}

pub fn apply_decay(
    uid: u64,
    edna: &mut EdnaClient,
) -> Result<()> {
    let mut db = edna.get_conn()?;
    let mut db2 = edna.get_conn()?;
    db.query_drop(&format!("DELETE FROM users WHERE id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hat_requests WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hats WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hidden_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM invitations WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM read_ribbons WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM saved_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM suggested_taggings WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM suggested_titles WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM tag_filters WHERE user_id={}", uid))?;

    let res = db.query_iter(&format!("SELECT id FROM comments WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE comments SET user_id={} WHERE id={}", new_user, id))?;
    }


    let res = db.query_iter(&format!("SELECT id FROM stories WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE stories SET user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM messages WHERE author_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE messages SET author_user_id={} WHERE id={}", new_user, id))?;
    }
    let res = db.query_iter(&format!("SELECT id FROM messages WHERE recipient_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("INSERT INTO `users` (`username`) VALUES ({})", new_user)).unwrap();
        db2.query_drop(&format!("UPDATE messages SET recipient_user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM mod_notes WHERE moderator_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("INSERT INTO `users` (`username`) VALUES ({})", new_user)).unwrap();
        db2.query_drop(&format!("UPDATE mod_notes SET moderator_user_id={} WHERE id={}", new_user, id))?;
    }
    let res = db.query_iter(&format!("SELECT id FROM mod_notes WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("INSERT INTO `users` (`username`) VALUES ({})", new_user)).unwrap();
        db2.query_drop(&format!("UPDATE mod_notes SET user_id={} WHERE id={}", new_user, id))?;
    }

   let res = db.query_iter(&format!("SELECT id FROM moderations WHERE moderator_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("INSERT INTO `users` (`username`) VALUES ({})", new_user)).unwrap();
        db2.query_drop(&format!("UPDATE moderations SET moderator_user_id={} WHERE id={}", new_user, id))?;
    }
    let res = db.query_iter(&format!("SELECT id FROM moderations WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("INSERT INTO `users` (`username`) VALUES ({})", new_user)).unwrap();
        db2.query_drop(&format!("UPDATE moderations SET user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM votes WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE votes SET user_id={} WHERE id={}", new_user, id))?;
    }

    Ok(())
}

pub fn apply_delete(
    uid: u64,
    edna: &mut EdnaClient,
) -> Result<()> {
    let mut db = edna.get_conn()?;
    let mut db2 = edna.get_conn()?;
    db.query_drop(&format!("DELETE FROM users WHERE id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hat_requests WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hats WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM hidden_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM invitations WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM read_ribbons WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM saved_stories WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM suggested_taggings WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM suggested_titles WHERE user_id={}", uid))?;
    db.query_drop(&format!("DELETE FROM tag_filters WHERE user_id={}", uid))?;

    db.query_drop(&format!("UPDATE comments SET comment='dummy text' WHERE user_id={}", uid))?;
    db.query_drop(&format!("UPDATE comments SET markeddown_comment='dummy text' WHERE user_id={}", uid))?;
    let res = db.query_iter(&format!("SELECT id FROM comments WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE comments SET user_id={} WHERE id={}", new_user, id))?;
    }


    db.query_drop(&format!("UPDATE stories SET url='dummy url' WHERE user_id={}", uid))?;
    db.query_drop(&format!("UPDATE stories SET title='dummy title' WHERE user_id={}", uid))?;
    let res = db.query_iter(&format!("SELECT id FROM stories WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE stories SET user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM messages WHERE author_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE messages SET author_user_id={} WHERE id={}", new_user, id))?;
    }
    let res = db.query_iter(&format!("SELECT id FROM messages WHERE recipient_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE messages SET recipient_user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM mod_notes WHERE moderator_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE mod_notes SET moderator_user_id={} WHERE id={}", new_user, id))?;
    }
    let res = db.query_iter(&format!("SELECT id FROM mod_notes WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE mod_notes SET user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM moderations WHERE moderator_user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE moderations SET moderator_user_id={} WHERE id={}", new_user, id))?;
    }
    let res = db.query_iter(&format!("SELECT id FROM moderations WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE moderations SET user_id={} WHERE id={}", new_user, id))?;
    }

    let res = db.query_iter(&format!("SELECT id FROM votes WHERE user_id = {}", uid))?;
    for row in res {
        let id : u64 = from_value(row.unwrap().unwrap()[0].clone());
        let new_user = insert_new_user(&mut db2);
        db2.query_drop(&format!("UPDATE votes SET user_id={} WHERE id={}", new_user, id))?;
    }

    Ok(())
}
