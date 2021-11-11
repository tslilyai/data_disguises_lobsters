use crate::backend::MySqlBackend;
use edna::*;
use mysql::from_value;
use mysql::prelude::*;
use mysql::*;
use std::collections::HashMap;

struct UpdateFK {
    new_uid: String,
    user: String,
    lec: u64,
    q: u64,
}

pub fn get_did() -> DID {
    1
}

pub fn apply(
    bg: &mut MySqlBackend,
) -> Result<(
    HashMap<(UID, DID), tokens::LocCap>,
    HashMap<(UID, DID), tokens::LocCap>,
)> {
    // DECOR ANSWERS
    bg.edna.start_disguise(get_did());

    // get all answers sorted by user and lecture
    let mut user_lec_answers: HashMap<(String, u64), Vec<u64>> = HashMap::new();
    flame::start("DB: get_answers");
    let res = bg.query_exec("all_answers", vec![]);
    for r in res {
        let uid: String = from_value(r[0].clone());
        let uidstr = uid.trim_matches('\'');
        let key: (String, u64) = (uidstr.to_string(), from_value(r[1].clone()));
        let val: u64 = from_value(r[2].clone());
        match user_lec_answers.get_mut(&key) {
            Some(qs) => qs.push(val),
            None => {
                user_lec_answers.insert(key, vec![val]);
            }
        };
    }
    flame::end("DB: get_answers");

    let mut users = vec![];
    let mut updates = vec![];
    for ((user, lecture), qs) in user_lec_answers {
        // insert a new pseudoprincipal
        flame::start("EDNA: create_pseudoprincipal");
        let (new_uid, rowvals) = bg.edna.create_new_pseudoprincipal();
        flame::end("EDNA: create_pseudoprincipal");

        // XXX issue where using bg adds quotes everywhere...
        users.push(
            rowvals
                .iter()
                .map(|rv| rv.value.clone())
                .collect::<Vec<String>>(),
        );

        // rewrite answers for all qs to point from user to new pseudoprincipal
        for q in qs {
            updates.push(UpdateFK {
                new_uid: new_uid.clone(),
                user: user.clone(),
                lec: lecture,
                q: q,
            });
        }

        // register new ownershiptoken for pseudoprincipal
        flame::start("ENDA: save_pseudoprincipal");
        bg.edna
            .save_pseudoprincipal_token(get_did(), user, new_uid, vec![]);
        flame::end("ENDA: save_pseudoprincipal");
    }

    flame::start("DB: insert pseudos");
    bg.handle.exec_batch(
        r"INSERT INTO `users` VALUES (:email, :apikey, :is_admin, :is_anon);",
        users.iter().map(|u| {
            params! {
                "email" => &u[0].trim_matches('\''),
                "apikey" => &u[1].trim_matches('\''),
                "is_admin" => &u[2],
                "is_anon" => &u[3],
            }
        }),
    )?;
    flame::end("DB: insert pseudos");

    flame::start("DB: update_answers");
    bg.handle.exec_batch(
        r"UPDATE answers SET `user` = :newuid WHERE `user` = :user AND lec = :lec AND q = :q;",
        updates.iter().map(|u| {
            params! {
                "newuid" => &u.new_uid.trim_matches('\''),
                "user" => &u.user.trim_matches('\''),
                "lec" => u.lec,
                "q" => u.q,
            }
        }),
    )?;
    flame::end("DB: update_answers");

    Ok(bg.edna.end_disguise(get_did()))
}

// we don't need to reveal
