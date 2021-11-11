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
    #[cfg(feature = "flame_it")]
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
    #[cfg(feature = "flame_it")]
    flame::end("DB: get_answers");

    let mut users = vec![];
    let mut updates = vec![];
    for ((user, lecture), qs) in user_lec_answers {
        // insert a new pseudoprincipal
        #[cfg(feature = "flame_it")]
        flame::start("EDNA: create_pseudoprincipal");
        let (new_uid, rowvals) = bg.edna.create_new_pseudoprincipal();
        #[cfg(feature = "flame_it")]
        flame::end("EDNA: create_pseudoprincipal");

        // XXX issue where using bg adds quotes everywhere...
        users.push(format!(
            "('{}', {}, {}, {})",
            rowvals[0].value,
            rowvals[1].value,
            rowvals[2].value,
            rowvals[3].value,
        ));

        // rewrite answers for all qs to point from user to new pseudoprincipal
        for q in qs {
            updates.push(UpdateFK {
                new_uid: new_uid.trim_matches('\'').to_string(),
                user: user.trim_matches('\'').to_string(),
                lec: lecture,
                q: q,
            });
        }

        // register new ownershiptoken for pseudoprincipal
        #[cfg(feature = "flame_it")]
        flame::start("ENDA: save_pseudoprincipal");
        bg.edna
            .save_pseudoprincipal_token(get_did(), user, new_uid, vec![]);
        #[cfg(feature = "flame_it")]
        flame::end("ENDA: save_pseudoprincipal");
    }

    #[cfg(feature = "flame_it")]
    flame::start("DB: insert pseudos");
    bg.handle
        .query_drop(&format!(r"INSERT INTO `users` VALUES {};", users.join(",")))?;
    #[cfg(feature = "flame_it")]
    flame::end("DB: insert pseudos");

    #[cfg(feature = "flame_it")]
    flame::start("DB: update_answers");
    bg.handle.exec_batch(
        r"UPDATE answers SET `user` = :newuid WHERE `user` = :user AND lec = :lec AND q = :q;",
        updates.iter().map(|u| {
            params! {
                "newuid" => &u.new_uid,
                "user" => &u.user,
                "lec" => u.lec,
                "q" => u.q,
            }
        }),
    )?;
    #[cfg(feature = "flame_it")]
    flame::end("DB: update_answers");

    Ok(bg.edna.end_disguise(get_did()))
}

// we don't need to reveal
