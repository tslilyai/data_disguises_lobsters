use crate::backend::MySqlBackend;
use crate::disguises::*;
use edna::*;
use mysql::from_value;
use mysql::prelude::*;
use mysql::*;
use std::collections::HashMap;
use std::time;

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
    is_baseline: bool,
) -> Result<(
    HashMap<(UID, DID), tokens::LocCap>,
    HashMap<(UID, DID), tokens::LocCap>,
)> {
    // DECOR ANSWERS
    if !is_baseline {
        bg.edna.start_disguise(get_did());
    }

    // get all answers sorted by user and lecture
    let mut user_lec_answers: HashMap<(String, u64), Vec<u64>> = HashMap::new();
    #[cfg(feature = "flame_it")]
    flame::start("DB: get_answers");

    let start = time::Instant::now();
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
    info!(bg.log, "get answers: {}", start.elapsed().as_millis());

    #[cfg(feature = "flame_it")]
    flame::end("DB: get_answers");

    let mut users = vec![];
    let mut updates = vec![];
    for ((user, lecture), qs) in user_lec_answers {
        let new_uid: String;
        if !is_baseline {
            // insert a new pseudoprincipal
            #[cfg(feature = "flame_it")]
            flame::start("EDNA: create_pseudoprincipal");
            let start = time::Instant::now();
            let p = bg.edna.create_new_pseudoprincipal();
            new_uid = p.0;
            let rowvals = p.1;
            info!(
                bg.log,
                "create pseudoprincipal: {}",
                start.elapsed().as_millis()
            );
            #[cfg(feature = "flame_it")]
            flame::end("EDNA: create_pseudoprincipal");

            // XXX issue where using bg adds quotes everywhere...
            users.push(format!(
                "({}, {}, {}, {})",
                rowvals[0].value, rowvals[1].value, rowvals[2].value, rowvals[3].value,
            ));

            // register new ownershiptoken for pseudoprincipal
            #[cfg(feature = "flame_it")]
            flame::start("ENDA: save_pseudoprincipal");
            let start = time::Instant::now();
            bg.edna
                .save_pseudoprincipal_token(get_did(), user.clone(), new_uid.clone(), vec![]);
            warn!(
                bg.log,
                "save pseudoprincipals: {}",
                start.elapsed().as_millis()
            );
            #[cfg(feature = "flame_it")]
            flame::end("ENDA: save_pseudoprincipal");
        } else {
            let rowvals = get_insert_guise_vals();
            users.push(format!(
                "({}, {}, {}, {})",
                rowvals[0], rowvals[1], rowvals[2], rowvals[3],
            ));
            new_uid = rowvals[0].to_string();
        }

        // rewrite answers for all qs to point from user to new pseudoprincipal
        for q in qs {
            updates.push(UpdateFK {
                new_uid: new_uid.trim_matches('\'').to_string(),
                user: user.trim_matches('\'').to_string(),
                lec: lecture,
                q: q,
            });
        }
    }

    #[cfg(feature = "flame_it")]
    flame::start("DB: insert pseudos");
    let start = time::Instant::now();
    bg.handle
        .query_drop(&format!(r"INSERT INTO `users` VALUES {};", users.join(",")))?;
    warn!(
        bg.log,
        "insert pseudoprincipals: {}",
        start.elapsed().as_millis()
    );
    #[cfg(feature = "flame_it")]
    flame::end("DB: insert pseudos");

    #[cfg(feature = "flame_it")]
    flame::start("DB: update_answers");
    let start = time::Instant::now();
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
    warn!(
        bg.log,
        "update {} fks: {}",
        updates.len(),
        start.elapsed().as_millis()
    );
    #[cfg(feature = "flame_it")]
    flame::end("DB: update_answers");

    if is_baseline {
        Ok((HashMap::new(), HashMap::new()))
    } else {
        Ok(bg.edna.end_disguise(get_did()))
    }
}

// we don't need to reveal
