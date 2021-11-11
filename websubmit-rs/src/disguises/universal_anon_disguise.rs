use crate::backend::MySqlBackend;
use edna::*;
use mysql::from_value;
use mysql::prelude::*;
use std::collections::HashMap;

pub fn get_did() -> DID {
    1
}

pub fn apply(
    bg: &mut MySqlBackend,
) -> Result<
    (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ),
    mysql::Error,
> {
    // DECOR ANSWERS
    bg.edna.start_disguise(get_did());

    // get all answers sorted by user and lecture
    let mut user_lec_answers: HashMap<(String, u64), Vec<u64>> = HashMap::new();
    flame::start("get_answers");
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
    flame::end("get_answers");

    for ((user, lecture), qs) in user_lec_answers {
        // insert a new pseudoprincipal
        flame::start("create_pseudoprincipal");
        let (new_uid, rowvals) = bg.edna.create_new_pseudoprincipal();
        flame::end("create_pseudoprincipal");

        // XXX issue where using bg adds quotes everywhere...
        flame::start("insert_pseudoprincipal");
        let q = format!(
            r"INSERT INTO {} VALUES ({});",
            "users",
            rowvals
                .iter()
                .map(|rv| rv.value.clone())
                .collect::<Vec<String>>()
                .join(",")
        );
        bg.handle.query_drop(q).unwrap();
        flame::end("insert_pseudoprincipal");

        // rewrite answers for all qs to point from user to new pseudoprincipal
        flame::start("update_answers");
        for q in qs {
            let q = format!(
                r"UPDATE {} SET `user` = {} WHERE `user` = '{}' AND lec = {} AND q = {};",
                "answers", new_uid, user, lecture, q
            );
            bg.handle.query_drop(q).unwrap();
        }
        flame::end("update_answers");

        // register new ownershiptoken for pseudoprincipal
        flame::start("save_pseudoprincipal");
        bg.edna
            .save_pseudoprincipal_token(get_did(), user, new_uid, vec![]);
        flame::end("save_pseudoprincipal");
    }

    Ok(bg.edna.end_disguise(get_did()))
}

// we don't need to reveal
