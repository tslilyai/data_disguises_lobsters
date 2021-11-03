use crate::backend::MySqlBackend;
use edna::*;
use mysql::from_value;
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
    let mut user_lec_answers: HashMap<(String, String), Vec<String>> = HashMap::new();
    let res = bg.query_exec("all_answers", vec![]);
    for r in res {
        let key: (String, String) = (from_value(r[0].clone()), from_value(r[1].clone()));
        let val: String = from_value(r[2].clone());
        match user_lec_answers.get_mut(&key) {
            Some(qs) => qs.push(val),
            None => {
                user_lec_answers.insert(key, vec![val]);
            }
        };
    }

    for ((user, lecture), qs) in user_lec_answers {
        // insert a new pseudoprincipal
        let (new_uid, rowvals) = bg.edna.create_new_pseudoprincipal();
        bg.insert(
            "users",
            rowvals.iter().map(|rv| rv.value.clone().into()).collect(),
        );

        // rewrite answers for all qs to point from user to new pseudoprincipal
        for q in qs {
            bg.update(
                "answers",
                vec![
                    user.clone().into(),
                    lecture.clone().into(),
                    q.clone().into(),
                ],
                vec![(0, new_uid.clone().into())],
            );
        }

        // register new ownershiptoken for pseudoprincipal
        bg.edna.save_pseudoprincipal_token(get_did(), user, new_uid, vec![]);
    }

    Ok(bg.edna.end_disguise(get_did()))
}

// we don't need to reveal
