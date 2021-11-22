use crate::backend::MySqlBackend;
use crate::disguises::*;
use edna::*;
use mysql::from_value;
use mysql::prelude::*;
use mysql::TxOpts;
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
    bg: &MySqlBackend,
    is_baseline: bool,
) -> Result<(
    HashMap<(UID, DID), tokens::LocCap>,
    HashMap<(UID, DID), tokens::LocCap>,
)> {
    // DECOR ANSWERS
    let mut locators = (HashMap::new(), HashMap::new());
    
    // TODO prevent new users from joining
    
    let mut conn = bg.handle();
    let mut users = vec![];
    let res = conn.query_iter("SELECT email FROM users WHERE is_anon = 0;")?;
    for r in res {
        let r = r.unwrap().unwrap();
        let uid: String = from_value(r[0].clone());
        users.push(uid);
    }
   
    for u in users {
        // TODO lock user account
        let mut txn = conn.start_transaction(TxOpts::default())?;
       
        let start = time::Instant::now();
        if !is_baseline {
            let edna = bg.edna.lock().unwrap();
            edna.start_disguise(get_did());
            drop(edna);
        }
        
        // get all answers sorted by user and lecture
        let mut user_lec_answers: HashMap<u64, Vec<u64>> = HashMap::new();
        #[cfg(feature = "flame_it")]
        flame::start("DB: get_answers");
        let res = txn.query_iter(&format!("SELECT lec, q FROM answers WHERE `user` = '{}';", u))?;
        for r in res {
            let r = r.unwrap().unwrap();
            let key: u64 = from_value(r[0].clone());
            let val: u64 = from_value(r[1].clone());
            match user_lec_answers.get_mut(&key) {
                Some(qs) => qs.push(val),
                None => {
                    user_lec_answers.insert(key, vec![val]);
                }
            };
        }
        info!(bg.log, "get answers: {}", start.elapsed().as_micros());

        #[cfg(feature = "flame_it")]
        flame::end("DB: get_answers");

        let mut updates = vec![];
        let mut pps = vec![];
        for (lecture, qs) in user_lec_answers {
            let new_uid: String;
            if !is_baseline {
                // insert a new pseudoprincipal
                #[cfg(feature = "flame_it")]
                flame::start("EDNA: create_pseudoprincipal");
                let start = time::Instant::now();
                let mut edna = bg.edna.lock().unwrap();
                let p = edna.create_new_pseudoprincipal();
                drop(edna);
                new_uid = p.0;
                let rowvals = p.1;
                info!(
                    bg.log,
                    "create pseudoprincipal: {}",
                    start.elapsed().as_micros()
                );
                #[cfg(feature = "flame_it")]
                flame::end("EDNA: create_pseudoprincipal");

                // XXX issue where using bg adds quotes everywhere...
                pps.push(format!(
                    "({}, {}, {}, {})",
                    rowvals[0].value, rowvals[1].value, rowvals[2].value, rowvals[3].value,
                ));

                // register new ownershiptoken for pseudoprincipal
                #[cfg(feature = "flame_it")]
                flame::start("ENDA: save_pseudoprincipal");
                let start = time::Instant::now();
                let edna = bg.edna.lock().unwrap();
                edna.save_pseudoprincipal_token(
                    get_did(),
                    u.clone(),
                    new_uid.clone(),
                    vec![],
                    &mut txn,
                );
                drop(edna);
                warn!(
                    bg.log,
                    "save pseudoprincipals: {}",
                    start.elapsed().as_micros()
                );
                #[cfg(feature = "flame_it")]
                flame::end("ENDA: save_pseudoprincipal");
            } else {
                let rowvals = get_insert_guise_vals();
                pps.push(format!(
                    "({}, {}, {}, {})",
                    rowvals[0], rowvals[1], rowvals[2], rowvals[3],
                ));
                new_uid = rowvals[0].to_string();
            }

            // rewrite answers for all qs to point from user to new pseudoprincipal
            for q in qs {
                updates.push(UpdateFK {
                    new_uid: new_uid.trim_matches('\'').to_string(),
                    user: u.trim_matches('\'').to_string(),
                    lec: lecture,
                    q: q,
                });
            }
        }

        if !pps.is_empty() {
            #[cfg(feature = "flame_it")]
            flame::start("DB: insert pseudos");
            warn!(
                bg.log,
                "Query: {}",
                &format!(r"INSERT INTO `users` VALUES {};", pps.join(","))
            );
            let start = time::Instant::now();
            txn.query_drop(&format!(r"INSERT INTO `users` VALUES {};", pps.join(",")))?;
            warn!(
                bg.log,
                "insert pseudoprincipals: {}",
                start.elapsed().as_micros()
            );
            #[cfg(feature = "flame_it")]
            flame::end("DB: insert pseudos");

            #[cfg(feature = "flame_it")]
            flame::start("DB: update_answers");
            let start = time::Instant::now();
            txn.exec_batch(
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
                start.elapsed().as_micros()
            );
            #[cfg(feature = "flame_it")]
            flame::end("DB: update_answers");
        }

        if !is_baseline {
            let edna = bg.edna.lock().unwrap();
            let res = edna.end_disguise(get_did(), &mut txn);
            drop(edna);
            locators.0.extend(&mut res.0.into_iter());
            locators.1.extend(&mut res.1.into_iter());
        }
        txn.commit()?;
    }
    Ok(locators)
}

// we don't need to reveal
