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
    bg: &MySqlBackend,
    is_baseline: bool,
) -> Result<HashMap<(UID, DID), tokens::LocCap>> {
    // DECOR ANSWERS
    let mut locators = HashMap::new();
    
    // TODO prevent new users from joining
    
    let mut db = bg.handle();
    let mut users = vec![];
    let res = db.query_iter("SELECT email FROM users WHERE is_anon = 0;")?;
    for r in res {
        let r = r.unwrap().unwrap();
        let uid: String = from_value(r[0].clone());
        users.push(uid);
    }
   
    for u in users {
        // XXX transaction
        //let mut txn = db.start_transaction(TxOpts::default()).unwrap();
        let beginning_start = time::Instant::now();
        // TODO lock user account
       
        let start = time::Instant::now();
        if !is_baseline {
            let edna = bg.edna.lock().unwrap();
            edna.start_disguise(get_did());
            drop(edna);
        }
        
        // get all answers sorted by user and lecture
        let mut user_lec_answers: HashMap<u64, Vec<u64>> = HashMap::new();
        let res = db.query_iter(&format!("SELECT lec, q FROM answers WHERE `user` = '{}';", u))?;
        //let res = txn.query_iter(&format!("SELECT lec, q FROM answers WHERE `user` = '{}';", u))?;
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
        debug!(bg.log, "WSAnon: get answers: {}", start.elapsed().as_micros());

        let mut updates = vec![];
        let mut pps = vec![];
        for (lecture, qs) in user_lec_answers {
            let new_uid: String;
            if !is_baseline {
                // insert a new pseudoprincipal
                let start = time::Instant::now();
                let mut edna = bg.edna.lock().unwrap();
                let p = edna.create_new_pseudoprincipal();
                drop(edna);
                new_uid = p.0;
                let rowvals = p.1;
                debug!(
                    bg.log,
                    "WSAnon: create pseudoprincipal: {}",
                    start.elapsed().as_micros()
                );

                // XXX issue where using bg adds quotes everywhere...
                pps.push(format!(
                    "({}, {}, {}, {})",
                    rowvals[0].value, rowvals[1].value, rowvals[2].value, rowvals[3].value,
                ));

                // register new ownershiptoken for pseudoprincipal
                let start = time::Instant::now();
                let edna = bg.edna.lock().unwrap();
                edna.save_pseudoprincipal_token(
                    get_did(),
                    u.clone(),
                    new_uid.clone(),
                    vec![],
                );
                drop(edna);
                debug!(
                    bg.log,
                    "WSAnon: save pseudoprincipals: {}",
                    start.elapsed().as_micros()
                );
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
            let start = time::Instant::now();
            //txn.query_drop(&format!(r"INSERT INTO `users` VALUES {};", pps.join(",")))?;
            db.query_drop(&format!(r"INSERT INTO `users` VALUES {};", pps.join(",")))?;
            debug!(
                bg.log,
                "WSAnon: INSERT INTO `users` VALUES {};: {}",
                pps.join(","),
                start.elapsed().as_micros()
            );
            let start = time::Instant::now();
            //txn.exec_batch(
            db.exec_batch(
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
            debug!(
                bg.log,
                "WSAnon: update {} fks: {}",
                updates.len(),
                start.elapsed().as_micros()
            );
            
        }

        if !is_baseline {
            let edna = bg.edna.lock().unwrap();
            let res = edna.end_disguise(); 
            drop(edna);
            locators.extend(&mut res.into_iter());
        }
        debug!(
            bg.log,
            "WSAnon: total: {}",
            beginning_start.elapsed().as_micros()
        );
        //txn.commit().unwrap();
    }
    Ok(locators)
}

// we don't need to reveal
