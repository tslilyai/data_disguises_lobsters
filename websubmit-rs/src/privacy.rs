use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::disguises;
use crate::email;
use chrono::prelude::*;
use mysql::from_value;
use mysql::Value;
use rocket::form::{Form, FromForm};
use rocket::http::{Cookie, CookieJar};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[derive(Serialize)]
pub(crate) struct LectureQuestion {
    pub id: u64,
    pub prompt: String,
    pub answer: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct LectureQuestionsContext {
    pub lec_id: u8,
    pub questions: Vec<LectureQuestion>,
    pub parent: &'static str,
}

#[derive(Serialize)]
pub struct LectureListEntry {
    id: u64,
    olc: String,
    label: String,
    num_qs: u64,
    num_answered: u64,
}

#[derive(Serialize)]
pub struct LectureListContext {
    lectures: Vec<LectureListEntry>,
    parent: &'static str,
}

#[derive(Debug, FromForm)]
pub(crate) struct RestoreRequest {
    decryption_cap: String,
    diff_loc_cap: u64,
    ownership_loc_cap: u64,
}

#[derive(Debug, FromForm)]
pub(crate) struct EditAnswerRequest {
    decryption_cap: String,
    ownership_loc_cap: String,
}

/*
 * ANONYMIZATION
 */
#[post("/")]
pub(crate) fn anonymize_answers(
    _adm: Admin,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let anon_disguise = Arc::new(disguises::universal_anon_disguise::get_disguise());
    let (dlcs, olcs) = bg
        .edna
        .apply_disguise(anon_disguise.clone(), vec![], vec![])
        .unwrap();
    assert!(dlcs.len() == 0);
    let local: DateTime<Local> = Local::now();
    for ((uid, _did), olc) in olcs {
        email::send(
            bg.log.clone(),
            "no-reply@csci2390-submit.cs.brown.edu".into(),
            vec![uid],
            "Your Websubmit Answers Have Been Anonymized".into(),
            format!(
                "Your data has been
                anonymized! To edit your answers submitted before {}.{}.{}, and after prior
                anonymizations, please click http://localhost:8000/edit/{}",
                local.year(),
                local.month(),
                local.day(),
                olc
            ),
        )
        .expect("failed to send email");
    }
    drop(bg);

    Redirect::to(format!("/leclist"))
}

#[get("/")]
pub(crate) fn anonymize(_adm: Admin) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("admin/anonymize", &ctx)
}

#[get("/<olc>")]
pub(crate) fn edit_decor(
    olc: u64,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Template {
    // get all lectures that potentially have answers
    let mut bg = backend.lock().unwrap();
    let res = bg.query_exec("leclist", vec![]);
    drop(bg);

    let lecs: Vec<_> = res
        .into_iter()
        .map(|r| LectureListEntry {
            olc: olc.to_string(),
            id: from_value(r[0].clone()),
            label: from_value(r[1].clone()),
            num_qs: if r[2] == mysql::Value::NULL {
                0u64
            } else {
                from_value(r[2].clone())
            },
            num_answered: 0u64,
        })
        .collect();

    let ctx = LectureListContext {
        lectures: lecs,
        parent: "layout",
    };
    Template::render("editdecor", &ctx)
}

#[post("/<num>", data = "<data>")]
pub(crate) fn edit_decor_lec(
    cookies: &CookieJar<'_>,
    num: u8,
    data: Form<EditAnswerRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Template {
    // TODO this doesn't let the original user edit their answer?
    
    let mut bg = backend.lock().unwrap();
    // get all the UIDs that this user can access
    let pps = bg.edna.get_pseudoprincipals(
        base64::decode(&data.decryption_cap).unwrap(),
        vec![u64::from_str(&data.ownership_loc_cap).unwrap()],
    );
    debug!(bg.log, "Got pps {:?}", pps); 
    // query for all answers (for all pps), choose the last updated one
    let key: Value = (num as u64).into();
    let now = Utc::now().naive_utc();
    let mut days_since = i64::MAX;
    let mut latest_user = String::new();
    let mut final_answers = HashMap::new();
    for pp in pps {
        let answers_res = bg.query_exec("my_answers_for_lec", vec![(num as u64).into(), pp.clone().into()]);
        let mut answers = HashMap::new();
        for r in answers_res {
            let id: u64 = from_value(r[2].clone());
            let atext: String = from_value(r[3].clone());
            let date: NaiveDateTime = from_value(r[4].clone());
            let my_days_since = now.signed_duration_since(date).num_days();
            debug!(bg.log, "Got date of {}, {} days before now", date, my_days_since);
            if my_days_since < days_since {
                days_since = my_days_since;
                latest_user = pp.clone();
            }
            answers.insert(id, atext);
        }
        if latest_user == pp {
            final_answers = answers;
        }
    }
    let res = bg.query_exec("qs_by_lec", vec![key]);
    debug!(bg.log, "Getting ApiKey of User {}",  latest_user.clone());
    let apikey_res = bg.query_exec("apikey_by_user", vec![latest_user.clone().into()]);
    let apikey: String = from_value(apikey_res[0][0].clone());
    drop(bg);

    let mut qs: Vec<LectureQuestion> = vec![];
    for r in res {
        let id: u64 = from_value(r[1].clone());
        let answer = final_answers.get(&id).map(|s| s.to_owned());
        if answer == None {
            continue;
        } 
        qs.push(LectureQuestion {
            id: id,
            prompt: from_value(r[2].clone()),
            answer: answer,
        });
    }
    qs.sort_by(|a, b| a.id.cmp(&b.id));

    let ctx = LectureQuestionsContext {
        lec_id: num,
        questions: qs,
        parent: "layout",
    };

    // this just lets the user act as the latest pseudoprincipal
    // but it won't reset afterward.... so the user won't be able to do anything else
    let cookie = Cookie::build("apikey", apikey.clone()).path("/").finish();
    cookies.add(cookie);

    Template::render("questions", &ctx)
}

/*
 * GDPR deletion
 */
#[post("/")]
pub(crate) fn delete(apikey: ApiKey, backend: &State<Arc<Mutex<MySqlBackend>>>) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let gdpr_disguise = Arc::new(disguises::gdpr_disguise::get_disguise(apikey.user.clone()));
    let (dlcs, olcs) = bg
        .edna
        .apply_disguise(gdpr_disguise.clone(), vec![], vec![])
        .unwrap();
    assert!(dlcs.len() <= 1);
    assert!(olcs.len() <= 1);
    debug!(bg.log, "Got DLCs {:?} and OLCS {:?}", dlcs, olcs);
    let dlc_str = match dlcs.get(&(apikey.user.clone(), gdpr_disguise.did)) {
        Some(dlc) => dlc.to_string(),
        None => 0.to_string(),
    };
    let olc_str = match olcs.get(&(apikey.key.clone(), gdpr_disguise.did)) {
        Some(olc) => olc.to_string(),
        None => 0.to_string(),
    };

    email::send(
        bg.log.clone(),
        "no-reply@csci2390-submit.cs.brown.edu".into(),
        vec![apikey.user.clone()],
        "You Have Deleted Your Websubmit Account".into(),
        format!("You have successfully deleted your account! To restore your account, please click http://localhost:8000/restore/{}/{}", 
            dlc_str, olc_str)
    )
    .expect("failed to send email");
    drop(bg);

    Redirect::to(format!("/login"))
}

#[get("/<diff_loc_cap>/<ownership_loc_cap>")]
pub(crate) fn restore(diff_loc_cap: u64, ownership_loc_cap: u64) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("DIFFLC", diff_loc_cap.to_string());
    ctx.insert("OWNLC", ownership_loc_cap.to_string());
    ctx.insert("parent", String::from("layout"));
    Template::render("restore", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn restore_account(
    data: Form<RestoreRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let decryption_cap: Vec<u8> = base64::decode(&data.decryption_cap)
        .expect("Bad decryption capability in post request");
    let olcs = if data.ownership_loc_cap != 0 {
        vec![data.ownership_loc_cap]
    } else {
        vec![]
    };
    bg.edna
        .reverse_disguise(
            disguises::gdpr_disguise::get_disguise_id(),
            decryption_cap,
            vec![data.diff_loc_cap],
            olcs,
        )
        .expect("Failed to reverse GDPR deletion disguise");
    drop(bg);

    Redirect::to(format!("/login"))
}
