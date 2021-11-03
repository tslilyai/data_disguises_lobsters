use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::disguises;
use crate::email;
use chrono::prelude::*;
use mysql::from_value;
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
    label: String,
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
pub(crate) struct EditCapabilitiesRequest {
    decryption_cap: String,
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
    let (dlcs, olcs) =
        disguises::universal_anon_disguise::apply(&mut bg).unwrap();
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
pub(crate) fn edit_as_pseudoprincipal(cookies: &CookieJar<'_>, olc: u64) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));

    // save olc
    let cookie = Cookie::new("olc", olc.to_string());
    //cookies.add_private(cookie);
    cookies.add(cookie);

    Template::render("edit_as_pseudoprincipal/get_decryption_cap", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn edit_as_pseudoprincipal_lecs(
    cookies: &CookieJar<'_>,
    data: Form<EditCapabilitiesRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Template {
    let cookie = Cookie::new("decryptioncap", data.decryption_cap.to_string());
    //cookies.add_private(cookie);
    cookies.add(cookie);

    let mut bg = backend.lock().unwrap();
    let res = bg.query_exec("leclist", vec![]);
    drop(bg);

    let lecs: Vec<_> = res
        .into_iter()
        .map(|r| LectureListEntry {
            id: from_value(r[0].clone()),
            label: from_value(r[1].clone()),
        })
        .collect();

    let ctx = LectureListContext {
        lectures: lecs,
        parent: "layout",
    };
    Template::render("edit_as_pseudoprincipal/lectures", &ctx)
}

#[get("/<lid>")]
pub(crate) fn edit_lec_answers_as_pseudoprincipal(
    cookies: &CookieJar<'_>,
    lid: u64,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Template {
    let decryption_cap = cookies.get("decryptioncap").unwrap().value();
    let olc: u64 = u64::from_str(cookies.get("olc").unwrap().value()).unwrap();

    let mut bg = backend.lock().unwrap();
    // get all the UIDs that this user can access
    let pps = bg
        .edna
        .get_pseudoprincipals(base64::decode(decryption_cap).unwrap(), vec![olc]);
    debug!(bg.log, "Got pps {:?}", pps);

    // get all answers for lectures
    let mut answers = HashMap::new();
    let mut apikey = String::new();
    for pp in pps {
        let answers_res = bg.query_exec("answers_by_user", vec![pp.clone().into()]);
        if !answers_res.is_empty() {
            for r in answers_res {
                let qid: u64 = from_value(r[2].clone());
                let atext: String = from_value(r[3].clone());
                answers.insert(qid, atext);
            }
            debug!(bg.log, "Getting ApiKey of User {}", pp.clone());
            let apikey_res = bg.query_exec("apikey_by_user", vec![pp.clone().into()]);
            apikey = from_value(apikey_res[0][0].clone());
            break;
        }
    }

    let res = bg.query_exec("qs_by_lec", vec![lid.into()]);
    drop(bg);
    let mut qs: Vec<LectureQuestion> = vec![];
    for r in res {
        let qid: u64 = from_value(r[1].clone());
        let answer = answers.get(&qid).map(|s| s.to_owned());
        if answer == None {
            continue;
        }
        qs.push(LectureQuestion {
            id: qid,
            prompt: from_value(r[2].clone()),
            answer: answer,
        });
    }
    qs.sort_by(|a, b| a.id.cmp(&b.id));

    let ctx = LectureQuestionsContext {
        lec_id: lid as u8,
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
    // TODO composition
    let (dlcs, olcs) =
        disguises::gdpr_disguise::apply(&mut bg, apikey.user.clone(), vec![], vec![])
            .unwrap();
    assert!(dlcs.len() <= 1);
    assert!(olcs.len() <= 1);
    debug!(bg.log, "Got DLCs {:?} and OLCS {:?}", dlcs, olcs);
    let dlc_str = match dlcs.get(&(apikey.user.clone(), disguises::gdpr_disguise::get_did())) {
        Some(dlc) => dlc.to_string(),
        None => 0.to_string(),
    };
    let olc_str = match olcs.get(&(apikey.key.clone(), disguises::gdpr_disguise::get_did())) {
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
    let decryption_cap: Vec<u8> =
        base64::decode(&data.decryption_cap).expect("Bad decryption capability in post request");
    let olcs = if data.ownership_loc_cap != 0 {
        vec![data.ownership_loc_cap]
    } else {
        vec![]
    };
    disguises::gdpr_disguise::reveal(&mut bg, decryption_cap, vec![data.diff_loc_cap], olcs)
        .expect("Failed to reverse GDPR deletion disguise");
    drop(bg);

    Redirect::to(format!("/login"))
}
