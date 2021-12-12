use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::config::Config;
use crate::disguises;
use crate::email;
//use chrono::prelude::*;
use mysql::from_value;
use rocket::form::{Form, FromForm};
use rocket::http::{Cookie, CookieJar};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::{HashMap};
use edna::tokens::LocCap;

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
    loc_caps: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct DeleteRequest {
    decryption_cap: String,
    loc_caps: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct EditCapabilitiesRequest {
    decryption_cap: String,
    loc_caps: String,
}

/*
 * ANONYMIZATION
 */
#[post("/")]
pub(crate) fn anonymize_answers(
    _adm: Admin,
    bg: &State<MySqlBackend>,
    config: &State<Config>,
) -> Redirect {
    let olcs = disguises::universal_anon_disguise::apply(&*bg, config.is_baseline).unwrap();
    warn!(bg.log, "olcs are {:?}", olcs);
    //let local: DateTime<Local> = Local::now();
    for ((uid, _did), olcs) in olcs {
        assert!(!olcs.is_empty());
        email::send(
            bg.log.clone(),
            "no-reply@csci2390-submit.cs.brown.edu".into(),
            vec![uid],
            "Your Websubmit Answers Have Been Anonymized".into(),
            format!("CAPS#{}", serde_json::to_string(&olcs).unwrap()),
        )
        .expect("failed to send email");
    }

    Redirect::to(format!("/leclist"))
}

#[get("/")]
pub(crate) fn anonymize(_adm: Admin) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("admin/anonymize", &ctx)
}

#[get("/")]
pub(crate) fn edit_as_pseudoprincipal() -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("edit_as_pseudoprincipal/get_decryption_cap", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn edit_as_pseudoprincipal_lecs(
    cookies: &CookieJar<'_>,
    data: Form<EditCapabilitiesRequest>,
    bg: &State<MySqlBackend>,
) -> Template {
    let cookie = Cookie::new("decryptioncap", data.decryption_cap.to_string());
    cookies.add(cookie);
    let res = bg.query_exec("leclist", vec![]);

    // save lcs
    let cookie = Cookie::new("lcs", data.loc_caps.to_string());
    cookies.add(cookie);

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
    bg: &State<MySqlBackend>,
) -> Template {
    let decryption_cap = cookies.get("decryptioncap").unwrap().value();
    let lcs: Vec<LocCap> = serde_json::from_str(&cookies.get("lcs").unwrap().value()).unwrap();
    let edna = bg.edna.lock().unwrap();
    
    // get all the UIDs that this user can access
    let pps = edna
        .get_pseudoprincipals(base64::decode(decryption_cap).unwrap(), lcs);
    warn!(bg.log, "Got pps {:?}", pps);
    drop(edna);

    // get all answers for lectures
    let mut answers = HashMap::new();
    let mut apikey = String::new();
    for pp in pps {
        let answers_res = bg.query_exec("my_answers_for_lec", vec![lid.into(), pp.clone().into()]);
        warn!(bg.log, "Got answers of user {}: {:?}", pp, answers_res);
        if !answers_res.is_empty() {
            for r in answers_res {
                let qid: u64 = from_value(r[2].clone());
                let atext: String = from_value(r[3].clone());
                answers.insert(qid, atext);
            }
            warn!(bg.log, "Getting ApiKey of User {}", pp.clone());
            let apikey_res = bg.query_exec("apikey_by_user", vec![pp.clone().into()]);
            apikey = from_value(apikey_res[0][0].clone());
            break;
        }
    }

    let res = bg.query_exec("qs_by_lec", vec![lid.into()]);
    warn!(bg.log, "Setting API key to user key {}", apikey);
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
    cookies.remove(Cookie::named("decryptioncap"));
    cookies.remove(Cookie::named("lcs"));
    let cookie = Cookie::build("apikey", apikey.clone()).path("/").finish();
    cookies.add(cookie);

    Template::render("questions", &ctx)
}

/*
 * GDPR deletion
 */
#[get("/")]
pub(crate) fn delete() -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("delete", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn delete_submit(
    apikey: ApiKey,
    data: Form<DeleteRequest>,
    bg: &State<MySqlBackend>,
    config: &State<Config>,
) -> Redirect {
    let decryption_cap: Vec<u8> = if !config.is_baseline {
        base64::decode(&data.decryption_cap).expect("Bad decryption capability in post request")
    } else {
        vec![]
    };

    warn!(bg.log, "Going to deserialize lcs {}", data.loc_caps);
    // get caps from data for composition of GDPR on top of anonymization
    let loc_caps: Vec<LocCap> = serde_json::from_str(&data.loc_caps).unwrap();
    let lcsmap = disguises::gdpr_disguise::apply(
        &*bg,
        apikey.user.clone(),
        decryption_cap,
        loc_caps,
        config.is_baseline,
    )
    .unwrap();
    let mut lcs = vec![];
    for ((_uid, _did), mut lcsofuser) in lcsmap {
        lcs.append(&mut lcsofuser);
    }

    email::send(
        bg.log.clone(),
        "no-reply@csci2390-submit.cs.brown.edu".into(),
        vec![apikey.user.clone()],
        "You Have Deleted Your Websubmit Account".into(),
        format!("CAPS#{}", serde_json::to_string(&lcs).unwrap()),
        //"You have successfully deleted your account! To restore your account, please click http://localhost:8000/restore/{}",
    )
    .expect("failed to send email");

    Redirect::to(format!("/login"))
}

#[get("/<loc_caps>")]
pub(crate) fn restore(loc_caps: String) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("LC", loc_caps);
    ctx.insert("parent", String::from("layout"));
    Template::render("restore", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn restore_account(
    data: Form<RestoreRequest>,
    bg: &State<MySqlBackend>,
    config: &State<Config>,
) -> Redirect {
    let decryption_cap: Vec<u8> =
        base64::decode(&data.decryption_cap).expect("Bad decryption capability in post request");
    let loc_caps: Vec<LocCap> = serde_json::from_str(&data.loc_caps).unwrap();

    disguises::gdpr_disguise::reveal(
        &*bg,
        decryption_cap,
        loc_caps,
        config.is_baseline,
    )
    .expect("Failed to reverse GDPR deletion disguise");

    Redirect::to(format!("/login"))
}
