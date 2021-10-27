use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::disguises;
use crate::email;
use rocket::form::{Form, FromForm};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    let (_dlcs, _olcs) = bg
        .edna
        .apply_disguise(anon_disguise.clone(), vec![], vec![])
        .unwrap();
    // TODO email stuff
    drop(bg);

    Redirect::to(format!("/leclist"))
}

#[get("/")]
pub(crate) fn anonymize(_adm: Admin) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("admin/anonymize", &ctx)
}

#[post("/<lecnum>", data = "<data>")]
pub(crate) fn edit_decor_answer(
    lecnum: u8,
    data: Form<EditAnswerRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    drop(bg);

    Redirect::to(format!("/login"))
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
        None => String::new(),
    };
    let olc_str = match olcs.get(&(apikey.key.clone(), gdpr_disguise.did)) {
        Some(olc) => olc.to_string(),
        None => String::new(),
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

    // TODO email stuff
    drop(bg);

    Redirect::to(format!("/login"))
}

#[get("/<diff_loc_cap>/<ownership_loc_cap>")]
pub(crate) fn restore_all(
    diff_loc_cap: u64,
    ownership_loc_cap: u64
) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("DIFFLC", diff_loc_cap.to_string());
    ctx.insert("OWNLC", ownership_loc_cap.to_string());
    ctx.insert("parent", String::from("layout"));
    Template::render("restore", &ctx)
}

#[get("/<diff_loc_cap>")]
pub(crate) fn restore_diff_only(
    diff_loc_cap: u64,
) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("DIFFLC", diff_loc_cap.to_string());
    ctx.insert("OWNLC", 0.to_string());
    ctx.insert("parent", String::from("layout"));
    Template::render("restore", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn restore_account(
    data: Form<RestoreRequest>,
    backend: &State<Arc<Mutex<MySqlBackend>>>,
) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let decryption_cap : Vec<u8> = serde_json::from_str(&data.decryption_cap).expect("Bad decryption capability in post request");
    let olcs = if data.ownership_loc_cap != 0 {
        vec![data.ownership_loc_cap]
    } else {
        vec![]
    };
    bg.edna.reverse_disguise(disguises::gdpr_disguise::get_disguise_id(), decryption_cap, vec![data.diff_loc_cap], olcs).expect("Failed to reverse GDPR deletion disguise");
    drop(bg);

    Redirect::to(format!("/login"))
}
