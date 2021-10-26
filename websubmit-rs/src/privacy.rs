use crate::admin::Admin;
use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::disguises;
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

/*
 * GDPR deletion
 */
#[post("/")]
pub(crate) fn delete(apikey: ApiKey, backend: &State<Arc<Mutex<MySqlBackend>>>) -> Redirect {
    let mut bg = backend.lock().unwrap();
    let gdpr_disguise = Arc::new(disguises::gdpr_disguise::get_disguise(apikey.user));
    let (_dlcs, _olcs) = bg
        .edna
        .apply_disguise(gdpr_disguise.clone(), vec![], vec![])
        .unwrap();
    // TODO email stuff
    drop(bg);

    Redirect::to(format!("/login"))
}
