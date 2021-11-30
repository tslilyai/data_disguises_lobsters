use edna::EdnaClient;
use rocket::State;
use std::sync::{Arc, Mutex};

#[post("/register_principal")]
pub(crate) fn register_principal(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}

#[post("/start_disguise")]
pub(crate) fn start_disguise(edna: &State<Arc<Mutex<EdnaClient>>>) {
    let e = edna.lock().unwrap();
    e.start_disguise(1234)
}

#[post("/end_disguise")]
pub(crate) fn end_disguise(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}

#[post("/get_pseudoprincipals_of")]
pub(crate) fn get_pseudoprincipals_of(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}

#[post("/get_tokens_of_disguise")]
pub(crate) fn get_tokens_of_disguise(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}

#[post("/create_pseudoprincipal")]
pub(crate) fn create_pseudoprincipal(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}

#[post("/save_pseudoprincipal_token")]
pub(crate) fn save_pseudoprincipal_token(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}

#[post("/save_diff_token")]
pub(crate) fn save_diff_token(edna: &State<Arc<Mutex<EdnaClient>>>) {
    unimplemented!()
}
