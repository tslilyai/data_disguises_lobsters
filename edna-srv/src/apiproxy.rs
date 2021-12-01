use edna::EdnaClient;
use rocket::serde::{json::Json, Deserialize};
use rocket::State;
use rsa::pkcs1::ToRsaPrivateKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Serialize)]
pub struct RegisterPrincipalResponse {
    // base64-encoded private key
    privkey: String,
}

#[post("/register_principal", format = "json", data = "<data>")]
pub(crate) fn register_principal(
    data: Json<edna::UID>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<RegisterPrincipalResponse> {
    let mut e = edna.lock().unwrap();
    let privkey = e.register_principal(data.to_owned());
    let privkey_str = base64::encode(&privkey.to_pkcs1_der().unwrap().as_der().to_vec());
    return Json(RegisterPrincipalResponse {
        privkey: privkey_str,
    });
}

#[get("/start_disguise/<did>")]
pub(crate) fn start_disguise(did: edna::DID, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let e = edna.lock().unwrap();
    e.start_disguise(did)
}

#[derive(Serialize)]
pub struct EndDisguiseResponse {
    diff_locators: HashMap<(edna::UID, edna::DID), edna::tokens::LocCap>,
    ownership_locators: HashMap<(edna::UID, edna::DID), edna::tokens::LocCap>,
}

#[get("/end_disguise/<did>")]
pub(crate) fn end_disguise(
    did: edna::DID,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<EndDisguiseResponse> {
    let e = edna.lock().unwrap();
    let locators = e.end_disguise(did);
    return Json(EndDisguiseResponse {
        diff_locators: locators.0,
        ownership_locators: locators.1,
    });
}

#[derive(Deserialize)]
pub struct GetPseudoprincipals {
    decrypt_cap: edna::tokens::DecryptCap,
    ownership_locators: Vec<edna::tokens::LocCap>,
}

#[post("/get_pseudoprincipals_of", format = "json", data = "<data>")]
pub(crate) fn get_pseudoprincipals_of(
    data: Json<GetPseudoprincipals>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<Vec<edna::UID>> {
    let e = edna.lock().unwrap();
    // TODO(malte): cloning here because get_pseudoprincipals expects owned caps; may be fine to
    // use refs in the Edna API here?
    let pps = e.get_pseudoprincipals(data.decrypt_cap.clone(), data.ownership_locators.clone());
    return Json(pps);
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
