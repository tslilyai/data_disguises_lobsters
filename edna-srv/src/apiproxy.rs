use crate::lobsters_disguises;
use crate::hotcrp_disguises;
use crate::*;
use edna::EdnaClient;
use rocket::serde::{json::Json, Deserialize};
use rocket::State;
use rsa::pkcs1::ToRsaPrivateKey;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use log::error;

/************************
 * High-Level API
 ************************/
#[derive(Deserialize)]
pub struct ApplyDisguise {
    decrypt_cap: edna::tokens::DecryptCap,
    ownership_locators: Vec<edna::tokens::LocCap>,
}
#[derive(Serialize, Deserialize)]
pub struct ApplyDisguiseResponse {
    pub diff_locators: HashMap<edna::UID, edna::tokens::LocCap>,
    pub ownership_locators: HashMap<edna::UID, edna::tokens::LocCap>,
}

#[post("/<app>/<did>/<uid>", format = "json", data = "<data>")]
pub(crate) fn apply_disguise(
    app: &str,
    did: edna::DID,
    uid: edna::UID,
    data: Json<ApplyDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<ApplyDisguiseResponse> {
    warn!("Applying disguise {} for {}", did, uid);
    let disguise = match app {
        LOBSTERS_APP => lobsters_disguises::get_disguise_with_ids(did, uid),
        HOTCRP_APP => hotcrp_disguises::get_disguise_with_ids(did, uid),
        _ => unimplemented!("unsupported app")
    };
    let mut e = edna.lock().unwrap();
    let locators = e
        .apply_disguise(
            disguise,
            data.decrypt_cap.clone(),
            data.ownership_locators.clone(),
        )
        .unwrap();
    let mut diff_locators = HashMap::new();
    let mut own_locators = HashMap::new();
    for ((uid, _), dl) in locators.0.iter() {
        diff_locators.insert(uid.clone(), *dl);
    }
    for ((uid, _), ol) in locators.1.iter() {
        own_locators.insert(uid.clone(), *ol);
    }
    warn!("Applying disguise return response {:?}", locators);
    let json = Json(ApplyDisguiseResponse {
        diff_locators: diff_locators,
        ownership_locators: own_locators,
    });
    return json;
}

#[derive(Deserialize)]
pub struct RevealDisguise {
    decrypt_cap: edna::tokens::DecryptCap,
    diff_locators: Vec<edna::tokens::LocCap>,
    ownership_locators: Vec<edna::tokens::LocCap>,
}

#[post("/<did>", format = "json", data = "<data>")]
pub(crate) fn reveal_disguise(
    did: edna::DID,
    data: Json<RevealDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) {
    let mut e = edna.lock().unwrap();
    // XXX clones
    e.reverse_disguise(
        did,
        data.decrypt_cap.clone(),
        data.diff_locators.clone(),
        data.ownership_locators.clone(),
    ).unwrap();
}

/************************
 * Low-Level API
 ************************/
#[derive(Serialize, Deserialize)]
pub struct RegisterPrincipalResponse {
    // base64-encoded private key
    pub privkey: String,
}

#[post("/", data = "<data>")]
pub(crate) fn register_principal(
    data: String, //Json<edna::UID>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<RegisterPrincipalResponse> {
    error!("Received JSON in regprin {:?}", data);
    let mut e = edna.lock().unwrap();
    let privkey = e.register_principal(&data.to_owned());
    let privkey_str = base64::encode(&privkey.to_pkcs1_der().unwrap().as_der().to_vec());
    return Json(RegisterPrincipalResponse {
        privkey: privkey_str,
    });
}

#[get("/<did>")]
pub(crate) fn start_disguise(did: edna::DID, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let e = edna.lock().unwrap();
    e.start_disguise(did)
}

#[derive(Serialize)]
pub struct EndDisguiseResponse {
    diff_locators: HashMap<edna::UID, edna::tokens::LocCap>,
    ownership_locators: HashMap<edna::UID, edna::tokens::LocCap>,
}

#[get("/<did>")]
pub(crate) fn end_disguise(
    did: edna::DID,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<EndDisguiseResponse> {
    let _ = did;
    let e = edna.lock().unwrap();
    let locators = e.end_disguise();
    let mut diff_locators = HashMap::new();
    let mut own_locators = HashMap::new();
    for ((uid, _), dl) in locators.0.iter() {
        diff_locators.insert(uid.clone(), *dl);
    }
    for ((uid, _), ol) in locators.1.iter() {
        own_locators.insert(uid.clone(), *ol);
    }
    return Json(EndDisguiseResponse {
        diff_locators: diff_locators,
        ownership_locators: own_locators,
    });
}

#[derive(Deserialize)]
pub struct GetPseudoprincipals {
    decrypt_cap: edna::tokens::DecryptCap,
    ownership_locators: Vec<edna::tokens::LocCap>,
    reveal: bool,
}

#[post("/", format = "json", data = "<data>")]
pub(crate) fn get_pseudoprincipals_of(
    data: Json<GetPseudoprincipals>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<Vec<edna::UID>> {
    let e = edna.lock().unwrap();
    // TODO(malte): cloning here because get_pseudoprincipals expects owned caps; may be fine to
    // use refs in the Edna API here?
    let pps = e.get_pseudoprincipals(data.decrypt_cap.clone(), data.ownership_locators.clone(), data.reveal);
    return Json(pps);
}

#[derive(Deserialize)]
pub struct GetTokensOfDisguise {
    did: edna::DID,
    decrypt_cap: edna::tokens::DecryptCap,
    diff_locators: Vec<edna::tokens::LocCap>,
    ownership_locators: Vec<edna::tokens::LocCap>,
    reveal: bool,
}

#[derive(Serialize)]
pub struct GetTokensOfDisguiseResponse {
    diff_tokens: Vec<Vec<u8>>,
    ownership_tokens: Vec<Vec<u8>>,
}

#[post("/", format = "json", data = "<data>")]
pub(crate) fn get_tokens_of_disguise(
    data: Json<GetTokensOfDisguise>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<GetTokensOfDisguiseResponse> {
    let e = edna.lock().unwrap();
    let tokens = e.get_tokens_of_disguise(
        data.did,
        data.decrypt_cap.clone(),
        data.diff_locators.clone(),
        data.ownership_locators.clone(),
        data.reveal,
    );
    return Json(GetTokensOfDisguiseResponse {
        diff_tokens: tokens.0,
        ownership_tokens: tokens.1,
    });
}

#[derive(Serialize)]
pub struct CreatePseudoprincipalResponse {
    uid: edna::UID,
    row: Vec<edna::RowVal>,
}

#[get("/")]
pub(crate) fn create_pseudoprincipal(
    edna: &State<Arc<Mutex<EdnaClient>>>,
) -> Json<CreatePseudoprincipalResponse> {
    let mut e = edna.lock().unwrap();
    let pp = e.create_new_pseudoprincipal();
    return Json(CreatePseudoprincipalResponse {
        uid: pp.0,
        row: pp.1,
    });
}

#[derive(Deserialize)]
pub struct SavePseudoprincipalToken {
    did: edna::DID,
    old_uid: edna::UID,
    new_uid: edna::UID,
    token_bytes: Vec<u8>,
}

#[post("/", format = "json", data = "<data>")]
pub(crate) fn save_pseudoprincipal_token(
    data: Json<SavePseudoprincipalToken>,
    edna: &State<Arc<Mutex<EdnaClient>>>,
) {
    let e = edna.lock().unwrap();
    e.save_pseudoprincipal_token(
        data.did.clone(),
        data.old_uid.clone(),
        data.new_uid.clone(),
        data.token_bytes.clone(),
    );
}

#[derive(Deserialize)]
pub struct SaveDiffToken {
    uid: edna::UID,
    did: edna::DID,
    data: Vec<u8>,
    is_global: bool,
}

#[post("/", format = "json", data = "<data>")]
pub(crate) fn save_diff_token(data: Json<SaveDiffToken>, edna: &State<Arc<Mutex<EdnaClient>>>) {
    let e = edna.lock().unwrap();
    e.save_diff_token(
        data.uid.clone(),
        data.did.clone(),
        data.data.clone(),
        data.is_global,
    );
}
