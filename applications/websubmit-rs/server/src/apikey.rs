use crate::backend::MySqlBackend;
use crate::config::Config;
use crate::email;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use mysql::from_value;
use rocket::form::Form;
use rocket::http::Status;
use rocket::http::{Cookie, CookieJar};
use rocket::outcome::IntoOutcome;
use rocket::request::{self, FromRequest, Request};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use rsa::pkcs1::ToRsaPrivateKey;
use std::collections::HashMap;
use std::time;

/// (username, apikey)
pub(crate) struct ApiKey {
    pub user: String,
    key: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct ApiKeyRequest {
    email: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct ApiKeySubmit {
    key: String,
}

#[derive(Debug)]
pub(crate) enum ApiKeyError {
    Ambiguous,
    Missing,
    BackendFailure,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for ApiKey {
    type Error = ApiKeyError;

    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let be = request
            .guard::<&State<MySqlBackend>>()
            .await
            .unwrap();
        request
            .cookies()
            .get("apikey")
            .and_then(|cookie| cookie.value().parse().ok())
            .and_then(|key: String| match check_api_key(&be, &key) {
                Ok(user) => {
                    //println!("API key in cookie for user {}", user);
                    Some(ApiKey { user, key })
                }
                Err(_) => None,
            })
            .into_outcome((Status::Unauthorized, ApiKeyError::Missing))
    }
}

#[post("/", data = "<data>")]
pub(crate) fn generate(
    data: Form<ApiKeyRequest>,
    bg: &State<MySqlBackend>,
    config: &State<Config>,
) -> Template {

    // generate an API key from email address
    let start = time::Instant::now();
    let mut hasher = Sha256::new();
    hasher.input_str(&data.email);
    // add a secret to make API keys unforgeable without access to the server
    hasher.input_str(&config.secret);
    let hash = hasher.result_str();
    info!(bg.log, "apikey hash: {}", start.elapsed().as_micros());

    let is_admin = if config.admins.contains(&data.email) {
        1.into()
    } else {
        0.into()
    };

    // insert into MySql if not exists
    let start = time::Instant::now();
    bg.insert(
        "users",
        vec![
            data.email.as_str().into(),
            hash.as_str().into(),
            is_admin,
            false.into(),
        ],
    );
    info!(bg.log, "user insert: {}", start.elapsed().as_micros());
    
    let mut privkey_str = String::new();
    if !config.is_baseline {
        // register user if not exists
        let start = time::Instant::now();
        let private_key = bg.edna.lock().unwrap().register_principal(&data.email);
        privkey_str = base64::encode(&private_key.to_pkcs1_der().unwrap().as_der().to_vec());
        info!(bg.log, "register principal: {}", start.elapsed().as_micros());
    }

    let start = time::Instant::now();
    if config.send_emails {
        email::send(
            bg.log.clone(),
            "no-reply@csci2390-submit.cs.brown.edu".into(),
            vec![data.email.clone()],
            format!("{} API key", config.class),
            format!("APIKEY:{}\nDECRYPTCAP:{}", hash.as_str(), privkey_str,),
        )
        .expect("failed to send API key email");
    }
    info!(bg.log, "send apikey email: {}", start.elapsed().as_micros());

    // return to user
    let mut ctx = HashMap::new();
    ctx.insert("apikey_email", data.email.clone());
    ctx.insert("parent", "layout".into());
    Template::render("apikey/generate", &ctx)
}

pub(crate) fn check_api_key(
    bg: &State<MySqlBackend>,
    key: &str,
) -> Result<String, ApiKeyError> {
    let rs = bg.query_exec("users_by_apikey", vec![key.into()]);
    if rs.len() < 1 {
        Err(ApiKeyError::Missing)
    } else if rs.len() > 1 {
        Err(ApiKeyError::Ambiguous)
    } else if rs.len() >= 1 {
        // user email
        Ok(from_value::<String>(rs[0][0].clone()))
    } else {
        Err(ApiKeyError::BackendFailure)
    }
}

#[post("/", data = "<data>")]
pub(crate) fn check(
    data: Form<ApiKeySubmit>,
    cookies: &CookieJar<'_>,
    bg: &State<MySqlBackend>,
) -> Redirect {
    // check that the API key exists and set cookie
    let res = check_api_key(&*bg, &data.key);
    match res {
        Err(ApiKeyError::BackendFailure) => {
            eprintln!("Problem communicating with MySql backend");
        }
        Err(ApiKeyError::Missing) => {
            eprintln!("No such API key: {}", data.key);
        }
        Err(ApiKeyError::Ambiguous) => {
            eprintln!("Ambiguous API key: {}", data.key);
        }
        Ok(_) => (),
    }

    if res.is_err() {
        Redirect::to("/")
    } else {
        let cookie = Cookie::build("apikey", data.key.clone()).path("/").finish();
        cookies.add(cookie);
        Redirect::to("/leclist")
    }
}
