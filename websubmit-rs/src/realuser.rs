use crate::apikey::ApiKey;
use crate::backend::MySqlBackend;
use crate::config::Config;
use crate::questions::{LectureQuestion, LectureQuestionsContext};
use mysql::from_value;
use rocket::form::Form;
use rocket::http::Status;
use rocket::outcome::IntoOutcome;
use rocket::request::{self, FromRequest, Request};
use rocket::response::Redirect;
use rocket::State;
use rocket_dyn_templates::Template;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct RealUser;

#[derive(Debug)]
pub(crate) enum RealUserError {
    Unauthorized,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for RealUser {
    type Error = RealUserError;

    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let apikey = request.guard::<ApiKey>().await.unwrap();
        let backend = request.guard::<MySqlBackend>().await.unwrap();
        bg = backend.lock().unwrap();
        let res = bg.query_exec("users_by_apikey", vec![apikey.user.into()]);
        drop(bg);

        // check if user is a real user
        let out = if res.len() < 1 {
            None
        } else {
            match res[0][3] {
                0 => Some(RealUser),
                1 => None,
            }
        };
        // only real users can perform this action
        out.into_outcome((Status::Unauthorized, RealUserError::Unauthorized))
    }
}
