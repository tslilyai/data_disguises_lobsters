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

pub(crate) struct Admin;

#[derive(Debug)]
pub(crate) enum AdminError {
    Unauthorized,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Admin {
    type Error = AdminError;

    async fn from_request(request: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let apikey = request.guard::<ApiKey>().await.unwrap();
        let cfg = request.guard::<&State<Config>>().await.unwrap();

        let res = if cfg.admins.contains(&apikey.user) {
            Some(Admin)
        } else {
            None
        };

        res.into_outcome((Status::Unauthorized, AdminError::Unauthorized))
    }
}

#[derive(Debug, FromForm)]
pub(crate) struct AddLectureQuestionForm {
    q_id: u64,
    q_prompt: String,
}

#[derive(Debug, FromForm)]
pub(crate) struct AdminLecAdd {
    lec_id: u8,
    lec_label: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct User {
    email: String,
    apikey: String,
    is_admin: u8,
}

#[derive(Serialize)]
struct UserContext {
    users: Vec<User>,
    parent: &'static str,
}

#[get("/")]
pub(crate) fn lec_add(_adm: Admin) -> Template {
    let mut ctx = HashMap::new();
    ctx.insert("parent", String::from("layout"));
    Template::render("admin/lecadd", &ctx)
}

#[post("/", data = "<data>")]
pub(crate) fn lec_add_submit(
    _adm: Admin,
    data: Form<AdminLecAdd>,
    bg: &State<MySqlBackend>,
) -> Redirect {
    // insert into MySql if not exists
    bg.insert(
        "lectures",
        vec![
            (data.lec_id as u64).into(),
            data.lec_label.to_string().into(),
        ],
    );

    Redirect::to("/leclist")
}

#[get("/<num>")]
pub(crate) fn lec(_adm: Admin, num: u8, 
    bg: &State<MySqlBackend>)
-> Template {
    let res = bg.query_exec("qs_by_lec", vec![(num as u64).into()]);
    let mut qs: Vec<_> = res
        .into_iter()
        .map(|r| {
            let id: u64 = from_value(r[1].clone());
            LectureQuestion {
                id: id,
                prompt: from_value(r[2].clone()),
                answer: None,
            }
        })
        .collect();
    qs.sort_by(|a, b| a.id.cmp(&b.id));

    let ctx = LectureQuestionsContext {
        lec_id: num,
        questions: qs,
        parent: "layout",
    };
    Template::render("admin/lec", &ctx)
}

#[post("/<num>", data = "<data>")]
pub(crate) fn addq(
    _adm: Admin,
    num: u8,
    data: Form<AddLectureQuestionForm>,
    bg: &State<MySqlBackend>,
) -> Redirect {
    bg.insert(
        "questions",
        vec![
            (num as u64).into(),
            (data.q_id as u64).into(),
            data.q_prompt.to_string().into(),
        ],
    );

    Redirect::to(format!("/admin/lec/{}", num))
}

#[get("/<num>/<qnum>")]
pub(crate) fn editq(
    _adm: Admin,
    num: u8,
    qnum: u8,
    bg: &State<MySqlBackend>,
) -> Template {
    let res = bg.query_exec("qs_by_lec", vec![(num as u64).into()]);

    let mut ctx = HashMap::new();
    for r in res {
        if r[1] == (qnum as u64).into() {
            ctx.insert("lec_qprompt", from_value(r[2].clone()));
        }
    }
    ctx.insert("lec_id", format!("{}", num));
    ctx.insert("lec_qnum", format!("{}", qnum));
    ctx.insert("parent", String::from("layout"));
    Template::render("admin/lecedit", &ctx)
}

#[post("/editq/<num>", data = "<data>")]
pub(crate) fn editq_submit(
    _adm: Admin,
    num: u8,
    data: Form<AddLectureQuestionForm>,
    bg: &State<MySqlBackend>,
) -> Redirect {
    bg.update(
        "questions",
        vec![(num as u64).into(), (data.q_id as u64).into()],
        vec![(2, data.q_prompt.to_string().into())],
    );

    Redirect::to(format!("/admin/lec/{}", num))
}

#[get("/")]
pub(crate) fn get_registered_users(
    _adm: Admin,
    bg: &State<MySqlBackend>,
    config: &State<Config>,
) -> Template {
    let res = bg.query_exec("all_users", vec![]);

    let users: Vec<_> = res
        .into_iter()
        .map(|r| User {
            email: from_value(r[0].clone()),
            apikey: from_value(r[2].clone()),
            is_admin: if config.admins.contains(&from_value(r[0].clone())) {
                1
            } else {
                0
            }, // r[1].clone().into(), this type conversion does not work
        })
        .collect();

    let ctx = UserContext {
        users: users,
        parent: "layout",
    };
    Template::render("admin/users", &ctx)
}
