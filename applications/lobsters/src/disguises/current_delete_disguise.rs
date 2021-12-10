use crate::*;
use chrono;
use edna::predicate::*;
use edna::spec::*;
use sql_parser::ast::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/*  def delete!
    User.transaction do
      self.comments
        .where("score < 0")
        .find_each {|c| c.delete_for_user(self) }

      self.sent_messages.each do |m|
        m.deleted_by_author = true
        m.save
      end
      self.received_messages.each do |m|
        m.deleted_by_recipient = true
        m.save
      end

      self.invitations.destroy_all

      self.session_token = nil
      self.check_session_token

      self.deleted_at = Time.current
      self.good_riddance?
      self.save!
    end
  end

  def undelete!
    User.transaction do
      self.sent_messages.each do |m|
        m.deleted_by_author = false
        m.save
      end
      self.received_messages.each do |m|
        m.deleted_by_recipient = false
        m.save
      end

      self.deleted_at = nil
      self.save!
    end
  end

  # ensures some users talk to a mod before reactivating
  def good_riddance?
    return if self.is_banned? # https://www.youtube.com/watch?v=UcZzlPGnKdU
    self.email = "#{self.username}@lobsters.example" if \
      self.karma < 0 ||
      (self.comments.where('created_at >= now() - interval 30 day AND is_deleted').count +
       self.stories.where('created_at >= now() - interval 30 day AND is_expired AND is_moderated')
         .count >= 3) ||
      FlaggedCommenters.new('90d').check_list_for(self)
  end

  def self.disown_all_by_author! author
    author.stories.update_all(:user_id => inactive_user.id)
    author.comments.update_all(:user_id => inactive_user.id)
    refresh_counts! author
  end
*/

fn get_eq_pred(col: &str, val: String) -> Vec<Vec<PredClause>> {
    vec![vec![PredClause::ColValCmp {
        col: col.to_string(),
        val: val,
        op: BinaryOperator::Eq,
    }]]
}

pub fn get_disguise_id() -> u64 {
    2
}
pub fn get_disguise(user_id: u64) -> Disguise {
    Disguise {
        did: get_disguise_id(),
        user: user_id.to_string(),
        table_disguises: get_table_disguises(user_id),
        table_info: disguises::get_table_info(),
        use_txn: false,
    }
}
fn gen_timestamp(_: &str) -> String {
    let ts = chrono::Local::now().naive_local();
    ts.format("%c").to_string()
}
fn gen_true_str(_: &str) -> String {
    true.to_string()
}
fn gen_nil(_: &str) -> String {
    "NULL".to_string()
}
fn gen_anon(_: &str) -> String {
    // assume inactive user has id 1
    "1".to_string()
}
fn gen_true(_: &str) -> bool {
    true
}
pub fn get_table_disguises(
    user_id: u64,
) -> HashMap<String, Arc<RwLock<Vec<ObjectTransformation>>>> {
    let mut hm = HashMap::new();

    // REMOVE
    hm.insert(
        "invitations".to_string(),
        Arc::new(RwLock::new(vec![ObjectTransformation {
            pred: get_eq_pred("user_id", user_id.to_string()),
            trans: Arc::new(RwLock::new(TransformArgs::Remove)),
            global: false,
        }])),
    );

    // MODIFY
    hm.insert(
        "users".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: get_eq_pred("id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "deleted_at".to_string(),
                    generate_modified_value: Box::new(gen_timestamp),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "session_token".to_string(),
                    generate_modified_value: Box::new(gen_nil),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
        ])),
    );

    hm.insert(
        "comments".to_string(),
        Arc::new(RwLock::new(vec![
            ObjectTransformation {
                pred: vec![vec![
                    PredClause::ColValCmp {
                        col: "user_id".to_string(),
                        val: user_id.to_string(),
                        op: BinaryOperator::Eq,
                    },
                    PredClause::ColValCmp {
                        col: "score".to_string(),
                        val: 0.to_string(),
                        op: BinaryOperator::Lt,
                    },
                ]],
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "is_deleted".to_string(),
                    generate_modified_value: Box::new(gen_true_str),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
            // disown all comments by point to inactive-user
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "user_id".to_string(),
                    generate_modified_value: Box::new(gen_anon),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
        ])),
    );
    hm.insert(
        "stories".to_string(),
        Arc::new(RwLock::new(vec![
            // disown all stories by pointing to inactive-user
            ObjectTransformation {
                pred: get_eq_pred("user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "user_id".to_string(),
                    generate_modified_value: Box::new(gen_anon),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
        ])),
    );

    hm.insert(
        "messages".to_string(),
        Arc::new(RwLock::new(vec![
            // remove if both parties deleted
            ObjectTransformation {
                pred: vec![
                    vec![
                        PredClause::ColValCmp {
                            col: "author_user_id".to_string(),
                            val: user_id.to_string(),
                            op: BinaryOperator::Eq,
                        },
                        PredClause::ColValCmp {
                            col: "deleted_by_recipient".to_string(),
                            val: 1.to_string(),
                            op: BinaryOperator::Eq,
                        },
                    ],
                    vec![
                        PredClause::ColValCmp {
                            col: "recipient_user_id".to_string(),
                            val: user_id.to_string(),
                            op: BinaryOperator::Eq,
                        },
                        PredClause::ColValCmp {
                            col: "deleted_by_author".to_string(),
                            val: 1.to_string(),
                            op: BinaryOperator::Eq,
                        },
                    ],
                ],
                trans: Arc::new(RwLock::new(TransformArgs::Remove)),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("author_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "deleted_by_author".to_string(),
                    generate_modified_value: Box::new(gen_true_str),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
            ObjectTransformation {
                pred: get_eq_pred("recipient_user_id", user_id.to_string()),
                trans: Arc::new(RwLock::new(TransformArgs::Modify {
                    col: "deleted_by_recipient".to_string(),
                    generate_modified_value: Box::new(gen_true_str),
                    satisfies_modification: Box::new(gen_true),
                })),
                global: false,
            },
        ])),
    );
    hm
}
