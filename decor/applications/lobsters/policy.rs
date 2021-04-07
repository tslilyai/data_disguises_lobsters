/*use decor::policy::{GeneratePolicy, GuiseColumnPolicy, ObjectGuisePolicies, EdgePolicy, MaskPolicy, ObjectName};
use std::collections::HashMap;
use std::rc::Rc;

fn get_pc_guise_policies() -> ObjectGuisePolicies {
    let mut guise_policies : ObjectGuisePolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    guise_policies.insert("users".to_string(), Rc::new(users_map));

    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("created_at".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Custom(Box::new(|time| time.to_string())))); 
    stories_map.insert("user_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("title".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("description".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("short_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("is_expired".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("upvotes".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("downvotes".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("is_moderated".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("hotness".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("markeddown_description".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("story_cache".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("comments_count".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("merged_story_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("unavailable_at".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("twitter_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("user_is_author".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    guise_policies.insert("stories".to_string(), Rc::new(stories_map));

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    taggings_map.insert("story_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::ForeignKey("stories".to_string())));
    taggings_map.insert("tag_id".to_string(), GuiseColumnPolicy::CloneAll);
    guise_policies.insert("taggings".to_string(), Rc::new(taggings_map));

    let mut comments_map = HashMap::new();
    comments_map.insert("short_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    guise_policies.insert("comments".to_string(), Rc::new(comments_map));

    guise_policies 
}

fn get_cp_guise_policies() -> ObjectGuisePolicies {
    let mut guise_policies : ObjectGuisePolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Default(0.to_string())));
    guise_policies.insert("users".to_string(), Rc::new(users_map));

    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Random));
    stories_map.insert("created_at".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Custom(Box::new(|time| time.to_string())))); 
    stories_map.insert("user_id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("title".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("description".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("short_id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Random));
    stories_map.insert("is_expired".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Random));
    stories_map.insert("upvotes".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("downvotes".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("is_moderated".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("hotness".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("markeddown_description".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("story_cache".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("comments_count".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("merged_story_id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("unavailable_at".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("twitter_id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("user_is_author".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Default(0.to_string())));
    guise_policies.insert("stories".to_string(), Rc::new(stories_map));

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::Random));
    taggings_map.insert("story_id".to_string(), GuiseColumnPolicy::CloneOne(GeneratePolicy::ForeignKey("stories".to_string())));
    taggings_map.insert("tag_id".to_string(), GuiseColumnPolicy::CloneAll);
    guise_policies.insert("taggings".to_string(), Rc::new(taggings_map));

    let mut comments_map = HashMap::new();
    comments_map.insert("short_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    guise_policies.insert("comments".to_string(), Rc::new(comments_map));
    
    guise_policies 
}

fn get_edge_policies() -> HashMap<ObjectName, Rc<Vec<EdgePolicy>>> {
    use decor::policy::EdgePolicyType::*;

    let mut edge_policies = HashMap::new();
    /* 
     * Any relationship to users should be decorrelated
    */
    edge_policies.insert("stories".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "users".to_string(),
                column: "user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
        ])
    );
    edge_policies.insert("comments".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "users".to_string(),
                column: "user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "stories".to_string(),
                column: "story_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "comments".to_string(),
                column: "parent_comment_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "comments.thread_id".to_string(),
                column: "thread_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
    edge_policies.insert("hats".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "users".to_string(),
                column: "user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            }
        ])
    );
    edge_policies.insert("moderations".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "users".to_string(),
                column: "user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "users".to_string(),
                column: "moderator_user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "stories".to_string(),
                column: "story_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "comments".to_string(),
                column: "comment_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ])
    );
    edge_policies.insert("invitations".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "users".to_string(),
                column: "user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            }
        ])
    );
    edge_policies.insert("messages".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "users".to_string(),
                column: "author_user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "users".to_string(),
                column: "recipient_user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            }
        ])
    );
    edge_policies.insert("votes".to_string(),
        Rc::new(vec![
             EdgePolicy{
                parent: "users".to_string(),
                column: "user_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "stories".to_string(),
                column: "story_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
        ]),
    );
    /* 
     * Taggings to non-user entities 
     * It's fine to keep multiple tags per story clustered, but we should make sure that the user's
     * stories don't make up more than 25% of all stories with this tag
     */
    edge_policies.insert("taggings".to_string(),
        Rc::new(vec![
            EdgePolicy{
                parent: "stories".to_string(),
                column: "story_id".to_string(),
                pc_policy: Retain,
                cp_policy: Retain,
            },
            EdgePolicy{
                parent: "tags".to_string(),
                column: "tag_id".to_string(),
                pc_policy: Decorrelate(0.0),
                cp_policy: Retain,
            },
        ])
    );
    edge_policies 
}

pub fn get_lobsters_policy() -> MaskPolicy {
    MaskPolicy{
        unsub_object_type: "users".to_string(), 
        pc_guise_policies : get_pc_guise_policies(), 
        cp_guise_policies : get_cp_guise_policies(), 
        edge_policies : get_edge_policies(),
    }
}*/
