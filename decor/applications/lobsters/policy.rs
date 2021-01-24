use decor::policy::{GeneratePolicy, GhostColumnPolicy, EntityGhostPolicies, EdgePolicy, MaskPolicy, EntityName};
use std::collections::HashMap;
use std::rc::Rc;

fn get_pc_ghost_policies() -> EntityGhostPolicies {
    let mut ghost_policies : EntityGhostPolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users".to_string(), Rc::new(users_map));

    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("created_at".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Custom(Box::new(|time| time.to_string())))); 
    stories_map.insert("user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("title".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("description".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("short_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("is_expired".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("upvotes".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("downvotes".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("is_moderated".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("hotness".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("markeddown_description".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("story_cache".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("comments_count".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("merged_story_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("unavailable_at".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("twitter_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("user_is_author".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("stories".to_string(), Rc::new(stories_map));

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    taggings_map.insert("story_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("stories".to_string())));
    taggings_map.insert("tag_id".to_string(), GhostColumnPolicy::CloneAll);
    ghost_policies.insert("taggings".to_string(), Rc::new(taggings_map));
    
    ghost_policies 
}

fn get_cp_ghost_policies() -> EntityGhostPolicies {
    let mut ghost_policies : EntityGhostPolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users".to_string(), Rc::new(users_map));

    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Random));
    stories_map.insert("created_at".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Custom(Box::new(|time| time.to_string())))); 
    stories_map.insert("user_id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("title".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("description".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("short_id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Random));
    stories_map.insert("is_expired".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Random));
    stories_map.insert("upvotes".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("downvotes".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("is_moderated".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("hotness".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("markeddown_description".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("story_cache".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("comments_count".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("merged_story_id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("unavailable_at".to_string(), GhostColumnPolicy::CloneAll);
    stories_map.insert("twitter_id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("user_is_author".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("stories".to_string(), Rc::new(stories_map));

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::Random));
    taggings_map.insert("story_id".to_string(), GhostColumnPolicy::CloneOne(GeneratePolicy::ForeignKey("stories".to_string())));
    taggings_map.insert("tag_id".to_string(), GhostColumnPolicy::CloneAll);
    ghost_policies.insert("taggings".to_string(), Rc::new(taggings_map));
    
    ghost_policies 
}

fn get_edge_policies() -> HashMap<EntityName, Rc<Vec<EdgePolicy>>> {
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
        unsub_entity_type: "users".to_string(), 
        pc_ghost_policies : get_pc_ghost_policies(), 
        cp_ghost_policies : get_cp_ghost_policies(), 
        edge_policies : get_edge_policies(),
    }
}
