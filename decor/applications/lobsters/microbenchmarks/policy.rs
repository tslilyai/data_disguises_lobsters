use decor::policy::{GeneratePolicy, GhostColumnPolicy, EntityGhostPolicies, KeyRelationship, ApplicationPolicy};
use std::collections::HashMap;

fn get_ghost_policies() -> EntityGhostPolicies {
    let mut ghost_policies : EntityGhostPolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users".to_string(), users_map);

    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    //stories_map.insert("created_at".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Custom(Box::new(|time| time)))); 
    ////TODO custom functions not supported because of clone / hash reasons... 
    stories_map.insert("created_at".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string()))); //TODO randomize
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
    ghost_policies.insert("stories".to_string(), stories_map);

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    taggings_map.insert("story_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("stories".to_string())));
    taggings_map.insert("tag_id".to_string(), GhostColumnPolicy::CloneAll);
    ghost_policies.insert("taggings".to_string(), taggings_map);
    
    ghost_policies 
}

pub fn get_lobsters_policy() -> ApplicationPolicy {
    use decor::policy::DecorrelationPolicy::*;
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(), 
        ghost_policies : get_ghost_policies(), 
        edge_policies : vec![
            /* 
             * Any relationship to users should be decorrelated
             */
            KeyRelationship{
                child: "stories".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "comments".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "hats".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "moderator_user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "invitations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "messages".to_string(),
                parent: "users".to_string(),
                column_name: "author_user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "messages".to_string(),
                parent: "users".to_string(),
                column_name: "recipient_user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "votes".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            /* 
             * Relationships from moderations to non-user entities 
             */
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "comments".to_string(),
                column_name: "comment_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            /* 
             * Relationships from comments to non-user entities
             */
            KeyRelationship{
                child: "comments".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "comments".to_string(),
                parent: "comments".to_string(),
                column_name: "parent_comment_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "comments".to_string(),
                parent: "comments.thread_id".to_string(),
                column_name: "thread_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            /* 
             * Taggings to non-user entities 
             * It's fine to keep multiple tags per story clustered, but we should make sure that the user's
             * stories don't make up more than 25% of all stories with this tag
             */
            KeyRelationship{
                child: "taggings".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "taggings".to_string(),
                parent: "tags".to_string(),
                column_name: "tag_id".to_string(),
                parent_child_decorrelation_policy: NoDecorSensitivity(0.25),
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            /* 
             * Votes to stories
             */
            KeyRelationship{
                child: "votes".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
        ]
    }
}
