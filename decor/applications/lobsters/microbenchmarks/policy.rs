use decor::policy::{GeneratePolicy, GhostColumnPolicy, EntityGhostPolicies, KeyRelationship, ApplicationPolicy};
use std::collections::HashMap;

fn get_ghost_policies() -> EntityGhostPolicies<'static> {
    let mut ghost_policies : EntityGhostPolicies = HashMap::new();

    let mut users_map = HashMap::new();
    users_map.insert("id", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma", GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users", users_map);

    let mut stories_map = HashMap::new();
    stories_map.insert("id", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("created_at", GhostColumnPolicy::Generate(GeneratePolicy::Custom(Box::new(|time| time)))); //TODO randomize
    stories_map.insert("user_id", GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey));
    stories_map.insert("url", GhostColumnPolicy::CloneAll);
    stories_map.insert("title", GhostColumnPolicy::CloneAll);
    stories_map.insert("description", GhostColumnPolicy::CloneAll);
    stories_map.insert("short_id", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("is_expired", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("upvotes", GhostColumnPolicy::CloneAll);
    stories_map.insert("downvotes", GhostColumnPolicy::CloneAll);
    stories_map.insert("is_moderated", GhostColumnPolicy::CloneAll);
    stories_map.insert("hotness", GhostColumnPolicy::CloneAll);
    stories_map.insert("markeddown_description", GhostColumnPolicy::CloneAll);
    stories_map.insert("story_cache", GhostColumnPolicy::CloneAll);
    stories_map.insert("comments_count", GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("merged_story_id", GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("unavailable_at", GhostColumnPolicy::CloneAll);
    stories_map.insert("twitter_id", GhostColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("user_is_author", GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("stories", stories_map);

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id", GhostColumnPolicy::Generate(GeneratePolicy::Random));
    taggings_map.insert("story_id", GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey));
    taggings_map.insert("tag_id", GhostColumnPolicy::CloneAll);
    ghost_policies.insert("taggings", taggings_map);
    
    ghost_policies 
}

pub fn get_lobsters_policy() -> ApplicationPolicy<'static> {
    use decor::policy::DecorrelationPolicy::*;
    ApplicationPolicy{
        entity_type_to_decorrelate: "users", 
        ghost_policies : get_ghost_policies(), 
        edge_policies : vec![
            /* 
             * Any relationship to users should be decorrelated
             */
            KeyRelationship{
                child: "stories",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "comments",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "hats",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "moderations",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "moderations",
                parent: "users",
                column_name: "moderator_user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "invitations",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "messages",
                parent: "users",
                column_name: "author_user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "messages",
                parent: "users",
                column_name: "recipient_user_id",
                decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "votes",
                parent: "users",
                column_name: "user_id",
                decorrelation_policy: Decor,
            },
            /* 
             * Relationships from moderations to non-user entities 
             */
            KeyRelationship{
                child: "moderations",
                parent: "stories",
                column_name: "story_id",
                decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "moderations",
                parent: "comments",
                column_name: "comment_id",
                decorrelation_policy: NoDecorRetain,
            },
            /* 
             * Relationships from comments to non-user entities
             */
            KeyRelationship{
                child: "comments",
                parent: "stories",
                column_name: "story_id",
                decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "comments",
                parent: "comments",
                column_name: "parent_comment_id",
                decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "comments",
                parent: "comments.thread_id",
                column_name: "thread_id",
                decorrelation_policy: NoDecorRetain,
            },
            /* 
             * Taggings to non-user entities 
             * It's fine to keep multiple tags per story clustered, but we should make sure that the user's
             * stories don't make up more than 25% of all stories with this tag
             */
            KeyRelationship{
                child: "taggings",
                parent: "stories",
                column_name: "story_id",
                decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "taggings",
                parent: "tags",
                column_name: "tag_id",
                decorrelation_policy: NoDecorSensitivity(0.25),
            },
            /* 
             * Votes to stories
             */
            KeyRelationship{
                child: "votes",
                parent: "stories",
                column_name: "story_id",
                decorrelation_policy: NoDecorRetain,
            },
        ]
    }
}
