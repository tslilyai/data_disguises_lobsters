use decor::policy::{GeneratePolicy, GhostColumnPolicy, GhostPolicy, EntityGhostPolicies, KeyRelationship};
use sql_parser::ast::*;

let mut ghost_policies : parentGhostPolicies = Hashmap::new();

ghost_policies.insert("users", [
    ("id", GhostColumnPolicy::Generate(GeneratePolicy::Random)),
    ("username", GhostColumnPolicy::Generate(GeneratePolicy::Random)),
    ("karma", GhostColumnPolicy::Generate(GeneratePolicy::Default(0))),
].iter().clone().collect());

ghost_policies.insert("stories", [
    ("id", GhostColumnPolicy::Generate(GeneratePolicy::Random)),
    ("created_at", GhostColumnPolicy::Generate(GeneratePolicy::Custom(|time| time + random()))),
    ("user_id", GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey)),
    ("url", GhostColumnPolicy::CloneAll),
    ("title", GhostColumnPolicy::CloneAll),
    ("description", GhostColumnPolicy::CloneAll),
    ("short_id", GhostColumnPolicy::Generate(GeneratePolicy::Custom(|_| hash(self.id)))),
    ("is_expired", GhostColumnPolicy::Generate(GeneratePolicy::Random),
    ("upvotes", GhostColumnPolicy::CloneAll),
    ("downvotes", GhostColumnPolicy::CloneAll),
    ("is_moderated", GhostColumnPolicy::CloneAll),
    ("hotness", GhostColumnPolicy::CloneAll),
    ("markeddown_description", GhostColumnPolicy::CloneAll),
    ("story_cache", GhostColumnPolicy::CloneAll),
    ("comments_count", GhostColumnPolicy::Generate(GeneratePolicy::Default(0))),
    ("merged_story_id", GhostColumnPolicy::Generate(GeneratePolicy::Default(Value::Null))),
    ("unavailable_at", GhostColumnPolicy::CloneAll),
    ("twitter_id", GhostColumnPolicy::Generate(GeneratePolicy::Default(Value::Null))),
    ("user_is_author", GhostColumnPolicy::Generate(GeneratePolicy::Default(0))),
]).iter().clone().collect();

ghost_policies.insert("taggings", [
    ("id", GhostColumnPolicy::Generate(GeneratePolicy::Random)),
    ("story_id", GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey)),
    ("tag_id", GhostColumnPolicy::CloneAll),
].iter().clone().collect());

let lobsters_policy : policy::ApplicationPolicy = (
    ghost_policies, 
    vec![
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
            decorrelation_Policy: NoDecorSensitivity(0.25),
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
    ].iter().clone().collect()
);
