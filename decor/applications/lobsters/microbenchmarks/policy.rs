use decor::policy::{GeneratePolicy, GhostColumnPolicy, GhostPolicy, AttributeGhostPolicies, ClusterPolicy, Cluster};
use sql_parser::ast::*;

let mut ghost_policies : AttributeGhostPolicies = Hashmap::new();

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
         * Clusters around users should all be broken!
         */
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "stories",
            attribute: "users",
            column_name: "user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "comments",
            attribute: "users",
            column_name: "user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "hats",
            attribute: "users",
            column_name: "user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "moderations",
            attribute: "users",
            column_name: "user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "moderations",
            attribute: "users",
            column_name: "moderator_user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "invitations",
            attribute: "users",
            column_name: "user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "messages",
            attribute: "users",
            column_name: "author_user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "messages",
            attribute: "users",
            column_name: "recipient_user_id",
        }),
        ClusterPolicy::Decor(Cluster{
            cluster_entity: "votes",
            attribute: "users",
            column_name: "user_id",
        }),
        /* 
         * Clusters of moderations around non-user entities: Moderations don't usually leak information
         * about the story or comment's owner, so don't break up these clusters or add fake moderations 
         */
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "moderations",
            attribute: "stories",
            column_name: "story_id",
        }),
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "moderations",
            attribute: "comments",
            column_name: "comment_id",
        }),
        /* 
         * Clusters of comments around non-user entities: Comments usually don't leak information about
         * the user, so don't break up clusters or add fake comments
         */
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "comments",
            attribute: "stories",
            column_name: "story_id",
        }),
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "comments",
            attribute: "comments",
            column_name: "parent_comment_id",
        }),
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "comments",
            attribute: "comments.thread_id",
            column_name: "thread_id",
        }),
        /* 
         * Clusters of taggings around non-user entities 
         * It's fine to keep multiple tags per story clustered, but we should make sure that the user's
         * stories don't make up more than 25% of all stories with this tag
         */
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "taggings",
            attribute: "stories",
            column_name: "story_id",
        }),
        ClusterPolicy::NoDecorThreshold{
            c: Cluster{
                cluster_entity: "taggings",
                attribute: "tags",
                column_name: "tag_id",
            },
            cluster_threshold: 0.25,
        },
        /* 
         * Clusters of votes around things can stay
         */
        ClusterPolicy::NoDecorRetain(Cluster{
            cluster_entity: "votes",
            attribute: "stories",
            column_name: "story_id",
        }),
    ].iter().clone().collect()
);
