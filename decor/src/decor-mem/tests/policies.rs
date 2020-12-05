use std::collections::{HashMap,HashSet};

use decor_mem::policy::{
    KeyRelationship, GhostColumnPolicy, GeneratePolicy, EntityGhostPolicies,
    DecorrelationPolicy::{Decor, NoDecorRemove, NoDecorSensitivity, NoDecorRetain}, ApplicationPolicy};

fn ghost_gen_policies() -> EntityGhostPolicies {
    let mut ghost_policies = HashMap::new();
    
    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users".to_string(), users_map);

    let mut mods_map = HashMap::new();
    mods_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    mods_map.insert("moderator_user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    mods_map.insert("story_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("stories".to_string())));
    mods_map.insert("user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    mods_map.insert("action".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("text".to_string())));
    ghost_policies.insert("moderations".to_string(), mods_map);
   
    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("google.com".to_string())));
    stories_map.insert("is_moderated".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("0".to_string())));
    ghost_policies.insert("stories".to_string(), stories_map);
    ghost_policies
}

pub fn noop_policy() -> ApplicationPolicy {
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: vec![
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "moderator_user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            },
            KeyRelationship{
                child: "stories".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
            }
        ]
    }
}
pub fn decor_all_policy() -> ApplicationPolicy {
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: vec![
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "moderator_user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
            },
             KeyRelationship{
                child: "moderations".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "stories".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
            }
        ]
    }
}
pub fn sensitive_policy() -> ApplicationPolicy {
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: vec![
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorSensitivity(0.4),
                child_parent_decorrelation_policy: NoDecorSensitivity(0.4),
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "moderator_user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorSensitivity(0.4),
                child_parent_decorrelation_policy: NoDecorSensitivity(0.4),
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorSensitivity(0.4),
                child_parent_decorrelation_policy: NoDecorSensitivity(0.4),
            },
            KeyRelationship{
                child: "stories".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorSensitivity(0.4),
                child_parent_decorrelation_policy: NoDecorSensitivity(0.4),
            }
        ]
    }
}
pub fn remove_policy() -> ApplicationPolicy {
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: vec![
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRemove,
                child_parent_decorrelation_policy: NoDecorRemove,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "moderator_user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRemove,
                child_parent_decorrelation_policy: NoDecorRemove,
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRemove,
                child_parent_decorrelation_policy: NoDecorRemove,
            },
            KeyRelationship{
                child: "stories".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRemove,
                child_parent_decorrelation_policy: NoDecorRemove,
            }
        ]
    }
}

pub fn combined_policy() -> ApplicationPolicy {
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: vec![
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorSensitivity(0.4),
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "users".to_string(),
                column_name: "moderator_user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorSensitivity(0.4),
            },
            KeyRelationship{
                child: "moderations".to_string(),
                parent: "stories".to_string(),
                column_name: "story_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: Decor,
            },
            KeyRelationship{
                child: "stories".to_string(),
                parent: "users".to_string(),
                column_name: "user_id".to_string(),
                parent_child_decorrelation_policy: Decor,
                child_parent_decorrelation_policy: NoDecorRetain,
            }
        ]
    }
}
