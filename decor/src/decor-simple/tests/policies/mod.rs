use std::collections::{HashMap};
use std::rc::Rc;

use decor_simple::policy::{
    EdgePolicy, GhostColumnPolicy, GeneratePolicy, EntityGhostPolicies, ApplicationPolicy,
    EdgePolicyType::*
};

pub enum PolicyType {
    Noop,
    Combined,
}

fn ghost_gen_policies() -> EntityGhostPolicies {
    let mut ghost_policies = HashMap::new();
    
    let mut users_map = HashMap::new();
    users_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("username".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    users_map.insert("karma".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    ghost_policies.insert("users".to_string(), Rc::new(users_map));

    let mut mods_map = HashMap::new();
    mods_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    mods_map.insert("moderator_user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    mods_map.insert("story_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("stories".to_string())));
    mods_map.insert("user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    mods_map.insert("action".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("text".to_string())));
    ghost_policies.insert("moderations".to_string(), Rc::new(mods_map));
   
    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("user_id".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("google.com".to_string())));
    stories_map.insert("is_moderated".to_string(), GhostColumnPolicy::Generate(GeneratePolicy::Default("0".to_string())));
    ghost_policies.insert("stories".to_string(), Rc::new(stories_map));
    ghost_policies
}

pub fn noop_policy() -> ApplicationPolicy {
    let mut edge_policies = HashMap::new();
    edge_policies.insert("moderations".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Retain,
            cp_policy: Retain,
        },
        EdgePolicy{
            parent: "users".to_string(),
            column: "moderator_user_id".to_string(),
            pc_policy: Retain,
            cp_policy: Retain,
        },
        EdgePolicy{
            parent: "stories".to_string(),
            column: "story_id".to_string(),
            pc_policy: Retain,
            cp_policy: Retain,
        }
    ]));
    edge_policies.insert("stories".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Retain,
            cp_policy: Retain,
        }
    ]));
    ApplicationPolicy{
        unsub_entity_type: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: edge_policies,
    }
}
/*pub fn decor_all_policy() -> ApplicationPolicy {
    let mut edge_policies = HashMap::new();
    edge_policies.insert("moderations".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Decorrelate(0.0),
        },
        EdgePolicy{
            parent: "users".to_string(),
            column: "moderator_user_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Decorrelate(0.0),
        },
        EdgePolicy{
            parent: "stories".to_string(),
            column: "story_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Decorrelate(0.0),
        }
    ]));
    edge_policies.insert("stories".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Decorrelate(0.0),
        }
    ]));
    ApplicationPolicy{
        unsub_entity_type: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: edge_policies,
    }
}*/
/*pub fn sensitive_policy() -> ApplicationPolicy {
    let mut edge_policies = HashMap::new();
    edge_policies.insert("moderations".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Decorrelate(0.4),
            cp_policy: Decorrelate(0.4),
        },
        EdgePolicy{
            parent: "users".to_string(),
            column: "moderator_user_id".to_string(),
            pc_policy: Decorrelate(0.4),
            cp_policy: Decorrelate(0.4),
        },
        EdgePolicy{
            parent: "stories".to_string(),
            column: "story_id".to_string(),
            pc_policy: Decorrelate(0.4),
            cp_policy: Decorrelate(0.4),
        }
    ]));
    edge_policies.insert("stories".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Decorrelate(0.4),
            cp_policy: Decorrelate(0.4),
        }
    ]));
    ApplicationPolicy{
        unsub_entity_type: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: edge_policies,
    }
}*/
/*pub fn remove_policy() -> ApplicationPolicy {
    let mut edge_policies = HashMap::new();
    edge_policies.insert("moderations".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Delete(0.0),
            cp_policy: Delete(0.0),
        },
        EdgePolicy{
            parent: "users".to_string(),
            column: "moderator_user_id".to_string(),
            pc_policy: Delete(0.0),
            cp_policy: Delete(0.0),
        },
        EdgePolicy{
            parent: "stories".to_string(),
            column: "story_id".to_string(),
            pc_policy: Delete(0.0),
            cp_policy: Delete(0.0),
        }
    ]));
    edge_policies.insert("stories".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Delete(0.0),
            cp_policy: Delete(0.0),
        }
    ]));
    ApplicationPolicy{
        unsub_entity_type: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: edge_policies,
    }
}*/
pub fn combined_policy() -> ApplicationPolicy {
    let mut edge_policies = HashMap::new();
    edge_policies.insert("moderations".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Decorrelate(0.4),
        },
        EdgePolicy{
            parent: "users".to_string(),
            column: "moderator_user_id".to_string(),
            pc_policy: Delete(0.0),
            cp_policy: Decorrelate(0.4),
        },
        EdgePolicy{
            parent: "stories".to_string(),
            column: "story_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Decorrelate(0.0),
        }
    ]));
    edge_policies.insert("stories".to_string(), Rc::new(vec![
        EdgePolicy{
            parent: "users".to_string(),
            column: "user_id".to_string(),
            pc_policy: Decorrelate(0.0),
            cp_policy: Retain,
        }
    ]));
    ApplicationPolicy{
        unsub_entity_type: "users".to_string(),
        ghost_policies: ghost_gen_policies(), 
        edge_policies: edge_policies,
    }
}
