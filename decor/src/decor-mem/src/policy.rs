pub enum GeneratePolicy {
    Random,
    Default,
    Custom<F>(f: F) // where F: FnMut(Column) -> Column,
}
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(gp: GeneratePolicy),
    Generate(gp: GeneratePolicy),
}
pub type EntityGhostPolicy = Vec<GhostColumnPolicy>;

pub enum ClusterPolicy {
    // Do not break up these clusters
    // The entities in the cluster and their dependencies are removed.
    NoDecorRemove {
        cluster_entity: Entity,
        identifier_entity: Entity,
    }

    // Do not break up these clusters, and add ghosts to the cluster.
    // Must specify a cluster ghost generation policy so that we can 
    // add ghosts.
    NoDecorThreshold {
        cluster_entity: Entity,
        identifier_entity: Entity,
        cluster_threshold: f64,
        cluster_ghost_policy: EntityGhostPolicy,
    }
    
    // Decorrelate these clusters from their identifier by breaking the
    // identifier into ghosts.
    // Must specify an identifier ghost generation policy so that we can 
    // add ghosts.
    Decor {
        cluster_entity: Entity,
        identifier_entity: Entity,
        identifier_ghosts_policy: EntityGhostPolicy,
    }
}

pub type ApplicationPolicy = Vec<ClusterPolicy>;
