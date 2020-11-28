pub type Column: String; // column name
pub type Attribute: String; // entity or column name
pub type Entity: String; // table name, or foreign key

pub enum GeneratePolicy {
    Random,
    Default,
    Custom<F>(f: F), // where F: FnMut(Column) -> Column,
    ForeignKey, // generate a foreign ghost and set value to the ID for this ghost
}
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(gp: GeneratePolicy),
    Generate(gp: GeneratePolicy),
}
pub type GhostPolicy = HashMap<Column, GhostColumnPolicy>;
pub type AttributeGhostPolicies = HashMap<Attribute, GhostPolicy>;

pub struct Cluster {
    cluster_entity: Entity,
    identifier: Attribute,
    column_name: String,
}

pub enum ClusterPolicy {
    // Do not break up these clusters
    // The entities in the cluster and their dependencies are removed.
    NoDecorRemove(Cluster),

    // The entities in the cluster are kept, without adding any noise
    NoDecorRetain(Cluster),

    // Do not break up these clusters, and add ghosts to the cluster.
    // Must specify a cluster ghost generation policy so that we can 
    // add ghosts.
    NoDecorThreshold {
        c: Cluster,
        threshold: f64,
    }
    
    // Decorrelate these clusters from their identifier by breaking the
    // identifier into ghosts.
    // Must specify an identifier ghost generation policy so that we can 
    // add ghosts.
    Decor(Cluster),
}

pub type ApplicationPolicy = (AttributeGhostPolicies, Vec<ClusterPolicy>);
