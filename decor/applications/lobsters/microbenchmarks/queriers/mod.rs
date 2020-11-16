pub(crate) mod frontpage; // read-only
pub(crate) mod vote; // read story + insert vote + update hotness
pub(crate) mod user; // load profile of user
pub(crate) mod post_story; // post story
pub(crate) mod comment; // add comment to story
pub(crate) mod expensive_queries; // for benchmarking
