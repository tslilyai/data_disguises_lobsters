use std::collections::HashMap;
use edna::*;

pub fn get_did() -> DID {
    0
}

pub fn apply(
    edna: &mut EdnaClient,
    user_email: UID,
    decryption_cap: tokens::DataCap,
    loc_caps: Vec<tokens::LocCap>,
) -> Result<
    (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ),
    mysql::Error,
> {
    edna.start_disguise(get_did());
    Ok(edna.end_disguise(get_did()))
}

// REMOVE USER
// REMOVE ANSWERS

pub fn reveal(
    edna: &mut EdnaClient,
    decryption_cap: tokens::DataCap,
    diff_loc_caps: Vec<tokens::LocCap>,
    ownership_loc_caps: Vec<tokens::LocCap>,
) -> Result<(), mysql::Error> {
    Ok(())
}
// REMOVE USER
// REMOVE ANSWERS
