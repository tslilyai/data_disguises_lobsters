use crate::backend::MySqlBackend;
use edna::*;
use std::collections::HashMap;

pub fn get_did() -> DID {
    0
}

pub fn apply(
    bg: &mut MySqlBackend,
    user_email: UID,
    decryption_cap: tokens::DecryptCap,
    loc_caps: Vec<tokens::LocCap>,
) -> Result<
    (
        HashMap<(UID, DID), tokens::LocCap>,
        HashMap<(UID, DID), tokens::LocCap>,
    ),
    mysql::Error,
> {
    bg.edna.start_disguise(get_did());
    Ok(bg.edna.end_disguise(get_did()))
}

// REMOVE USER
// REMOVE ANSWERS

pub fn reveal(
    bg: &mut MySqlBackend,
    decryption_cap: tokens::DecryptCap,
    diff_loc_caps: Vec<tokens::LocCap>,
    ownership_loc_caps: Vec<tokens::LocCap>,
) -> Result<(), mysql::Error> {
    
    let (diff_toks, own_toks) = bg.edna.get_tokens_of_disguise_and_mark_revealed(
        get_did(),
        decryption_cap,
        diff_loc_caps,
        ownership_loc_caps,
    );

    Ok(())
}
// REMOVE USER
// REMOVE ANSWERS
