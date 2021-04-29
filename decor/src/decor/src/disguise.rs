use crate::stats::*;
use crate::types::*;
use crate::*;
use crate::{decorrelate, history, modify};

pub fn apply(
    disguise: &Disguise,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = history::DisguiseEntry {
        user_id: disguise.user_id.unwrap_or(0),
        disguise_id: disguise.disguise_id,
        reverse: false,
    };

    // REMOVAL
    for tableinfo in &disguise.remove_names {
        remove::remove_obj_txn(disguise, tableinfo, txn, stats)?;
    }

    // DECORRELATION
    for tableinfo in &disguise.update_names {
        modify::modify_obj_txn(disguise, tableinfo, txn, stats)?;
        decorrelate::decor_obj_txn(disguise, tableinfo, txn, stats)?;
    }
    record_disguise(&de, txn, stats)?;
    Ok(())
}

pub fn undo(
    user_id: Option<u64>,
    disguise_id: u64,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = history::DisguiseEntry {
        disguise_id: disguise_id,
        user_id: match user_id {
            Some(u) => u,
            None => 0,
        },
        reverse: true,
    };

    // only reverse if disguise has been applied
    if !history::is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        record_disguise(&de, txn, stats)?;
    }
    Ok(())
}
