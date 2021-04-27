use crate::gdpr_disguise::constants::*;
use crate::*;
use decor::decorrelate;
use decor::history;
use decor::remove;

/*
 * GDPR REMOVAL DISGUISE
 */

pub fn undo(
    user_id: Option<u64>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // user must be provided as input
    let user_id = user_id.unwrap();

    let de = history::DisguiseEntry {
        disguise_id: GDPR_DISGUISE_ID,
        user_id: user_id,
        reverse: true,
    };

    // only apply if disguise has been applied
    if !history::is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        decor::record_disguise(&de, txn, stats)?;
    }
    Ok(())
}

pub fn apply(
    user_id: Option<u64>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // user must be provided as input
    let user_id = user_id.unwrap();

    let de = history::DisguiseEntry {
        disguise_id: GDPR_DISGUISE_ID,
        user_id: user_id,
        reverse: false,
    };

    // only apply if disguise is reversed (or hasn't been applied)
    if history::is_disguise_reversed(&de, txn, stats)? {
        // DECORRELATION TXNS
        for tableinfo in get_update_names() {
            decorrelate::decor_obj_txn_for_user(
                user_id,
                GDPR_DISGUISE_ID,
                &tableinfo,
                SCHEMA_UID_COL,
                datagen::get_insert_guise_contact_info_cols,
                datagen::get_insert_guise_contact_info_vals,
                txn,
                stats,
            )?;
        }

        // REMOVAL TXNS
        for tableinfo in get_remove_names() {
            remove::remove_obj_txn_for_user(
                user_id,
                GDPR_DISGUISE_ID,
                &tableinfo,
                SCHEMA_UID_COL,
                SCHEMA_UID_TABLE,
                txn,
                stats,
            )?;
        }

        decor::record_disguise(&de, txn, stats)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn apply_none() {
        let _ = env_logger::builder()
            // Include all events in tests
            .filter_level(log::LevelFilter::Warn)
            //.filter_level(log::LevelFilter::Error)
            // Ensure events are captured by `cargo test`
            .is_test(true)
            // Ignore errors initializing the logger if tests race to configure it
            .try_init();

        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url: String;
        let mut db: mysql::Conn;

        let test_dbname = "test_conf_none";
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        datagen::populate_database(&mut db).unwrap();
        let mut stats = QueryStat::new();
        apply(Some(1), &mut db, &mut stats).unwrap()
    }
}
