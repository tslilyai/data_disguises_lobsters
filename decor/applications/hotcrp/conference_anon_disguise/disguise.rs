use crate::conference_anon_disguise::constants::*;
use crate::decorrelate;
use crate::*;
use decor::vault::*;
use decor::history::*;
use decor::helpers::*;
use sql_parser::ast::*;

/*
 * CONFERENCE ANONYMIZATION DISGUISE
 */

pub fn undo(
    user_id: Option<u64>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = DisguiseEntry {
        disguise_id: CONF_ANON_DISGUISE_ID,
        user_id: 0,
        reverse: true,
    };

    // only reverse if disguise has been applied
    if !is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        decor::record_disguise(&de, txn, stats)?;
    }
    Ok(())
}

pub fn undo_for_user(
    user_id: u64,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // Only decorrelated tables are "PaperReviewPreference" and "PaperWatch"

    /*
     * Get decorrelated children, and restore ownership
     */
    let mut children = get_user_entries_of_table_in_vault(
        user_id,
        "PaperReviewPreference",
        false, /* is_reversed */
        txn,
        stats,
    )?;
    children.append(&mut get_user_entries_of_table_in_vault(
        user_id,
        "PaperWatch",
        false, /* is_reversed */
        txn,
        stats,
    )?);
    // we need some way to be able to identify these objects...
    // assume that there is exactly one PaperWatch and one PaperReviewPreference for any user
    for c in children {
        let new_contact_id = get_value_of_col(&c.new_value, SCHEMA_UID_COL).unwrap();
        let old_contact_id = get_value_of_col(&c.old_value, SCHEMA_UID_COL).unwrap();
        assert!(old_contact_id == user_id.to_string());
        get_query_rows_txn(
            &Statement::Update(UpdateStatement {
                table_name: string_to_objname(&"PaperReviewPreference"),
                assignments: vec![Assignment {
                    id: Ident::new(SCHEMA_UID_COL.to_string()),
                    value: Expr::Value(Value::Number(user_id.to_string())),
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        SCHEMA_UID_COL.to_string(),
                    )])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(new_contact_id))),
                }),
            }),
            txn,
            stats,
        )?;
        mark_vault_entry_reversed(&c, txn, stats)?;
    }

    /*
     * Delete created guises from old disguise
     */
    let mut guise_ves = get_user_entries_with_referencer_in_vault(
        user_id,
        "PaperWatch",
        false, /* is_reversed */
        txn,
        stats,
    )?;
    guise_ves.append(&mut get_user_entries_with_referencer_in_vault(
        user_id,
        "PaperReviewPreference",
        false, /* is_reversed */
        txn,
        stats,
    )?);
    for guise in guise_ves {
        // TODO delete guise
        get_query_rows_txn(
            &Statement::Delete(DeleteStatement {
                table_name: string_to_objname(SCHEMA_UID_TABLE),
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        SCHEMA_UID_COL.to_string(),
                    )])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(guise.guise_id.to_string()))),
                }),
            }),
            txn,
            stats,
        )?;
        // mark vault entries as reversed
        mark_vault_entry_reversed(&guise, txn, stats)?;
    }

    Ok(())
}


pub fn apply(
    user_id: Option<u64>,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // we should be able to reapply the conference anonymization disguise, in case more data has
    // been added in the meantime
    let de = DisguiseEntry {
        disguise_id: CONF_ANON_DISGUISE_ID,
        user_id: 0,
        reverse: false,
    };
    
    // DECORRELATION
    for tablefk in get_decor_names() {
        match user_id {
            Some(uid) => decorrelate::decor_obj_txn_for_user(uid, CONF_ANON_DISGUISE_ID, &tablefk, txn, stats)?,
            None => decorrelate::decor_obj_txn(CONF_ANON_DISGUISE_ID, &tablefk, txn, stats)?,
        }
    }
    decor::record_disguise(&de, txn, stats)?;
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

        let test_dbname = "test_gdpr_none";
        url = String::from("mysql://tslilyai:pass@127.0.0.1");
        db = mysql::Conn::new(&url).unwrap();
        db.query_drop(&format!("DROP DATABASE IF EXISTS {};", &test_dbname))
            .unwrap();
        db.query_drop(&format!("CREATE DATABASE {};", &test_dbname))
            .unwrap();
        assert_eq!(db.ping(), true);
        assert_eq!(db.select_db(&format!("{}", test_dbname)), true);
        warn!("***************** POPULATING ****************");
        datagen::populate_database(&mut db).unwrap();
        warn!("***************** APPLYING CONF_ANON DISGUISE ****************");
        let mut stats = QueryStat::new();
        apply(None, &mut db, &mut stats).unwrap()
    }
}
