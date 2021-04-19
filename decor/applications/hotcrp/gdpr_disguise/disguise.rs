use crate::conference_anon_disguise;
use crate::decorrelate;
use crate::gdpr_disguise::constants::*;
use crate::*;
use decor::disguises::*;
use decor::helpers::stats::QueryStat;
use decor::helpers::*;
use mysql::TxOpts;
use sql_parser::ast::*;

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

    let de = DisguiseEntry {
        disguise_id: GDPR_DISGUISE_ID,
        user_id: user_id,
        reverse: true,
    };

    // only apply if disguise has been applied
    if !is_disguise_reversed(&de, txn, stats)? {
        // TODO undo disguise

        insert_disguise_history_entry(&de, txn, stats)?;
    }
    Ok(())
}

fn remove_obj_txn(
    user_id: u64,
    name: &str,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let selection = Some(Expr::BinaryOp {
        left: Box::new(Expr::Identifier(vec![
            Ident::new(SCHEMA_UID_COL.to_string()), // assumes fkcol is uid_col
        ])),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
    });

    /*
     * PHASE 1: OBJECT SELECTION
     */
    let predicated_objs =
        get_query_rows_txn(&select_statement(name, selection.clone()), txn, stats)?;

    /* PHASE 2: OBJECT MODIFICATION */
    get_query_rows_txn(
        &Statement::Delete(DeleteStatement {
            table_name: string_to_objname(SCHEMA_UID_TABLE),
            selection: selection,
        }),
        txn,
        stats,
    )?;

    /* PHASE 3: VAULT UPDATES */
    let mut vault_vals = vec![];
    for objrow in &predicated_objs {
        vault_vals.push(VaultEntry {
            vault_id: 0,
            disguise_id: GDPR_DISGUISE_ID,
            user_id: user_id,
            guise_name: name.to_string(),
            guise_id: 0,
            referencer_name: "".to_string(),
            update_type: DELETE_GUISE,
            modified_cols: vec![],
            old_value: objrow.clone(),
            new_value: vec![],
            reversed: false,
        });
    }
    insert_vault_entries(&vault_vals, txn, stats)?;
    Ok(())
}

pub fn apply(
    user_id: Option<u64>,
    db: &mut mysql::Conn,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    // user must be provided as input
    let user_id = user_id.unwrap();

    let mut txn = db.start_transaction(TxOpts::default())?;

    let de = DisguiseEntry {
        disguise_id: GDPR_DISGUISE_ID,
        user_id: user_id,
        reverse: false,
    };

    // only apply if disguise is reversed (or hasn't been applied)
    if is_disguise_reversed(&de, &mut txn, stats)? {
        // UNDO PRIOR DISGUISES
        // TODO check if previous disguises have been applied; if so, undo them for this user
        conference_anon_disguise::undo(Some(user_id), &mut txn, stats)?;

        // DECORRELATION TXNS
        for tablefk in get_decor_names() {
            decorrelate::decor_obj_txn_for_user(user_id, GDPR_DISGUISE_ID, &tablefk, &mut txn, stats)?;
        }

        // REMOVAL TXNS
        for name in get_remove_names() {
            remove_obj_txn(user_id, name, &mut txn, stats)?;
        }

        insert_disguise_history_entry(&de, &mut txn, stats)?;
    }

    txn.commit()
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
