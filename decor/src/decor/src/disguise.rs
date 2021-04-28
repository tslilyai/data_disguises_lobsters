use crate::stats::*;
use crate::types::*;
use crate::*;
use crate::{decorrelate, history, modify};

pub fn get_select(
    user_id: Option<u64>,
    tableinfo: &TableInfo,
    disguise: &Disguise,
) -> Option<Expr> {
    let mut select = None;
    match user_id {
        Some(user_id) => {
            let mut selection = Expr::Value(Value::Boolean(false));
            // if this is the user table, check for ID equivalence
            if tableinfo.name == disguise.guise_info.name {
                selection = Expr::BinaryOp {
                    left: Box::new(selection),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(vec![Ident::new(
                            disguise.guise_info.id.to_string(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                    }),
                };
            } else {
                // otherwise, we want to remove all objects possibly referencing the user
                // NOTE : this assumes that all "used_fks" point to users table
                for fk in &tableinfo.used_fks {
                    selection = Expr::BinaryOp {
                        left: Box::new(selection),
                        op: BinaryOperator::Or,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(vec![Ident::new(
                                fk.referencer_col.to_string(),
                            )])),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                        }),
                    };
                }
            }
            select = Some(selection);
        }
        None => (),
    }
    select
}

pub fn apply(
    user_id: Option<u64>,
    disguise: &Disguise,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let de = history::DisguiseEntry {
        disguise_id: disguise.disguise_id,
        user_id: match user_id {
            Some(u) => u,
            None => 0,
        },
        reverse: false,
    };

    // REMOVAL
    for tableinfo in &disguise.remove_names {
        remove::remove_obj_txn(user_id, disguise, tableinfo, txn, stats)?;
    }

    // DECORRELATION
    for tableinfo in &disguise.update_names {
        modify::modify_obj_txn(user_id, disguise, tableinfo, txn, stats)?;
        match user_id {
            Some(uid) => {
                decorrelate::decor_obj_txn_for_user(uid, disguise, tableinfo, txn, stats)?;
            }
            None => {
                decorrelate::decor_obj_txn(disguise, tableinfo, txn, stats)?;
            }
        }
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
