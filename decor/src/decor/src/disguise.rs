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
                            disguise.guise_info.id_col.to_string(),
                        )])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::Number(user_id.to_string()))),
                    }),
                };
            } else {
                // otherwise, we want to remove all objects possibly referencing the user
                // NOTE : this assumes that all "fks_to_decor" point to users table
                for fk in &tableinfo.fks_to_decor {
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
