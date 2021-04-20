use crate::*;
use decor::helpers::*;
use decor::vault;
use sql_parser::ast::*;

pub fn remove_obj_txn_for_user(
    user_id: u64,
    disguise_id: u64,
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
     * PHASE 0: What vault operations must come "after" removal?
     * ==> Those that have made the object to remove inaccessible, namely those that would have
     * satisfied the predicate, but no longer do.
     *
     * TODO also undo any operations that happened in that disguise after these decorrelation
     * modifications?
     *
     * TODO this only is correct if all decorrelated FKs are to the contactInfo table
     *
     * Note: we don't need to redo these because deletion is final!
     */
    if name != SCHEMA_UID_TABLE {
        vault::reverse_vault_decor_referencer_entries(
            user_id,
            name,
            SCHEMA_UID_COL,
            SCHEMA_UID_TABLE,
            txn,
            stats,
        )?;
    }

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
        vault_vals.push(vault::VaultEntry {
            vault_id: 0,
            disguise_id: disguise_id,
            user_id: user_id,
            guise_name: name.to_string(),
            guise_id: 0,
            referencer_name: "".to_string(),
            update_type: vault::DELETE_GUISE,
            modified_cols: vec![],
            old_value: objrow.clone(),
            new_value: vec![],
            reverses: None,
        });
    }
    vault::insert_vault_entries(&vault_vals, txn, stats)?;
    Ok(())
}


