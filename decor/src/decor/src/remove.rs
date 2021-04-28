use crate::helpers::*;
use crate::stats::*;
use crate::types::*;
use crate::vault;
use sql_parser::ast::*;
use std::str::FromStr;

pub fn remove_obj_txn(
    disguise: &Disguise,
    tableinfo: &TableInfo,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let name = tableinfo.name.clone();
    let id_cols = tableinfo.id_cols.clone();
    let fks = &tableinfo.used_fks;

    let mut idents = vec![];
    for id in &disguise.guise_info.ids {
        idents.push(Ident::new(id))
    }
    let selection = None;

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
    // TODO need a way to figure out how to get fks to recorrelate...
    /*
       vault::reverse_vault_decor_referencer_entries(
        user_id,
        &name,
        &
        &disguise.guise_info.name,
        txn,
        stats,
    )?;
    */

    /*
     * PHASE 1: OBJECT SELECTION
     */
    let predicated_objs =
        get_query_rows_txn(&select_statement(&name, selection.clone()), txn, stats)?;

    /* PHASE 2: OBJECT MODIFICATION */
    get_query_rows_txn(
        &Statement::Delete(DeleteStatement {
            table_name: string_to_objname(&name),
            selection: selection,
        }),
        txn,
        stats,
    )?;

    /* PHASE 3: VAULT UPDATES */
    let mut vault_vals = vec![];
    for objrow in &predicated_objs {
        let ids : Vec<String> = id_cols
            .iter()
            .map(|c| get_value_of_col(objrow, &c).unwrap())
            .collect();
        for fk in fks {
            let uid = get_value_of_col(&objrow, &fk.referencer_col).unwrap();
            vault_vals.push(vault::VaultEntry {
                vault_id: 0,
                disguise_id: disguise.disguise_id,
                user_id: u64::from_str(&uid).unwrap(),
                guise_name: name.clone(),
                guise_id_cols: id_cols.clone(),
                guise_ids: ids.clone(),
                referencer_name: "".to_string(),
                update_type: vault::DELETE_GUISE,
                modified_cols: vec![],
                old_value: objrow.clone(),
                new_value: vec![],
                reverses: None,
            });
        }
    }
    vault::insert_vault_entries(&vault_vals, txn, stats)?;
    Ok(())
}

pub fn remove_obj_txn_for_user(
    user_id: u64,
    disguise: &Disguise,
    tableinfo: &TableInfo,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let name = tableinfo.name.clone();
    let id_cols = tableinfo.id_cols.clone();

    let mut selection = Expr::Value(Value::Boolean(false));
    // if this is the user table, check for ID equivalence
    if name == disguise.guise_info.name {
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
        for fk in &tableinfo.fks {
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
    // TODO need a way to figure out how to get fks to recorrelate...
    /*
       vault::reverse_vault_decor_referencer_entries(
        user_id,
        &name,
        &
        &disguise.guise_info.name,
        txn,
        stats,
    )?;
    */

    /*
     * PHASE 1: OBJECT SELECTION
     */
    let predicated_objs =
        get_query_rows_txn(&select_statement(&name, selection.clone()), txn, stats)?;

    /* PHASE 2: OBJECT MODIFICATION */
    get_query_rows_txn(
        &Statement::Delete(DeleteStatement {
            table_name: string_to_objname(&name),
            selection: selection,
        }),
        txn,
        stats,
    )?;

    /* PHASE 3: VAULT UPDATES */
    let mut vault_vals = vec![];
    for objrow in &predicated_objs {
        let ids = id_cols
            .iter()
            .map(|c| get_value_of_col(objrow, &c).unwrap())
            .collect();
        vault_vals.push(vault::VaultEntry {
            vault_id: 0,
            disguise_id: disguise.disguise_id,
            user_id: user_id,
            guise_name: name.clone(),
            guise_id_cols: id_cols.clone(),
            guise_ids: ids,
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
