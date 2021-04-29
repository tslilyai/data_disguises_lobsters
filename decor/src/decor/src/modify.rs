use crate::helpers::*;
use crate::stats::QueryStat;
use crate::types::*;
use crate::vault;
use sql_parser::ast::*;
use std::str::FromStr;

pub fn modify_obj_txn(
    disguise: &Disguise,
    table_dis: &TableDisguise,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let name = &table_dis.name;
    let id_cols = table_dis.id_cols.clone();
    let modified_cols = &table_dis.cols_to_update;
    let fks = &table_dis.fks_to_decor;

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let mut vault_vals = vec![];
    for colmod in modified_cols {
        let objs = get_query_rows_txn(&select_statement(&name, colmod.modify_predicate), txn, stats)?;
        if objs.is_empty() {
            continue;
        }

        for obj in &objs {
            let old_val = get_value_of_col(&obj, &colmod.col).unwrap();
            let new_val = (*(colmod.generate_modified_value))(&old_val);

            let selection = get_select_of_row(table_dis, obj);

            /*
             * PHASE 2: OBJECT MODIFICATIONS
             * */
            get_query_rows_txn(
                &Statement::Update(UpdateStatement {
                    table_name: string_to_objname(&name),
                    assignments: vec![Assignment {
                        id: Ident::new(colmod.col.clone()),
                        value: Expr::Value(Value::String(new_val.clone())),
                    }],
                    selection: Some(selection),
                }),
                txn,
                stats,
            )?;

            /*
             * PHASE 3: VAULT UPDATES
             * */
            let new_obj: Vec<RowVal> = obj
                .iter()
                .map(|v| {
                    if v.column == colmod.col {
                        RowVal {
                            column: v.column.clone(),
                            value: new_val.clone(),
                        }
                    } else {
                        v.clone()
                    }
                })
                .collect();
            
            // XXX insert a vault entry for every owning user (every fk)
            // should just update for the calling user, if there is one?
            let ids = get_ids(table_dis, obj);
            for fk in fks {
                let uid = get_value_of_col(&obj, &fk.referencer_col).unwrap();
                vault_vals.push(vault::VaultEntry {
                    vault_id: 0,
                    disguise_id: disguise.disguise_id,
                    user_id: u64::from_str(&uid).unwrap(),
                    guise_name: name.clone(),
                    guise_id_cols: id_cols.clone(),
                    guise_ids: ids.clone(),
                    referencer_name: "".to_string(),
                    update_type: vault::UPDATE_GUISE,
                    modified_cols: modified_cols.iter().map(|mc| mc.col.clone()).collect(),
                    old_value: obj.clone(),
                    new_value: new_obj.clone(),
                    reverses: None,
                });
            }
        }
    }
    /* PHASE 3: Batch vault updates */
    vault::insert_vault_entries(&vault_vals, txn, stats)?;
    Ok(())
}
