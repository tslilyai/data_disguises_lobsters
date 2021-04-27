use crate::helpers::*;
use crate::stats::QueryStat;
use crate::types::*;
use crate::vault;
use sql_parser::ast::*;
use std::str::FromStr;

pub fn modify_obj_txn(
    disguise: &Disguise,
    tableinfo: &TableInfo,
    txn: &mut mysql::Transaction,
    stats: &mut QueryStat,
) -> Result<(), mysql::Error> {
    let name = &tableinfo.name;
    let id_cols = tableinfo.id_cols.clone();
    let modified_cols = &tableinfo.used_cols;
    let fks = &tableinfo.used_fks;

    /* PHASE 1: SELECT REFERENCER OBJECTS */
    let objs = get_query_rows_txn(&select_statement(&name, None), txn, stats)?;
    if objs.is_empty() {
        return Ok(());
    }

    let mut vault_vals = vec![];
    for obj in &objs {
        for colmod in modified_cols {
            let new_val = (*(colmod.generate_modified_value))();
            
            let mut selection = Expr::Value(Value::Boolean(true));
            let ids: Vec<String> = id_cols
                .iter()
                .map(|id_col| get_value_of_col(&obj, &id_col).unwrap())
                .collect();
            for (i, id) in ids.iter().enumerate() {
                let eq_selection = Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(id_cols[i].clone())])),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::String(id.to_string()))),
                };
                selection = Expr::BinaryOp {
                    left: Box::new(selection),
                    op: BinaryOperator::And,
                    right: Box::new(eq_selection),
                };
            }

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
            // insert a vault entry for every owning user
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
