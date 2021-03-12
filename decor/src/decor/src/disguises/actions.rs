use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::*;
use crate::types::*;
use crate::{policy, helpers};

pub type GID = ID;
pub type RefrID = ID;

fn set_gid_closure(gid: u64) -> impl Fn(&str) -> String {
    Box::new(move |_| gid.to_string())
}

pub enum Action {
    // insert copy of guise of table Table with id GID
    CopyGuise(GID),
    // modify the table column of guise with ID GID according to the custom fxn
    ModifyGuise(GID, Vec<(TableCol, impl Fn(&str) -> String)>),
    // rewrite the referencer's value in the current TableCol column to point to GID
    RedirectReferencer(ForeignKeyCol, RefrID, GID),
    // delete the referencer and descendants
    DeleteReferencer(RefrID),
    // delete the specified guise and descendants 
    DeleteGuise(GID),
}

// optionally returns a GID if a GID is created
pub fn perform_action(
    action: Action,
    schema_config: &policy::SchemaConfig,
    db: &mut mysql::Conn,
) -> Result<Option<GID>, mysql::Error>
{
    use Action::*;
    //warn!("perform action: {}", action);
    
    match action {
        CopyGuise(gid) => {
            let new = gid.copy_row_with_modifications(vec![], db)?;
            return Ok(Some(ID{
                table: gid.table,
                id: new,
                id_col_index: gid.id_col_index,
                id_col_name: gid.id_col_name,
            }));
        }
        ModifyGuise(gid, mods) => {
            gid.update_row_with_modifications(mods, db)?;
        }
        RedirectReferencer(fkcol, rid, gid) => {
            let mods = vec![(
                TableCol {
                    table: fkcol.child_table,
                    col_name: fkcol.col_name,
                    col_index: fkcol.col_index,
                }, 
                set_gid_closure(gid.id)
            )];
            rid.update_row_with_modifications(mods, db);
        }
        DeleteReferencer(rid) => {
            recursive_remove_guise(rid, schema_config, db)?;
        }
        DeleteGuise(gid) => {
            recursive_remove_guise(gid, schema_config, db)?;
        }
    }
    Ok(None)
}

fn recursive_remove_guise(gid: ID, schema_config: &policy::SchemaConfig, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
    let mut seen = HashSet::new();
    let mut to_traverse = vec![];
    let mut table_to_ids : HashMap<String, Vec<u64>> = HashMap::new();

    // do recursive traversal to get descendants, ignore cycles
    to_traverse.push(gid);
    while to_traverse.len() > 0 {
        let id = to_traverse.pop().unwrap();
        seen.insert(id.clone());

        let refs = id.get_referencers(schema_config, db)?;
        for rid in &refs {
            if seen.get(rid) == None {
                to_traverse.push(rid.clone());
                match table_to_ids.get_mut(&id.table) {
                    Some(ids) => ids.push(id.id),
                    None => {
                        table_to_ids.insert(id.table.clone(), vec![id.id]);
                    }
                }
            }
        }
    }

    // actually delete, one query per table
    for (table, ids) in table_to_ids.iter() {
        let id_exprs : Vec<Expr> = ids.iter().map(|id| Expr::Value(Value::Number(id.to_string()))).collect();
        let id_col = schema_config.id_cols.get(table).unwrap();
        let selection = Some(Expr::InList{
                expr: Box::new(Expr::Value(Value::String(id_col.col_name.clone()))),
                list: id_exprs,
                negated: false,
        });
        let delete_from_table = Statement::Delete(DeleteStatement {
            table_name: helpers::string_to_objname(&table),
            selection: selection.clone(),
        });
        db.query_drop(format!("{}", delete_from_table.to_string()))?;
        //self.cur_stat.nqueries+=1;
        //self.cur_stat.nobjects+=ids.len();
    }
    Ok(())
}
