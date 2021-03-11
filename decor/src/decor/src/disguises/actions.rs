use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::*;
use log::{warn};
use crate::types::*;

pub type GID = ID;
pub type RefrID = ID;

pub enum Action {
    // insert copy of guise of table Table with id GID
    pub CopyGuise(GID),
    // modify the table column of guise with ID GID according to the custom fxn
    pub ModifyGuise(GID, Vec<(TableCol, Box<dyn Fn(&str) -> String)>),
    // rewrite the referencer's value in the current TableCol column to point to GID
    pub RedirectReferencer(ForeignKeyCol, RefrID, GID),
    // delete the referencer and descendants
    pub DeleteReferencer(RefrID),
    // delete the specified guise and descendants 
    pub DeleteGuise(GID),
}

// optionally returns a GID if a GID is created
pub fn perform_action(
    action: Action,
    db: &mut mysql::Conn,
) -> Result<Option<GID>, mysql::Error>
{
    warn!("perform action: {}", action);
    
    match action {
        CopyGuise(gid) => {
            let new = gid.copy_row_with_modifications(vec![], db)?;
            Ok(Some(new))
        }
        ModifyGuise(gid, mods) => {
            gid.update(mods, db)?;
            Ok(None)
        }
        RedirectReferencer(fkcol, rid, gid) => {
            let update_f = |_| gid.id;
            let mods = vec![(
                TableCol {
                    table: fkcol.child_table,
                    colname: fkcol.colname,
                    colindex: fkcol.colindex,
                }, 
                Box::new(update_f)
            )];
            rid.update(mods, db);
        }
        DeleteReferencer(rid) => {
            recursive_remove_guise(rid, schema_config, db)?;
            Ok(None)
        }
        DeleteGuise(gid) => {
            recursive_remove_guise(gid, schema_config, db)?;
            Ok(None)
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
        id = to_traverse.pop().unwrap();
        seen.push(id);

        let refs = id.get_referencers(schema_config, db);
        for rid in refs {
            if seen.get(rid) == None {
                to_traverse.push(rid);
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
        let id_col = schema.get(&table).unwrap();
        let selection = Some(Expr::InList{
                expr: Box::new(id_col.col_name),
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
