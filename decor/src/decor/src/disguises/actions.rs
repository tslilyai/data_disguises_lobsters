use crate::types::*;
use crate::{helpers, policy};
use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::*;

pub type GID = ID;
pub type RefrID = ID;

fn set_gid_closure(gid: u64) -> Box<dyn Fn(&str) -> String> {
    Box::new(move |_| gid.to_string())
}

// optionally returns a GID if a GID is created
pub fn copy_guise_with_modifications(
    gid: &GID,
    mods: &GuiseModifications,
    db: &mut mysql::Conn,
) -> Result<GID, mysql::Error> {
    let new = gid.copy_row_with_modifications(mods, db)?;
    Ok(ID {
        table: gid.table.clone(),
        id: new,
        id_col_index: gid.id_col_index,
        id_col_name: gid.id_col_name.clone(),
    })
}

pub fn redirect_referencer(
    fkcol: &ForeignKeyCol,
    rid: &RefrID,
    new_id: u64,
    db: &mut mysql::Conn,
) -> Result<(), mysql::Error> {
    let mods = vec![(
        TableCol {
            table: fkcol.referencer_table.clone(),
            col_name: fkcol.col_name.clone(),
            col_index: fkcol.col_index,
        },
        set_gid_closure(new_id),
    )];
    rid.update_row_with_modifications(&mods, db)
}

pub fn delete_guise(
    gid: &GID,
    schema_config: &policy::SchemaConfig,
    db: &mut mysql::Conn,
) -> Result<Vec<Row>, mysql::Error> {
    let mut removed = vec![];
    let mut seen = HashSet::new();
    let mut to_traverse = vec![];
    let mut table_to_ids: HashMap<String, Vec<u64>> = HashMap::new();

    // do recursive traversal to get descendants, ignore cycles
    to_traverse.push(gid.clone());
    while to_traverse.len() > 0 {
        let id = to_traverse.pop().unwrap();
        seen.insert(id.clone());

        let refs = id.get_referencers(schema_config, db)?;
        for (ridvec, _) in refs {
            for rid in ridvec {
                if seen.get(&rid.id) == None {
                    to_traverse.push(rid.id.clone());
                    removed.push(rid.clone());
                    match table_to_ids.get_mut(&id.table) {
                        Some(ids) => ids.push(id.id),
                        None => {
                            table_to_ids.insert(id.table.clone(), vec![id.id]);
                        }
                    }
                }
            }
        }
    }

    // actually delete, one query per table
    for (table, ids) in table_to_ids.iter() {
        let id_exprs: Vec<Expr> = ids
            .iter()
            .map(|id| Expr::Value(Value::Number(id.to_string())))
            .collect();
        let id_col_info = &schema_config.table_info.get(table).unwrap().id_col_info;
        let selection = Some(Expr::InList {
            expr: Box::new(Expr::Value(Value::String(id_col_info.col_name.clone()))),
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
    Ok(removed)
}
