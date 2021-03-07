use std::*;
use sql_parser::ast::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use log::{warn};
use std::sync::atomic::{Ordering, AtomicU64};

use crate::{helpers, ID_COL};
use crate::views::{Views};
use crate::types::{RowPtrs, RowPtr, ObjectIdentifier};
use crate::policy::{GuiseColumnPolicy, GeneratePolicy, ObjectGuisePolicies};

pub const GHOST_ID_START : u64 = 1<<20;
pub const GHOST_ID_MAX: u64 = 1<<30;
static LAST_GID: AtomicU64 = AtomicU64::new(GHOST_ID_START);

/* 
 * a single table's guises and their rptrs
 */
#[derive(Debug, Clone)]
pub struct TableGuiseEntities {
    pub table: String, 
    pub gids: Vec<u64>,
    pub rptrs: RowPtrs,
}

/* 
 * a root guise and the descendant guises 
 */
#[derive(Debug, Clone)]
pub struct GuiseFamily {
    pub root_table: String,
    pub root_gid: u64,
    pub family_members: Vec<TableGuiseEntities>,
}
impl GuiseFamily {
    pub fn guise_family_to_db_string(&self, oid: u64) -> String {
        let mut guise_names : Vec<(String, u64)> = vec![];
        for tableguises in &self.family_members{
            for gid in &tableguises.gids {
                guise_names.push((tableguises.table.to_string(), *gid));
            }
        }
        let guisedata = serde_json::to_string(&guise_names).unwrap();
        warn!("Guisedata serialized is {}", guisedata);
        let guisedata = helpers::escape_quotes_mysql(&guisedata);
        format!("({}, {}, '{}')", self.root_gid, oid, guisedata)
    }
}

/*
 * A variant of oid -> family of guises to store on-disk or serialize (no row pointers!)
 */
#[derive(Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Clone, Debug)]
pub struct GuiseOidMapping {
    pub name: ObjectIdentifier,
    pub root_gids: Vec<u64>,
    pub guises: Vec<(String, u64)>,
}

/* 
 * a base true object upon which to generate guises
 */
pub struct TemplateObject {
    pub name: ObjectIdentifier,
    pub row: RowPtr,
    pub fixed_colvals: Option<Vec<(usize, Value)>>,
}

pub fn is_guise_oid(gid: u64) -> bool {
    gid >= GHOST_ID_START
}

pub fn is_guise_oidval(val: &Value) -> bool {
    let gid = helpers::parser_val_to_u64(val);
    gid >= GHOST_ID_START
}

fn generate_new_guise_gids(needed: usize) -> Vec<Value> {
    let mut gids = vec![];
    let first_gid = LAST_GID.fetch_add(needed as u64, Ordering::SeqCst);
    for n in 0..needed {
        gids.push(Value::Number((first_gid + n as u64).to_string()));
    }
    gids
}

pub fn generate_new_guises_from(
    views: &Views,
    guise_policies: &ObjectGuisePolicies,
    template: &TemplateObject, 
    num_guises: usize,
) -> Result<Vec<TableGuiseEntities>, mysql::Error>
{
    use GuiseColumnPolicy::*;
    let start = time::Instant::now();
    let mut new_entities : Vec<TableGuiseEntities> = vec![];
    let from_cols = views.get_view_columns(&template.name.table);
    let gids = generate_new_guise_gids(num_guises);

    // NOTE : generating entities with foreign keys must also have ways to 
    // generate foreign key object or this will panic
    // If no guise generation policy is specified, we clone all
    let policies : Vec<&GuiseColumnPolicy> = match guise_policies.get(&template.name.table) {
        Some(gp) => from_cols.iter().map(|col| match gp.get(&col.to_string()) {
            Some(pol) => pol,
            None => &CloneAll,
        }).collect(),
        None => from_cols.iter().map(|_| &CloneAll).collect(),
    };
    let num_entities = gids.len();
    let mut new_vals : RowPtrs = vec![]; 
    for _ in 0..num_entities {
        new_vals.push(Rc::new(RefCell::new(vec![Value::Null; from_cols.len()]))); 
    }
    'col_loop: for (i, col) in from_cols.iter().enumerate() {
        let colname = col.to_string();
        
        // put gid for ID attribute
        if colname == ID_COL {
            for n in 0..num_entities {
                new_vals[n].borrow_mut()[i] = gids[n].clone();
            }
            continue 'col_loop;            
        }

        // set colval if specified
        if let Some(fixed) = &template.fixed_colvals {
            for (ci, val) in fixed {
                if i == *ci {
                    warn!("Generating value for col {} using fixed val {}", col, val);
                    for n in 0..num_entities {
                        new_vals[n].borrow_mut()[*ci] = val.clone();
                    } 
                    continue 'col_loop;
                }
            }
        }

        // otherwise, just follow policy
        let clone_val = &template.row.borrow()[i];
        warn!("Generating value for {}", col);
        match policies[i] {
            CloneAll => {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[i] = clone_val.clone();
                }
            }
            CloneOne(gen) => {
                // clone the value for the first row
                new_vals[0].borrow_mut()[i] = template.row.borrow()[i].clone();
                for n in 1..num_entities {
                    new_vals[n].borrow_mut()[i] = get_generated_val(views, guise_policies, &gen, clone_val, &mut new_entities)?;
                }
            }
            Generate(gen) => {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[i] = get_generated_val(views, guise_policies, &gen, clone_val, &mut new_entities)?;
                }
            }
        }
    }

    new_entities.push(TableGuiseEntities{
        table: template.name.table.to_string(), 
        gids: gids.iter().map(|gval| helpers::parser_val_to_u64(&gval)).collect(),
        rptrs: new_vals,
    });
 
    warn!("GHOSTS: adding {} new entities {:?} for table {}, dur {}", 
          num_entities, new_entities, template.name.table, start.elapsed().as_micros());
    Ok(new_entities)
}

pub fn generate_foreign_key_val(
    views: &Views,
    guise_policies: &ObjectGuisePolicies,
    table_name: &str,
    template_oid: u64,
    new_entities: &mut Vec<TableGuiseEntities>)
    -> Result<Value, mysql::Error> 
{
    // assumes there is at least one value here...
    let parent_table_row = views.get_row_of_id(table_name, template_oid);

    warn!("GHOSTS: Generating foreign key object for {} {:?}", table_name, parent_table_row);
    let mut guise_parent_fam = generate_new_guises_from(
        views, guise_policies,
        &TemplateObject{
            name: ObjectIdentifier {
                table: table_name.to_string(),
                oid: template_oid,
            },
            row: parent_table_row,
            fixed_colvals: None,
        }, 1)?;
    let gidval = Value::Number(guise_parent_fam[0].gids[0].to_string());
    new_entities.append(&mut guise_parent_fam);
    Ok(gidval)
}

pub fn get_generated_val(
    views: &Views,
    guise_policies: &ObjectGuisePolicies,
    gen: &GeneratePolicy, 
    base_val: &Value,
    new_entities: &mut Vec<TableGuiseEntities>,
) -> Result<Value, mysql::Error> {
    use GeneratePolicy::*;
    match gen {
        Random => Ok(helpers::get_random_parser_val_from(&base_val)),
        Default(val) => Ok(helpers::get_default_parser_val_with(&base_val, &val)),
        Custom(f) => Ok(helpers::get_computed_parser_val_with(&base_val, &f)),
        ForeignKey(table_name) => generate_foreign_key_val(
            views, guise_policies, 
            table_name, helpers::parser_val_to_u64(base_val),
            new_entities),
    }
}
