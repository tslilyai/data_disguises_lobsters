use std::*;
use sql_parser::ast::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use log::{debug, warn, error};
use std::sync::atomic::{Ordering, AtomicU64};

use crate::{helpers, views, ID_COL};
use crate::views::{Views, RowPtrs, RowPtr};
use crate::policy::{GhostColumnPolicy, GeneratePolicy, EntityGhostPolicies};

pub const GHOST_ID_START : u64 = 1<<20;
pub const GHOST_ID_MAX: u64 = 1<<30;
static LAST_GID: AtomicU64 = AtomicU64::new(GHOST_ID_START);

/* 
 * a single table's ghosts and their rptrs
 */
#[derive(Debug, Clone)]
pub struct TableGhostEntities {
    pub table: String, 
    pub gids: Vec<u64>,
    pub rptrs: RowPtrs,
}

/* 
 * a root ghost and the descendant ghosts 
 */
#[derive(Debug, Clone)]
pub struct GhostFamily {
    pub root_table: String,
    pub root_gid: u64,
    pub family_members: Vec<TableGhostEntities>,
}
impl GhostFamily {
    pub fn ghost_family_to_db_string(&self, eid: u64) -> String {
        let mut ghost_names : Vec<(String, u64)> = vec![];
        for tableghosts in &self.family_members{
            for gid in &tableghosts.gids {
                ghost_names.push((tableghosts.table.to_string(), *gid));
            }
        }
        let ghostdata = serde_json::to_string(&ghost_names).unwrap();
        warn!("Ghostdata serialized is {}", ghostdata);
        let ghostdata = helpers::escape_quotes_mysql(&ghostdata);
        format!("({}, {}, '{}')", self.root_gid, eid, ghostdata)
    }
}

/*
 * A variant of eid -> family of ghosts to store on-disk or serialize (no row pointers!)
 */
#[derive(Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Clone, Debug)]
pub struct GhostEidMapping {
    pub table: String,
    pub eid: u64,
    pub root_gid: u64,
    pub ghosts: Vec<(String, u64)>,
}

/* 
 * a base true entity upon which to generate ghosts
 */
pub struct TemplateEntity {
    pub table: String,
    pub row: RowPtr,
    pub fixed_colvals: Option<Vec<(usize, Value)>>,
}

pub fn is_ghost_eid(gid: u64) -> bool {
    gid >= GHOST_ID_START
}

pub fn is_ghost_eidval(val: &Value) -> bool {
    let gid = helpers::parser_val_to_u64(val);
    gid >= GHOST_ID_START
}

fn generate_new_ghost_gids(needed: usize) -> Vec<Value> {
    let mut gids = vec![];
    let first_gid = LAST_GID.fetch_add(needed as u64, Ordering::SeqCst) + 1;
    for n in 0..needed {
        gids.push(Value::Number((first_gid + n as u64).to_string()));
    }
    gids
}

pub fn generate_new_ghosts_from(
    views: &Views,
    ghost_policies: &EntityGhostPolicies,

    template: &TemplateEntity, 
    num_ghosts: usize,
) -> Result<Vec<TableGhostEntities>, mysql::Error>
{
    use GhostColumnPolicy::*;
    let start = time::Instant::now();
    let mut new_entities : Vec<TableGhostEntities> = vec![];
    let from_cols = views.get_view_columns(&template.table);
    let gids = generate_new_ghost_gids(num_ghosts);

    // NOTE : generating entities with foreign keys must also have ways to 
    // generate foreign key entity or this will panic
    let gp = ghost_policies.get(&template.table).unwrap();
    warn!("Getting policies from columns {:?}", from_cols);
    let policies : Vec<&GhostColumnPolicy> = from_cols.iter().map(|col| gp.get(&col.to_string()).unwrap()).collect();
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
                    new_vals[n].borrow_mut()[i] = get_generated_val(views, ghost_policies, &gen, clone_val, &mut new_entities)?;
                }
            }
            Generate(gen) => {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[i] = get_generated_val(views, ghost_policies, &gen, clone_val, &mut new_entities)?;
                }
            }
        }
    }

    new_entities.push(TableGhostEntities{
        table: template.table.to_string(), 
        gids: gids.iter().map(|gval| helpers::parser_val_to_u64(&gval)).collect(),
        rptrs: new_vals,
    });
 
    warn!("GHOSTS: adding {} new entities {:?} for table {}, dur {}", 
          num_entities, new_entities, template.table, start.elapsed().as_micros());
    Ok(new_entities)
}

pub fn generate_foreign_key_val(
    views: &views::Views,
    ghost_policies: &EntityGhostPolicies,
    table_name: &str,
    template_eid: u64,
    new_entities: &mut Vec<TableGhostEntities>)
    -> Result<Value, mysql::Error> 
{
    // assumes there is at least one value here...
    let parent_table_row = views.get_row_of_id(table_name, template_eid);

    warn!("GHOSTS: Generating foreign key entity for {} {:?}", table_name, parent_table_row);
    let mut ghost_parent_fam = generate_new_ghosts_from(
        views, ghost_policies,
        &TemplateEntity{
            table: table_name.to_string(),
            row: parent_table_row,
            fixed_colvals: None,
        }, 1)?;
    let gidval = Value::Number(ghost_parent_fam[0].gids[0].to_string());
    new_entities.append(&mut ghost_parent_fam);
    Ok(gidval)
}

pub fn get_generated_val(
    views: &views::Views,
    ghost_policies: &EntityGhostPolicies,
    gen: &GeneratePolicy, 
    base_val: &Value,
    new_entities: &mut Vec<TableGhostEntities>,
) -> Result<Value, mysql::Error> {
    use GeneratePolicy::*;
    match gen {
        Random => Ok(helpers::get_random_parser_val_from(&base_val)),
        Default(val) => Ok(helpers::get_default_parser_val_with(&base_val, &val)),
        Custom(f) => Ok(helpers::get_computed_parser_val_with(&base_val, &f)),
        ForeignKey(table_name) => generate_foreign_key_val(
            views, ghost_policies, 
            table_name, helpers::parser_val_to_u64(base_val),
            new_entities),
    }
}
