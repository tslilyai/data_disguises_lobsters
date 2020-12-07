use std::*;
use rand::prelude::*;
use mysql::prelude::*;
use sql_parser::ast::*;
use crate::{helpers, ghosts_map, views, ID_COL};
use crate::views::{Views, RowPtrs, RowPtr};
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use log::{debug, warn, error};

pub type ColumnName = String; // column name
pub type EntityName = String; // table name, or foreign key

#[derive(Clone, Debug, PartialEq)]
pub enum GeneratePolicy {
    Random,
    Default(String),
    //Custom(Box<dyn Fn(&str) -> String>), // column value -> column value
    ForeignKey(EntityName),
}
#[derive(Clone, Debug, PartialEq)]
pub enum GhostColumnPolicy {
    CloneAll,
    CloneOne(GeneratePolicy),
    Generate(GeneratePolicy),
}
pub type GhostPolicy = HashMap<ColumnName, GhostColumnPolicy>;
pub type EntityGhostPolicies = HashMap<EntityName, GhostPolicy>;

#[derive(Clone, Debug, PartialEq)]
pub enum DecorrelationPolicy {
    NoDecorRemove,
    NoDecorRetain,
    NoDecorSensitivity(f64),
    Decor,
}
#[derive(Clone, Debug)]
pub struct KeyRelationship {
    pub child: EntityName,
    pub parent: EntityName,
    pub column_name: ColumnName,
    pub parent_child_decorrelation_policy: DecorrelationPolicy,
    pub child_parent_decorrelation_policy: DecorrelationPolicy,
}

pub struct ApplicationPolicy {
    pub entity_type_to_decorrelate: EntityName,
    pub ghost_policies: EntityGhostPolicies, 
    pub edge_policies: Vec<KeyRelationship>,
}

pub struct Config {
    pub entity_type_to_decorrelate: String,

    // table and which columns(+parent type) that correspond to ghosts (edges that are decorrelated and store GIDs)
    // created when parent->child edges are decorrelated
    pub parent_child_ghosted_tables: HashMap<String, Vec<(String, String)>>,
    // table and which columns(+parent type) for which sensitivity to this parent should fall below specified threshold
    pub parent_child_sensitive_tables: HashMap<String, Vec<(String, String, f64)>>,
  
    // table and which columns(+parent type) that correspond to ghosts (edges that are decorrelated and store
    // GIDs), created when child->parent edges are decorrelated
    // NOTE: this is usually a subset of parent_child_ghosted_tables: an edge type that is
    // decorrelated when a child is sensitive should also be decorrelated when both the parent and
    // child are sensitive
    pub child_parent_ghosted_tables: HashMap<String, Vec<(String, String)>>,
    // table and which columns(+parent type) for which sensitivity to this parent should fall below specified threshold
    pub child_parent_sensitive_tables: HashMap<String, Vec<(String, String, f64)>>,

    // tables that can have parent->child edges
    pub tables_with_children: HashSet<String>,
    // is this child going to be left parent-less?
    pub completely_decorrelated_children: HashSet<String>,
    // is this parent going to be left edge-less?
    pub completely_decorrelated_parents: HashSet<String>,
}

pub fn policy_to_config(policy: &ApplicationPolicy) -> Config {
    let mut pc_gdts: HashMap<String, Vec<(String, String)>>= HashMap::new();
    let mut pc_sdts: HashMap<String, Vec<(String, String, f64)>>= HashMap::new();

    let mut cp_gdts: HashMap<String, Vec<(String, String)>>= HashMap::new();
    let mut cp_sdts: HashMap<String, Vec<(String, String, f64)>>= HashMap::new();
    
    let mut tables_with_children: HashSet<String> = HashSet::new();
     
    let mut attached_parents: HashSet<String> = HashSet::new();
    let mut attached_children: HashSet<String> = HashSet::new();
    let mut completely_decorrelated_parents: HashSet<String> = HashSet::new();
    let mut completely_decorrelated_children: HashSet<String> = HashSet::new();

    for kr in &policy.edge_policies {
        tables_with_children.insert(kr.parent.clone());
        completely_decorrelated_parents.insert(kr.parent.clone());
        completely_decorrelated_children.insert(kr.child.clone());
        
        match kr.parent_child_decorrelation_policy {
            DecorrelationPolicy::Decor => {
                if let Some(ghost_cols) = pc_gdts.get_mut(&kr.child) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone()));
                } else {
                    pc_gdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone())]);
                }
            } 
            DecorrelationPolicy::NoDecorRemove => {
                if let Some(ghost_cols) = pc_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 0.0));
                } else {
                    pc_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 0.0)]);
                }        
            } 
            DecorrelationPolicy::NoDecorSensitivity(s) => {
                attached_parents.insert(kr.parent.clone());
                if let Some(ghost_cols) = pc_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), s));
                } else {
                    pc_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), s)]);
                }        
            }
            DecorrelationPolicy::NoDecorRetain => {
                attached_parents.insert(kr.parent.clone());
                if let Some(ghost_cols) = pc_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 1.0));
                } else {
                    pc_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 1.0)]);
                }
            } 
        }
        match kr.child_parent_decorrelation_policy {
            DecorrelationPolicy::Decor => {
                if let Some(ghost_cols) = cp_gdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone()));
                } else {
                    cp_gdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone())]);
                }
            } 
            DecorrelationPolicy::NoDecorRemove => {
                if let Some(ghost_cols) = cp_sdts.get_mut(&kr.child.clone()) { ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 0.0));
                } else {
                    cp_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 0.0)]);
                }        
            } 
            DecorrelationPolicy::NoDecorSensitivity(s) => {
                attached_children.insert(kr.child.clone());
                if let Some(ghost_cols) = cp_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), s));
                } else {
                    cp_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), s)]);
                }        
            }
            DecorrelationPolicy::NoDecorRetain => {
                attached_children.insert(kr.child.clone());
                if let Some(ghost_cols) = cp_sdts.get_mut(&kr.child.clone()) {
                    ghost_cols.push((kr.column_name.clone(), kr.parent.clone(), 1.0));
                } else {
                    cp_sdts.insert(kr.child.clone(), vec![(kr.column_name.clone(), kr.parent.clone(), 1.0)]);
                }
            } 
        }
    }
 
    for child in attached_children {
        completely_decorrelated_children.remove(&child);
    }
    for parent in attached_parents {
        completely_decorrelated_parents.remove(&parent);
    }
    Config {
        entity_type_to_decorrelate: policy.entity_type_to_decorrelate.clone(), 
        parent_child_ghosted_tables: pc_gdts,
        parent_child_sensitive_tables: pc_sdts,

        child_parent_ghosted_tables: cp_gdts,
        child_parent_sensitive_tables: cp_sdts,
        
        tables_with_children: tables_with_children,
        completely_decorrelated_parents: completely_decorrelated_parents,
        completely_decorrelated_children: completely_decorrelated_children,
    }
}

pub type GeneratedEntities = Vec<(String, RowPtrs)>;

pub fn generate_new_entities_from(
    views: &Views,
    ghost_policies: &EntityGhostPolicies,
    db: &mut mysql::Conn,
    from_table: &str,
    from_vals: RowPtr, 
    eids: &Vec<Value>,
    set_colval: Option<(usize, Value)>,
    nqueries: &mut usize,
) 
    -> Result<GeneratedEntities, mysql::Error>
{
    use GhostColumnPolicy::*;
    let start = time::Instant::now();
    let mut new_entities : GeneratedEntities = vec![];
    let from_cols = views.get_view_columns(from_table);

    // NOTE : generating entities with foreign keys must also have ways to 
    // generate foreign key entity or this will panic
    let gp = ghost_policies.get(from_table).unwrap();
    warn!("Getting policies from {:?}, columns {:?}", gp, from_cols);
    let policies : Vec<GhostColumnPolicy> = from_cols.iter().map(|col| gp.get(&col.to_string()).unwrap().clone()).collect();
    let num_entities = eids.len();
    let mut new_vals : RowPtrs = vec![]; 
    for _ in 0..num_entities {
        new_vals.push(Rc::new(RefCell::new(vec![Value::Null; from_cols.len()]))); 
    }
    for (i, col) in from_cols.iter().enumerate() {
        let colname = col.to_string();
        // put in ID if specified
        if colname == ID_COL {
            for n in 0..num_entities {
                new_vals[n].borrow_mut()[i] = eids[n].clone();
            }
            continue;            
        }

        // set colval if specified
        if let Some((ci, val)) = &set_colval {
            if i == *ci {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[*ci] = val.clone();
                } 
                continue;
            }
        }

        // otherwise, just follow policy
        let clone_val = &from_vals.borrow()[i];
        warn!("Generating value using {:?} for {}", policies[i], col);
        match &policies[i] {
            CloneAll => {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[i] = clone_val.clone();
                }
            }
            CloneOne(gen) => {
                // clone the value for the first row
                new_vals[0].borrow_mut()[i] = from_vals.borrow()[i].clone();
                for n in 1..num_entities {
                    new_vals[n].borrow_mut()[i] = get_generated_val(views, ghost_policies, db, &gen, clone_val, &mut new_entities, nqueries)?;
                }
            }
            Generate(gen) => {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[i] = get_generated_val(views, ghost_policies, db, &gen, clone_val, &mut new_entities, nqueries)?;
                }
            }
        }
    }
    new_entities.push((from_table.to_string(), new_vals.clone())); 
  
    // insert new rows into actual data tables (do we really need to do this?)
    let mut parser_rows = vec![];
    for row in new_vals {
        let parser_row = row.borrow().iter()
            .map(|v| Expr::Value(v.clone()))
            .collect();
        parser_rows.push(parser_row);
    }
    let source = InsertSource::Query(Box::new(Query{
        ctes: vec![],
        body: SetExpr::Values(Values(parser_rows)),
        order_by: vec![],
        limit: None,
        fetch: None,
        offset: None,
    }));
    let dt_stmt = Statement::Insert(InsertStatement{
        table_name: helpers::string_to_objname(from_table),
        columns : from_cols.clone(),
        source : source, 
    });
    warn!("new entities issue_insert_dt_stmt: {} dur {}", dt_stmt, start.elapsed().as_micros());
    db.query_drop(dt_stmt.to_string())?;
    *nqueries+=1;

    warn!("UNSUB Done adding {} new entities {:?} for table {}, dur {}", 
          num_entities, new_entities, from_table, start.elapsed().as_micros());
    Ok(new_entities)
}

pub fn generate_foreign_key_value(
    views: &views::Views,
    ghost_policies: &EntityGhostPolicies,
    db: &mut mysql::Conn,
    table_name: &str,
    new_entities: &mut GeneratedEntities,
    nqueries: &mut usize) 
    -> Result<Value, mysql::Error> 
{
    let viewcols= views.get_view_columns(table_name);
    let viewptr = views.get_view(table_name).unwrap();
    
    // assumes there is at least once value here...
    let random_row : RowPtr;
    if viewptr.borrow().rows.borrow().len() > 0 {
        random_row = viewptr.borrow().rows.borrow().iter().next().unwrap().1.clone();
    } else {
        random_row = Rc::new(RefCell::new(vec![Value::Null; viewcols.len()]));
    }
    let mut rng: ThreadRng = rand::thread_rng();
    let gid = rng.gen_range(ghosts_map::GHOST_ID_START, ghosts_map::GHOST_ID_MAX);
    let gidval= Value::Number(gid.to_string());

    warn!("Generating foreign key entity for {} {:?}", table_name, random_row);
    new_entities.append(&mut generate_new_entities_from(
        views,
        ghost_policies,
        db, 
        &table_name,
        random_row,
        &vec![gidval.clone()],
        None,
        nqueries,
    )?);
    Ok(gidval)
}

pub fn get_generated_val(
    views: &views::Views,
    ghost_policies: &EntityGhostPolicies,
    db: &mut mysql::Conn,
    gen: &GeneratePolicy, 
    base_val: &Value,
    new_entities: &mut GeneratedEntities,
    nqueries: &mut usize
    ) 
-> Result<Value, mysql::Error> 
{
    use GeneratePolicy::*;
    match gen {
        Random => Ok(helpers::get_random_parser_val_from(&base_val)),
        Default(val) => Ok(helpers::get_default_parser_val_with(&base_val, &val)),
        //Custom(f) => helpers::get_computed_parser_val_with(&base_val, &f),
        ForeignKey(table_name) => generate_foreign_key_value(views, ghost_policies, db, table_name, new_entities, nqueries),
    }
}


