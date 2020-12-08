/* 
 * a single table's ghosts and their rptrs
 */
pub struct TableGhostEntities = {
    pub table: String, 
    pub gids: Vec<Value>,
    pub rptrs: RowPtrs,
};
/* 
 * a root ghost and the descendant ghosts 
 */
pub struct GhostFamily {
    pub root_table: String,
    pub root_gid: u64,
    pub family_members: Vec<TableGhostEntities>,
}
impl GhostFamily {
    pub fn ghost_family_to_db_string(eid: u64, gfam: &GhostFamily) -> String {
        let ghost_names = vec![];
        for gid in gfam.gids {
            ghost_names.push((family.table, gid));
        }
        let ghostdata = serde_json::to_string(&ghost_names).unwrap();
        format!("({}, {}, {}", eid, gfma.root_gid, ghostdata)
    }
}
/*
 * A variant of eid -> family of ghosts to store on-disk or serialize (no row pointers!)
 */
pub struct GhostEidMapping {
    table: String,
    eid2gidroot: Option<(u64, u64)>,
    ghosts: Vec<(String, u64),
}

/* 
 * a base true entity upon which to generate ghosts
 */
pub struct TemplateEntity {
    table: String,
    row: RowPtr,
    fixed_colvals: Option<Vec<(usize, Value)>>,
}

pub fn generate_new_ghosts_with_gids(
    views: &Views,
    ghost_policies: &EntityGhostPolicies,
    db: &mut mysql::Conn,
    template: &TemplateEntity, 
    gids: &Vec<Value>,
    nqueries: &mut usize,
) -> Result<Vec<TableGhostEntities>, mysql::Error>
{
    use GhostColumnPolicy::*;
    let start = time::Instant::now();
    let mut new_entities : Vec<TableGhostEntities> = vec![];
    let from_cols = views.get_view_columns(template.table);

    // NOTE : generating entities with foreign keys must also have ways to 
    // generate foreign key entity or this will panic
    let gp = ghost_policies.get(template.table).unwrap();
    warn!("Getting policies from {:?}, columns {:?}", gp, from_cols);
    let policies : Vec<GhostColumnPolicy> = from_cols.iter().map(|col| gp.get(&col.to_string()).unwrap().clone()).collect();
    let num_entities = gids.len();
    let mut new_vals : RowPtrs = vec![]; 
    for _ in 0..num_entities {
        new_vals.push(Rc::new(RefCell::new(vec![Value::Null; from_cols.len()]))); 
    }
    for (i, col) in from_cols.iter().enumerate() {
        let colname = col.to_string();
        // put in ID if specified
        if colname == ID_COL {
            for n in 0..num_entities {
                new_vals[n].borrow_mut()[i] = gids[n].clone();
            }
            continue;            
        }

        // set colval if specified
        if let Some(fixed) = template.fixed_colvals {
            for (ci, val) in fixed {
                if i == *ci {
                    for n in 0..num_entities {
                        new_vals[n].borrow_mut()[*ci] = val.clone();
                    } 
                    continue;
                }
            }
        }

        // otherwise, just follow policy
        let clone_val = &template.row.borrow()[i];
        warn!("Generating value using {:?} for {}", policies[i], col);
        match &policies[i] {
            CloneAll => {
                for n in 0..num_entities {
                    new_vals[n].borrow_mut()[i] = clone_val.clone();
                }
            }
            CloneOne(gen) => {
                // clone the value for the first row
                new_vals[0].borrow_mut()[i] = template.row.borrow()[i].clone();
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
    new_entities.push(GeneratedEntity{
        table: template.table.to_string(), 
        gids: gids.clone(),
        rptrs: new_vals,
    });
  
    // insert new rows into actual data tables 
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
        table_name: helpers::string_to_objname(template.table),
        columns : from_cols.clone(),
        source : source, 
    });
    warn!("new entities issue_insert_dt_stmt: {} dur {}", dt_stmt, start.elapsed().as_micros());
    db.query_drop(dt_stmt.to_string())?;
    *nqueries+=1;

    warn!("GHOSTS: adding {} new entities {:?} for table {}, dur {}", 
          num_entities, new_entities, template.table, start.elapsed().as_micros());
    Ok(new_entities)
}

pub fn generate_foreign_key_val(
    views: &views::Views,
    ghost_policies: &EntityGhostPolicies,
    db: &mut mysql::Conn,
    table_name: &str,
    new_entities: &mut Vec<TableGhostEntities>,
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
    let gidval = Value::Number(gid.to_string());

    warn!("GHOSTS: Generating foreign key entity for {} {:?}", table_name, random_row);
    new_entities.append(&mut generate_new_ghosts_with_gids(
        views, ghost_policies, db, 
        &TemplateEntity{
            table: table_name,
            row: random_row,
            fixed_colvals: None,
        },
        &vec![gidval.clone()],
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
    new_entities: &mut Vec<TableGhostEntities>,
    nqueries: &mut usize
) -> Result<Value, mysql::Error> {
    use GeneratePolicy::*;
    match gen {
        Random => Ok(helpers::get_random_parser_val_from(&base_val)),
        Default(val) => Ok(helpers::get_default_parser_val_with(&base_val, &val)),
        //Custom(f) => helpers::get_computed_parser_val_with(&base_val, &f),
        ForeignKey(table_name) => generate_foreign_key_value(views, ghost_policies, db, table_name, new_entities, nqueries),
    }
}
