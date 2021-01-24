use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::*;
use msql_srv::{QueryResultWriter};
use log::{warn};
use std::hash::{Hash, Hasher};

use crate::{policy, helpers, subscriber, query_simplifier, EntityData, ID_COL};
use crate::views::*;
use crate::ghosts::*;
use crate::policy::EdgePolicyType::{Delete, Retain, Decorrelate};
use crate::graph::*;

/* 
 * The controller issues queries to the database and materialized views.
 */
pub struct Querier {
    views: Views,
    policy: policy::ApplicationPolicy,
    pub subscriber: subscriber::Subscriber,
    
    // for tests
    params: super::TestParams,
    pub cur_stat: helpers::stats::QueryStat,
    pub stats: Vec<helpers::stats::QueryStat>,
}

#[derive(Clone, Debug)]
pub struct TraversedEntity {
    pub table_name: String,
    pub eid : u64,
    pub hrptr: HashedRowPtr,

    pub parent_table: String,
    pub parent_col_index: usize,
    pub from_pc_edge: bool,
}
impl Hash for TraversedEntity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.eid.hash(state);
    }
}
impl PartialEq for TraversedEntity {
    fn eq(&self, other: &TraversedEntity) -> bool {
        self.table_name == other.table_name && self.eid == other.eid
    }
}
impl Eq for TraversedEntity {} 

impl Querier {
    pub fn new(policy: policy::ApplicationPolicy, params: &super::TestParams) -> Self {
        Querier{
            views: Views::new(),
            policy: policy,
            subscriber: subscriber::Subscriber::new(),

            params: params.clone(),
            cur_stat: helpers::stats::QueryStat::new(),
            stats: vec![],
        }
    }   

    fn issue_statement (
            &mut self, 
            stmt: &Statement,
            db: &mut mysql::Conn) 
        -> Result<(Vec<TableColumnDef>, RowPtrs, Vec<usize>), mysql::Error>
    {
        warn!("issue statement: {}", stmt);
        let mut view_res : (Vec<TableColumnDef>, RowPtrs, Vec<usize>) = (vec![], vec![], vec![]);
        
        // TODO consistency?
        match stmt {
            Statement::Select(SelectStatement{query, ..}) => {
                view_res = self.views.query_iter(&query)?;
            }
            Statement::Insert(InsertStatement{
                table_name,
                columns, 
                source,
            }) => {
                warn!("Issuing {}", stmt);
                db.query_drop(stmt.to_string())?;
                self.cur_stat.nqueries+=1;

                let mut values = vec![];
                match source {
                    InsertSource::Query(q) => {
                        values = query_simplifier::insert_source_query_to_rptrs(&q, &self.views)?;
                    }
                    InsertSource::DefaultValues => (),
                }

                // insert into views
                self.views.insert(&table_name.to_string(), Some(&columns), &values)?;
            }
            Statement::Update(UpdateStatement{
                table_name,
                assignments,
                selection,
            }) => {
                let start = time::Instant::now();
                db.query_drop(stmt.to_string())?;
                self.cur_stat.nqueries+=1;
 
                // update views
                let mut assign_vals = vec![];
                // update all assignments to use only values
                for a in assignments {
                    assign_vals.push(query_simplifier::expr_to_value_expr(&a.value, &self.views)?);
                }
                self.views.update(&table_name.to_string(), &assignments, &selection, &assign_vals)?;
                warn!("update mvs: {}us", start.elapsed().as_micros());
            }
            Statement::Delete(DeleteStatement{
                table_name,
                selection,
            }) => {
                db.query_drop(stmt.to_string())?;
                self.cur_stat.nqueries+=1;
                self.views.delete(&table_name.to_string(), &selection)?;
            }
            Statement::CreateTable(CreateTableStatement{
                name,
                columns,
                constraints,
                indexes,
                with_options,
                if_not_exists,
                engine,
            }) => {
                let mut new_engine = engine.clone();
                if self.params.in_memory {
                    new_engine = Some(Engine::Memory);
                }

                let dtstmt = CreateTableStatement {
                    name: name.clone(),
                    columns: columns.clone(),
                    constraints: constraints.clone(),
                    indexes: indexes.clone(),
                    with_options: with_options.clone(),
                    if_not_exists: *if_not_exists,
                    engine: new_engine.clone(),
                };
                db.query_drop(dtstmt.to_string())?;
                self.cur_stat.nqueries+=1;

                // get parent columns so that we can keep track of the graph 
                let parent_cols_of_table = helpers::get_parent_col_indices_of_datatable(&self.policy, &name, columns);

                // create view for this table
                self.views.add_view(
                    name.to_string(), 
                    columns,
                    &indexes,
                    &constraints,
                    &parent_cols_of_table,
                );
            }
            Statement::DropObjects(DropObjectsStatement{
                object_type,
                names,
                ..
            }) => {
                match object_type {
                    ObjectType::Table => {
                        // alter the data table
                        db.query_drop(stmt.to_string())?;
                        self.cur_stat.nqueries+=1;

                        // remove view
                        self.views.remove_views(names);
                    }
                    _ => unimplemented!("Cannot drop object {}", stmt),
                }
            }
            _ => unimplemented!("stmt not supported: {}", stmt),
        }
        Ok(view_res)
    }

    pub fn record_query_stats(&mut self, qtype: helpers::stats::QueryType, dur: Duration) {
        self.cur_stat.nqueries += self.subscriber.get_nqueries();
        self.cur_stat.duration = dur;
        self.cur_stat.qtype = qtype;
        self.stats.push(self.cur_stat.clone());
        self.cur_stat.clear();
    }

    pub fn query<W: io::Write>(
        &mut self, 
        writer: QueryResultWriter<W>, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error>
    {
        let view_res = self.issue_statement(stmt, db)?;
        view_cols_rows_to_answer_rows(&view_res.0, view_res.1, &view_res.2, writer)
    }

    pub fn query_drop(
        &mut self, 
        stmt: &Statement, 
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        self.issue_statement(stmt, db)?; 
        Ok(())
    }

    /*******************************************************
     ****************** UNSUBSCRIPTION *********************
     *******************************************************/

    pub fn update_child_with_parent(&mut self, 
                                    child_table: &str,
                                    child_eid: u64,
                                    child_row: RowPtr,
                                    parent_col_index: usize,
                                    gid: u64, 
                                    db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        self.views.update_index_and_row_of_view(
                        child_table, child_row.clone(), parent_col_index, 
                        Some(&Value::Number(gid.to_string())));

        let parent_colname = self.views.get_view_colname(child_table, parent_col_index);
        let db_stmt = format!("UPDATE {} SET {} = {} WHERE {} = {}", 
                              child_table, 
                              parent_colname, 
                              gid,
                              ID_COL,
                              child_eid);
        db.query_drop(db_stmt)?;
        Ok(())
    }

    pub fn insert_ghosts_for_template(&mut self, 
                                template: &TemplateEntity,
                                is_pc: bool,
                                num_ghosts: usize,
                                db: &mut mysql::Conn) 
        -> Result<GhostEidMapping, mysql::Error> 
    {
        let start = time::Instant::now();
        let new_entities = match is_pc {
            true => generate_new_ghosts_from(&self.views, &self.policy.pc_ghost_policies, template, num_ghosts)?,
            false => generate_new_ghosts_from(&self.views, &self.policy.cp_ghost_policies, template, num_ghosts)?,
        };

        let mut ghost_names = vec![];
        let mut root_gids = vec![];
   
        for table_entities in new_entities {
            // insert new rows into actual data tables 
            let mut parser_rows = vec![];
            for row in &table_entities.rptrs {
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
                table_name: helpers::string_to_objname(&table_entities.table),
                columns : self.views.get_view_columns(&table_entities.table),
                source : source, 
            });
            warn!("insert_ghosts_for_template: {} dur {}", dt_stmt, start.elapsed().as_micros());
            db.query_drop(dt_stmt.to_string())?;
            self.cur_stat.nqueries+=1;
        
            self.views.insert(&table_entities.table, None, &table_entities.rptrs)?;
        
            for gid in table_entities.gids {
                if table_entities.table == template.table {
                    root_gids.push(gid);
                }
                ghost_names.push((table_entities.table.clone(), gid));
            }
        }

        let gem = GhostEidMapping{
            table: template.table.clone(),
            eid: template.eid,
            root_gids: root_gids.clone(),
            ghosts: ghost_names,
        };
        warn!("insert_ghost_for_eid gids {}, {:?}: {}us", template.eid, root_gids, start.elapsed().as_micros());
        Ok(gem)
    }

    pub fn unsubscribe<W: io::Write>(&mut self, uid: u64, db: &mut mysql::Conn, writer: QueryResultWriter<W>) 
        -> Result<(), mysql::Error> 
    {
        use policy::EdgePolicyType::*;

        warn!("UNSUB: Unsubscribing uid {}", uid);

        // table name of entity, eid, gids for eid
        let mut ghost_eid_mappings : Vec<GhostEidMapping> = vec![];

        // all entities to be replaced or removed, as they existed prior to unsubscription
        let mut nodes_to_remove : HashSet<EntityData> = HashSet::new();
       
        // all entities to be replaced by ghosted versions
        let mut nodes_to_ghost : HashSet<TraversedEntity> = HashSet::new();

        // track all parent-children edges, may have repeat children
        let mut traversed_entities: HashSet<TraversedEntity> = HashSet::new();

        // queue of children to look at next
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];

        // initialize with the entity specified by the uid
        let mut view_ptr = self.views.get_view(&self.policy.unsub_entity_type).unwrap();
        let matching_row = HashedRowPtr::new(view_ptr.borrow().get_row_of_id(uid), view_ptr.borrow().primary_index);
        let init_node = TraversedEntity{
                table_name: self.policy.unsub_entity_type.clone(),
                eid : uid,
                hrptr: matching_row.clone(),
                parent_table: "".to_string(),
                parent_col_index: 0,
                from_pc_edge: true,
        };

        // we will eventually remove this node
        nodes_to_remove.insert(subscriber::traversed_entity_to_entitydata(&init_node));

        children_to_traverse.push(init_node);
        
        /* 
         * Step 1: TRAVERSAL + DECORRELATION
         * TODO could parallelize to reduce time to traverse?
         */
        while children_to_traverse.len() > 0 {
            let start = time::Instant::now();

            let node = children_to_traverse.pop().unwrap();
            let mut children_to_decorrelate = vec![];
            let mut children_to_retain = vec![];
            let mut children_to_delete = vec![];
                                   
            // add entity to seen 
            traversed_entities.insert(node.clone()); 

            // get children of this node
            let children : EntityTypeRows;
            match self.views.graph.get_children_of_parent(&node.table_name, node.eid) {
                None => {
                    // this is a leaf node, we want to ghost it 
                    nodes_to_ghost.insert(node);
                    continue;
                }
                Some(cs) => children = cs,
            }
            warn!("UNSUB {} STEP 1: Traversing {:?}, found children {:?}", uid, node, children);

            // TODO make a pointer so we don't have to clone
            for ((child_table, child_ci), child_hrptrs) in children.iter() {
                let child_table_epolicies = self.policy.edge_policies.get(child_table).unwrap().clone();
                view_ptr = self.views.get_view(&child_table).unwrap();
                
                // first, check if anypolicy is decorrelate, so we can create these parent entities
                // all at once to satisfy CloneOne policies
                for policy in &*child_table_epolicies {
                    let ci = helpers::get_col_index(&policy.column, &view_ptr.borrow().columns).unwrap();
                    
                    // skip any policies for edges not to this parent table type
                    if ci != *child_ci ||  policy.parent != node.table_name {
                        continue;
                    }

                    for child_hrptr in child_hrptrs {
                        let child = TraversedEntity {
                            table_name: child_table.clone(),
                            eid: helpers::parser_val_to_u64(&child_hrptr.row().borrow()[view_ptr.borrow().primary_index]),
                            hrptr: child_hrptr.clone(),
                            parent_table: node.table_name.clone(), 
                            parent_col_index: ci,
                            from_pc_edge: true,
                        };
                        
                        let child_nodedata = subscriber::traversed_entity_to_entitydata(&child);

                        match policy.pc_policy {
                            Decorrelate(f) => { 
                                assert!(f < 1.0); 
                                children_to_decorrelate.push(child.clone());

                                // if child hasn't been seen and hasn't been ghosted, traverse
                                // child would only be ghosted if it itself had children that had
                                // to be decorrelated---if it's a ghost, this means that a prior
                                // unsubscription already took care of it!
                                if traversed_entities.get(&child).is_none() && !is_ghost_eid(child.eid) {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    nodes_to_remove.insert(child_nodedata.clone());
                                    children_to_traverse.push(child);
                                }
                            }
                            Delete(f) => {
                                assert!(f < 1.0); 
                                // add all the sensitive removed entities to return to the user 
                                self.get_tree_from_child(&child_nodedata, &mut nodes_to_remove);
                                
                                // don't add child to traversal queue
                                children_to_delete.push(child);
                            }
                            Retain => {
                                children_to_retain.push(child.clone());

                                // if child hasn't been seen yet, traverse
                                if traversed_entities.get(&child).is_none() && !is_ghost_eid(child.eid) {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    nodes_to_remove.insert(child_nodedata.clone());
                                    children_to_traverse.push(child);
                                }
                            }
                        }
                    }
                }
            }

            // create and rewrite ghosts for all children that need to be decorrelated
            // create all these ghosts at once
            if !children_to_decorrelate.is_empty() {
                let gem = self.insert_ghosts_for_template(
                    &TemplateEntity {
                        table: node.table_name.clone(),
                        eid: node.eid,
                        row: node.hrptr.row().clone(), 
                        fixed_colvals: None,
                    }, true, children_to_decorrelate.len(), db)?;
                // generate all ghost parents
                let gids = gem.root_gids.clone();
                ghost_eid_mappings.push(gem); 

                let mut gid_index = 0;
                for child in &children_to_decorrelate {
                    self.update_child_with_parent(&child.table_name, child.eid, child.hrptr.row().clone(), child.parent_col_index, gids[gid_index], db)?;
                    gid_index += 1;
                }

                // any retained children now point to the first generated ghost
                // this means that retained edges will point to the clone if there is a CloneOne
                // policy
                for child in &children_to_retain {
                    self.update_child_with_parent(&child.table_name, child.eid, child.hrptr.row().clone(), child.parent_col_index, gids[0], db)?;
                }
                
            } else {

                // replace this node with a ghost node because we haven't yet in decorrelation
                nodes_to_ghost.insert(node.clone());

            }
            warn!("UNSUB {} STEP 1: Duration to traverse+decorrelate {}, {:?}: {}us", 
                      uid, node.table_name.clone(), node, start.elapsed().as_micros());
        }
        
        /* 
         * Step 2: Child->Parent Decorrelation. 
         * For all edges to the parent entity that need to reach a particular sensitivity
         * threshold, decorrelate or remove the children; if retained, ghost the parent. 
         */
        self.unsubscribe_child_parent_edges(
            &traversed_entities, 
            &mut ghost_eid_mappings, 
            &mut nodes_to_remove, 
            db,
        )?;

        /*
         * Step 3: Change intermediate and leaf entities to ghosts 
         *  TODO optimize policies where all values are cloned
         */
        for entity in nodes_to_ghost {
            // this was already ghosted in a prior unsubscription
            if is_ghost_eid(entity.eid) {
                continue;
            }
            warn!("UNSUB {} STEP 3: Changing {:?} to ghost", uid, entity);
            // create ghost for this entity
            let gem = self.insert_ghosts_for_template(
                &TemplateEntity {
                    table: entity.table_name.clone(),
                    eid: entity.eid,
                    row: entity.hrptr.row().clone(), 
                    fixed_colvals: None,
                }, entity.from_pc_edge, 1, db)?;
            
            let gids = gem.root_gids.clone();
            ghost_eid_mappings.push(gem);
           
            // update children to point to the new ghost
            if let Some(children) = self.views.graph.get_children_of_parent(&entity.table_name, entity.eid) {
                warn!("Found children {:?} of {:?}", children, entity);
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    for hrptr in child_hrptrs {
                        assert!(hrptr.row().borrow()[*child_ci].to_string() == entity.eid.to_string());
                        self.update_child_with_parent(&child_table, entity.eid, hrptr.row().clone(), *child_ci, gids[0], db)?;
                    }
                }
            }
        }

        /*
         * Remove nodes from database and materialized views
         */
        self.remove_entities(&nodes_to_remove, db)?;

        /*
         * Step 5: Return data to user
         */
        self.subscriber.record_unsubbed_user_and_return_results(writer, uid, &mut ghost_eid_mappings, &mut nodes_to_remove, db)
    }

    pub fn unsubscribe_child_parent_edges(&mut self, 
        children: &HashSet<TraversedEntity>, 
        ghost_eid_mappings: &mut Vec<GhostEidMapping>,
        nodes_to_remove: &mut HashSet<EntityData>,
        db: &mut mysql::Conn) 
        -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();

        // for every parent edge from each seen child
        let mut tables_to_children : HashMap<String, Vec<&TraversedEntity>> = HashMap::new();
        for child in children.iter() {
            if let Some(cs) = tables_to_children.get_mut(&child.table_name) {
                cs.push(child);
            } else {
                tables_to_children.insert(child.table_name.clone(), vec![child]);
            }
        }
        
        for (table_name, table_children) in tables_to_children.iter() {
            let edge_policies = self.policy.edge_policies.get(table_name).unwrap().clone();
            let poster_child = table_children[0];
            let child_columns = self.views.get_view_columns(&table_name);
    
            for policy in &*edge_policies {
                let ci = child_columns.iter().position(|c| policy.column == c.to_string()).unwrap();
     
                // group all table children by EID
                let mut parent_eid_counts : HashMap<u64, Vec<&TraversedEntity>> = HashMap::new();
                for child in table_children {
                    
                    // make sure that we don't accidentally decorrelate or delete edges from the parent
                    // that led to this child
                    if child.parent_col_index == ci {
                        continue;
                    }

                    let parent_eid_val = &child.hrptr.row().borrow()[ci];
                    let parent_eid = helpers::parser_val_to_u64(parent_eid_val);
                    if let Some(count) = parent_eid_counts.get_mut(&parent_eid) {
                        count.push(child);
                    } else {
                        parent_eid_counts.insert(parent_eid, vec![child]);
                    }
                }

                let sensitivity : f64;
                match policy.cp_policy {
                    Decorrelate(f) => sensitivity = f,
                    Delete(f) => sensitivity = f,
                    Retain => sensitivity = 1.0,
                }

                // if we're retaining all edges, we don't do anything (we don't ghost parents in
                // the child->parent direction)
                if sensitivity == 1.0 {
                    continue;
                }

                // otherwise, decorrelate/delete as necessary
                for (parent_eid, sensitive_children) in parent_eid_counts.iter() {
                    // get all children of this type with the same parent entity eid
                    let childrows = self.views.graph.get_children_of_parent(&policy.parent, *parent_eid).unwrap();
                    let all_children = childrows.get(&(table_name.clone(), ci)).unwrap();
                    let total_count = all_children.len();
                    let sensitive_count = sensitive_children.len();
                    warn!("UNSUB STEP 2: Found {} total and {} sensitive children of type {} of parent {}.{}", 
                          total_count, sensitive_count, poster_child.table_name, policy.parent, parent_eid);
                    
                    let number_to_desensitize = (((sensitive_count as f64 * (1.0-sensitivity)) 
                                                  - (total_count  as f64* sensitivity)) 
                                                 / (1.0-sensitivity)).ceil() as i64;
                    if number_to_desensitize > 0 {
                        let parent_rptr = self.views.get_row_of_id(&policy.parent, *parent_eid);
                        
                        if number_to_desensitize > sensitive_count as i64 {
                            unimplemented!("No support for decorrelated or removing non-sensitive children");
                        }
                
                        match policy.cp_policy {
                            Decorrelate(f) => { 
                                // generate all ghost parents
                                assert!(f < 1.0); 
                               
                                /* 
                                 * We ignore the first parent (which might be a clone) because
                                 * we're actually retaining the original parent entity
                                 *
                                 * NOTE 1: this means that we can actually decorrelate each type of
                                 * child entity SEPARATELY, rather than having to do all children
                                 * together, as we do in the PC direction 
                                 *
                                 * NOTE 2: this means that we generate an extra ghost because
                                 * CloneOne policies no longer make a difference here
                                 */
                                let number_of_ghosts = (number_to_desensitize+1) as usize;
                                               
                                // create all these ghosts at once
                                let gem = self.insert_ghosts_for_template(
                                    &TemplateEntity {
                                        table: policy.parent.clone(),
                                        eid: *parent_eid,
                                        row: parent_rptr.clone(), 
                                        fixed_colvals: None,
                                    }, false, number_of_ghosts, db)?;
                               
                                let gids = gem.root_gids.clone();
                                ghost_eid_mappings.push(gem); 

                                let mut gid_index = 1;
                                for child in &sensitive_children[0..number_to_desensitize as usize] {
                                    self.update_child_with_parent(&poster_child.table_name, child.eid, child.hrptr.row().clone(), ci, gids[gid_index], db)?;
                                    gid_index += 1;
                                }
                            }
                            Delete(_) => {
                                // add all the sensitive removed entities to return to the user 
                                for child in &sensitive_children[0..number_to_desensitize as usize] {
                                    let child_nodedata = subscriber::traversed_entity_to_entitydata(&child);
                                    self.get_tree_from_child(&child_nodedata, nodes_to_remove);
                                }
                            }
                            _ => unimplemented!("Shouldn't have a retain policy with a positive number to desensitize")
                        }
                    }
                }
            }
        }
        warn!("UNSUB STEP 2: Duration {}us", start.elapsed().as_micros());
        Ok(())
    }

    pub fn get_tree_from_child(&mut self, child: &EntityData, nodes: &mut HashSet<EntityData>)
    {
        let mut children_to_traverse: Vec<EntityData> = vec![];
        children_to_traverse.push(child.clone());
        let mut node: EntityData;

        while children_to_traverse.len() > 0 {
            node = children_to_traverse.pop().unwrap().clone();

            // see if any entity has a foreign key to this one; we'll need to remove those too
            // NOTE: because traversal was parent->child, all potential children down the line
            // SHOULD already been in seen_children
            if let Some(children) = self.views.graph.get_children_of_parent(&node.table, node.eid) {
                for ((child_table, _child_ci), child_hrptrs) in children.iter() {
                    let view_ptr = self.views.get_view(&child_table).unwrap();
                    for hrptr in child_hrptrs {
                        children_to_traverse.push(EntityData {
                            table: child_table.clone(),
                            eid: helpers::parser_val_to_u64(&hrptr.row().borrow()[view_ptr.borrow().primary_index]),
                            row_strs: subscriber::hrptr_to_strs(&hrptr),
                        });
                    }
                }
            }
            nodes.insert(node);
        }
    }

    fn remove_entities(&mut self, nodes: &HashSet<EntityData>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        let id_col = Expr::Identifier(helpers::string_to_idents(ID_COL));
        let mut table_to_nodes : HashMap<String, Vec<u64>> = HashMap::new();

        for node in nodes {
            match table_to_nodes.get_mut(&node.table) {
                Some(ids) => ids.push(node.eid),
                None => {
                    table_to_nodes.insert(node.table.clone(), vec![node.eid]);
                }
            }
        }

        for (table, ids) in table_to_nodes.iter() {
            let eid_exprs : Vec<Expr> = ids.iter().map(|id| Expr::Value(Value::Number(id.to_string()))).collect();
            let selection = Some(Expr::InList{
                    expr: Box::new(id_col.clone()),
                    list: eid_exprs,
                    negated: false,
            });
         
            warn!("UNSUB STEP 4: deleting {:?} {:?}", table, ids);
            self.views.delete_rptrs_with_ids(&table, &ids)?;

            let delete_eid_from_table = Statement::Delete(DeleteStatement {
                table_name: helpers::string_to_objname(&table),
                selection: selection.clone(),
            });
            warn!("UNSUB STEP 4 delete: {}", delete_eid_from_table);
            db.query_drop(format!("{}", delete_eid_from_table.to_string()))?;
            self.cur_stat.nqueries+=1;
        }
        Ok(())
    }

    /*******************************************************
     ****************** RESUBSCRIPTION *********************
     *******************************************************/
    /* 
     * Set all user_ids in the ghosts table to specified user 
     * Refresh "materialized views"
     * TODO add back deleted content from shard
     */
    pub fn resubscribe(&mut self, uid: u64, ghost_eid_mappings: &Vec<GhostEidMapping>, entity_data: &Vec<EntityData>, db: &mut mysql::Conn) -> 
        Result<(), mysql::Error> {
        // TODO check auth token?
         warn!("Resubscribing uid {}", uid);
      
        let mut ghost_eid_mappings = ghost_eid_mappings.clone();
        let mut entity_data = entity_data.clone();
        self.subscriber.check_and_sort_resubscribed_data(uid, &mut ghost_eid_mappings, &mut entity_data, db)?;

        /*
         * Add resubscribing data to data tables + MVs 
         */
        // parse entity data into tables -> data
        let mut curtable = entity_data[0].table.clone();
        let mut curvals = vec![];
        for entity in entity_data {
            // do all the work for this table at once!
            if !(curtable == entity.table) {
                self.reinsert_entities(&curtable, &curvals, db)?;
                
                // reset 
                curtable = entity.table.clone();
                curvals = vec![entity.row_strs.clone()];
            } else {
                curvals.push(entity.row_strs.clone()); 
            }
        }
        self.reinsert_entities(&curtable, &curvals, db)?;

        // parse gids into table eids -> set of gids
        let mut table = ghost_eid_mappings[0].table.clone();
        let mut eid = ghost_eid_mappings[0].eid;
        let mut root_gids = ghost_eid_mappings[0].root_gids.clone();
        let mut ghosts : Vec<Vec<(String, u64)>> = vec![];
        for mapping in ghost_eid_mappings {
            // do all the work for this eid at once!
            if !(table == mapping.table && eid == mapping.eid) {
                self.replace_ghosts(&table, eid, root_gids, &ghosts, db)?;

                // reset 
                eid = mapping.eid.clone();
                root_gids = mapping.root_gids.clone();
                table = mapping.table.clone();
                ghosts = vec![mapping.ghosts.clone()];
            } else {
                ghosts.push(mapping.ghosts.clone());
            }
        }
        self.replace_ghosts(&table, eid, root_gids, &ghosts, db)?;

        Ok(())
    }

    fn reinsert_entities(&mut self, curtable: &str, curvals: &Vec<Vec<String>>, db: &mut mysql::Conn) 
    -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();
        let viewptr = &self.views.get_view(curtable).unwrap();
        warn!("{}: Reinserting values {:?}", curtable, curvals);
        let mut rowptrs = vec![];
        let mut bodyvals = vec![];
        for row in curvals {
            let vals = helpers::string_vals_to_parser_vals(row, &viewptr.borrow().columns);
            rowptrs.push(Rc::new(RefCell::new(vals.clone())));
            bodyvals.push(vals.iter().map(|v| Expr::Value(v.clone())).collect());
        }

        self.views.insert(curtable, None, &rowptrs)?;
        warn!("RESUB insert into view {:?} took {}us", rowptrs, start.elapsed().as_micros());

        let insert_entities_stmt = Statement::Insert(InsertStatement{
            table_name: helpers::string_to_objname(&curtable),
            columns: self.views.get_view_columns(&curtable),
            source: InsertSource::Query(Box::new(Query{
                ctes: vec![],
                body: SetExpr::Values(Values(bodyvals)),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            })),
        });

        warn!("RESUB issuing {}", insert_entities_stmt);
        db.query_drop(format!("{}", insert_entities_stmt))?;
        self.cur_stat.nqueries+=1;
       
        warn!("RESUB db {} finish reinsert took {}us", insert_entities_stmt.to_string(), start.elapsed().as_micros());
        Ok(())
    }
 

    fn replace_ghosts(&mut self, curtable: &str, eid: u64, root_gids: Vec<u64>, ghosts: &Vec<Vec<(String, u64)>>, db: &mut mysql::Conn) -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();
        
        // maps from tables to the gid/rptrs of ghost entities from that table
        let mut table_to_gid_rptrs: HashMap<String, Vec<(u64, RowPtr)>> = HashMap::new();
        for ancestor_group in ghosts {
            for (ancestor_table, ancestor_gid) in ancestor_group {
                // get rptr for this ancestor
                let view_ptr = self.views.get_view(&ancestor_table).unwrap();
                let ancestor_rptr = view_ptr.borrow().get_row_of_id(*ancestor_gid);

                // add to rptr to list to delete
                if let Some(gidrptrs) = table_to_gid_rptrs.get_mut(ancestor_table) {
                    gidrptrs.push((*ancestor_gid, ancestor_rptr.clone()));
                } else {
                    table_to_gid_rptrs.insert(
                        ancestor_table.to_string(), 
                        vec![(*ancestor_gid, ancestor_rptr.clone())]);
                }
            }
        }
        // we need to update child in the MV to now show the EID
        // Note: all ghosts in families will be deleted from the MV, so we only need to restore
        // the EID value for the root level GID entries
        for root_gid in root_gids {
            if let Some(children) = self.views.graph.get_children_of_parent(curtable, root_gid) {
                warn!("Get children of table {} GID {}: {:?}", curtable, root_gid, children);
                // for each child row
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    let child_viewptr = self.views.get_view(&child_table).unwrap();
                    let ghost_parent_keys = helpers::get_ghost_parent_key_indices_of_datatable(
                        &self.policy, &child_table, &child_viewptr.borrow().columns);
                    // if the child has a column that is ghosted and the ghost ID matches this gid
                    for (ci, parent_table) in &ghost_parent_keys {
                        if ci == child_ci && parent_table == &curtable {
                            for hrptr in child_hrptrs {
                                if hrptr.row().borrow()[*ci].to_string() == root_gid.to_string() {
                                    // then update this child to use the actual real EID
                                    warn!("Updating child row with GID {} to point to actual eid {}", root_gid, eid);
                                    self.update_child_with_parent(&child_table, root_gid, hrptr.row().clone(), *ci, eid, db)?;
                                }
                            }
                        }
                    }
                }
            }
        }

        // delete all ghosts from from MV
        for (table, gidrptrs) in table_to_gid_rptrs.iter() {
            self.views.delete_rptrs(&table, &gidrptrs.iter().map(|(_, rptr)| rptr.clone()).collect())?;
            warn!("RESUB: remove {} ghost entities from view {} took {}us", gidrptrs.len(), table, start.elapsed().as_micros());
        }

        // delete from actual data table if was created during unsub
        // this includes any recursively created parents
        for (table, gidrptrs) in table_to_gid_rptrs.iter() {
            let select_ghosts = Expr::InList{
                expr: Box::new(Expr::Identifier(helpers::string_to_idents(ID_COL))),
                list: gidrptrs.iter().map(|(gid, _)| Expr::Value(Value::Number(gid.to_string()))).collect(),
                negated: false,
            };
            let delete_gids_as_entities = Statement::Delete(DeleteStatement {
                table_name: helpers::string_to_objname(&table),
                selection: Some(select_ghosts),
            });
            warn!("RESUB removing entities: {}", delete_gids_as_entities);
            db.query_drop(format!("{}", delete_gids_as_entities.to_string()))?;
            self.cur_stat.nqueries+=1;
        }
        Ok(())
    }

    pub fn rebuild_view_with_all_rows(&mut self,
        name: &str,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        indexes: Vec<IndexDef>,
        db: &mut mysql::Conn) 
    {
        let objname = helpers::string_to_objname(name);
        // get parent columns so that we can keep track of the graph 
        let parent_cols_of_table = helpers::get_parent_col_indices_of_datatable(&self.policy, &objname, &columns);
        
        // create view for this table
        self.views.add_view(
            name.to_string(), 
            &columns,
            &indexes,
            &constraints,
            &parent_cols_of_table,
        );
        let viewptr = self.views.get_view(name).unwrap();
        
        // 1. get all rows of this table
        let get_all_rows_query = Query::select(Select{
            distinct: true,
            projection: vec![SelectItem::Wildcard],
            from: vec![TableWithJoins{
                relation: TableFactor::Table{
                    name: objname,
                    alias: None,
                },
                joins: vec![],
            }],
            selection: None,
            group_by: vec![],
            having: None,
        });
        let rows = db.query_iter(get_all_rows_query.to_string()).unwrap();
        
        // 2. convert rows to appropriate rowptrs
        let mut rptrs : RowPtrs = vec![];
        for row in rows {
            let vals = row.unwrap().unwrap();
            let parsed_row = helpers::string_vals_to_parser_vals(
                &vals.iter().map(|v| helpers::mysql_val_to_string(v)).collect(), 
                &viewptr.borrow().columns);
            rptrs.push(Rc::new(RefCell::new(parsed_row)));    
        }
        // 3. insert all rows 
        warn!("Rebuilding view {} with all rows {:?}", name, rptrs);
        self.views.insert(name, None, &rptrs).unwrap();
    }
}
