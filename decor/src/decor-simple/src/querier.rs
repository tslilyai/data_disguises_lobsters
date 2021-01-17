use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::*;
use msql_srv::{QueryResultWriter};
use log::{warn};

use crate::{policy, helpers, subscriber, query_simplifier, EntityData, ID_COL};
use crate::views::*;
use crate::ghosts::*;
use crate::policy::UnsubscribePolicy::{Delete, Retain, Decorrelate};
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TraversedEntity {
    pub table_name: String,
    pub eid : u64,
    pub hrptr: HashedRowPtr,

    pub parent_table: String,
    pub parent_col_index: usize,
}

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
    pub fn insert_ghost_for_template(&mut self, 
                                template: &TemplateEntity,
                                // TODO make more than one ghost family at a time
                                //num_ghosts: usize, 
                                db: &mut mysql::Conn) 
        -> Result<GhostEidMapping, mysql::Error> 
    {
        let start = time::Instant::now();
        let new_entities = generate_new_ghosts_from(&self.views, &self.policy.ghost_policies, template, 1)?;

        let mut ghost_names = vec![];
        let mut root_gid = 0;
   
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
            warn!("new entities issue_insert_dt_stmt: {} dur {}", dt_stmt, start.elapsed().as_micros());
            db.query_drop(dt_stmt.to_string())?;
            self.cur_stat.nqueries+=1;
        
            self.views.insert(&table_entities.table, None, &table_entities.rptrs)?;
        
            for gid in table_entities.gids {
                if root_gid == 0 {
                    root_gid = gid;
                }
                ghost_names.push((table_entities.table.clone(), gid));
            }
        }

        let gem = GhostEidMapping{
            table: template.table.clone(),
            eid: template.eid,
            root_gid: root_gid,
            ghosts: ghost_names,
        };
        warn!("insert_ghost_for_eid {}, {}: {}us", root_gid, template.eid, start.elapsed().as_micros());
        Ok(gem)
    }

    pub fn unsubscribe<W: io::Write>(&mut self, uid: u64, db: &mut mysql::Conn, writer: QueryResultWriter<W>) 
        -> Result<(), mysql::Error> 
    {
        use policy::UnsubscribePolicy::*;

        warn!("Unsubscribing uid {}", uid);

        // table name of entity, eid, gids for eid
        let mut ghost_eid_mappings : Vec<GhostEidMapping> = vec![];

        // all entities to be replaced or removed, as they existed prior to unsubscription
        let mut nodes_to_remove : Vec<TraversedEntity> = vec![];
       
        // all entities to be replaced by ghosted versions
        let mut nodes_to_ghost : HashSet<TraversedEntity> = HashSet::new();

        // track all parent-children edges, may have repeat children
        let mut traversed_entities: HashSet<TraversedEntity> = HashSet::new();

        // queue of children to look at next
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];

        // initialize with the entity specified by the uid
        let mut view_ptr = self.views.get_view(&self.policy.unsub_entity_type).unwrap();
        let matching_row = HashedRowPtr::new(view_ptr.borrow().get_row_of_id(uid), view_ptr.borrow().primary_index);
        children_to_traverse.push(TraversedEntity{
                table_name: self.policy.unsub_entity_type.clone(),
                eid : uid,
                hrptr: matching_row.clone(),
                parent_table: "".to_string(),
                parent_col_index: 0,
        });

        /* 
         * Step 1: TRAVERSAL + DECORRELATION
         * TODO could parallelize to reduce time to traverse?
         */
        while children_to_traverse.len() > 0 {
            let start = time::Instant::now();

            let node = children_to_traverse.pop().unwrap();
                                   
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
            warn!("Found children {:?} of {:?}", children, node);

            // TODO make a pointer so we don't have to clone
            for ((child_table, child_ci), child_hrptrs) in children.iter() {
                let child_table_epolicies = self.policy.edge_policies.get(child_table).unwrap().clone();
                view_ptr = self.views.get_view(&child_table).unwrap();

                for rptr in child_hrptrs {
                    for policy in &child_table_epolicies {
                        let ci = helpers::get_col_index(&policy.column, &view_ptr.borrow().columns).unwrap();
                        
                        // skip any policies for edges not to this parent table type
                        if ci != *child_ci ||  policy.parent != node.table_name {
                            continue;
                        }
                        
                        let child = TraversedEntity {
                            table_name: child_table.clone(),
                            eid: helpers::parser_val_to_u64(&rptr.row().borrow()[view_ptr.borrow().primary_index]),
                            hrptr: rptr.clone(),
                            parent_table: node.table_name.clone(), 
                            parent_col_index: ci,
                        };

                        // we decorrelate or delete *all* in the parent-child direction
                        match policy.pc_policy {
                            Decorrelate(f) => {
                                assert!(f < 1.0); 
                                               
                                // TODO could create all these ghosts at once?
                                let gem = self.insert_ghost_for_template(
                                    &TemplateEntity {
                                        table: node.table_name.clone(),
                                        eid: node.eid,
                                        row: node.hrptr.row().clone(), 
                                        fixed_colvals: None,
                                    }, db)?;
                                
                                let gid = gem.root_gid;
                                
                                ghost_eid_mappings.push(gem);
                                
                                self.views.update_index_and_row_of_view(&child_table, rptr.row().clone(), ci, Some(&Value::Number(gid.to_string())));

                                // if child hasn't been seen yet, traverse
                                if traversed_entities.get(&child).is_none() {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    children_to_traverse.push(child);
                                }

                                // we don't add this to the nodes to ghost because we've already
                                // decorrelated it
                            },
                            Delete(f) => {
                                assert!(f < 1.0); 
                                // add all the sensitive removed entities to return to the user 
                                nodes_to_remove.append(&mut self.get_tree_from_child(&child).into_iter().collect());

                                // replace this node with a ghost node
                                nodes_to_ghost.insert(node.clone());
                                
                                // don't add child to traversal queue
                            },
                            Retain => {
                                // replace this node with a ghost node
                                nodes_to_ghost.insert(node.clone());
                                
                                // if child hasn't been seen yet, traverse
                                if traversed_entities.get(&child).is_none() {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    children_to_traverse.push(child);
                                }
                           }
                        }
                    }
                }
            }
            warn!("UNSUB {}: Duration to traverse+decorrelate {}, {:?}: {}us", 
                      uid, node.table_name.clone(), node, start.elapsed().as_micros());
        }
        
        /* 
         * Step 3: Child->Parent Decorrelation. 
         * For all edges to the parent entity that need to reach a particular sensitivity
         * threshold, decorrelate or remove the children; if retained, ghost the parent. 
         */
        // TODO add to nodes_to_ghost(?)
        self.unsubscribe_child_parent_edges(&traversed_entities, &mut ghost_eid_mappings, &mut nodes_to_remove, &mut nodes_to_ghost, db)?;

        /*
         * Step 4: Change intermediate and leaf entities to ghosts TODO
         */
        for entity in nodes_to_ghost {
            // create ghost for this entity
            let gem = self.insert_ghost_for_template(
                &TemplateEntity {
                    table: entity.table_name.clone(),
                    eid: entity.eid,
                    row: entity.hrptr.row().clone(), 
                    fixed_colvals: None,
                }, db)?;
            let gid = gem.root_gid;
            ghost_eid_mappings.push(gem);
           
            // update children to point to the new ghost
            if let Some(children) = self.views.graph.get_children_of_parent(&entity.table_name, entity.eid) {
                warn!("Found children {:?} of {:?}", children, entity);
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    for hrptr in child_hrptrs {
                        assert!(hrptr.row().borrow()[*child_ci].to_string() == entity.eid.to_string());
                        self.views.update_index_and_row_of_view(&child_table, hrptr.row().clone(), *child_ci, Some(&Value::Number(gid.to_string())));
                    }
                }
            }

            // TODO optimize policies where all values are cloned
            // we ghosted this node, we want to remove it and return to the user
            nodes_to_remove.push(entity.clone());
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
        nodes_to_remove: &mut Vec<TraversedEntity>,
        nodes_to_ghost: &mut HashSet<TraversedEntity>,
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
            let child_columns = self.views.get_view_columns(&poster_child.table_name);
    
            for policy in edge_policies {
                let ci = child_columns.iter().position(|c| policy.column == c.to_string()).unwrap();
     
                // group all table children by EID
                let mut parent_eid_counts : HashMap<u64, Vec<&TraversedEntity>> = HashMap::new();
                for child in table_children {
                    let parent_eid_val = &child.hrptr.row().borrow()[ci];
                    let parent_eid = helpers::parser_val_to_u64(parent_eid_val);
                    if let Some(count) = parent_eid_counts.get_mut(&parent_eid) {
                        count.push(child);
                    } else {
                        parent_eid_counts.insert(parent_eid, vec![child]);
                    }
                }

                let sensitivity : f64;
                match policy.pc_policy {
                    Decorrelate(f) => sensitivity = f,
                    Delete(f) => sensitivity = f,
                    Retain => sensitivity = 1.0,
                }

                // if we're retaining all edges, we just need to ghost the parent
                if sensitivity == 1.0 {
                    for (parent_eid, _) in parent_eid_counts.iter() {
                        // get parent entity, add it to nodes to ghost
                        let parent_rptr = self.views.get_row_of_id(&policy.parent, *parent_eid);
                        nodes_to_ghost.insert(TraversedEntity{
                            table_name: policy.parent.clone(),
                            eid: *parent_eid,
                            hrptr: HashedRowPtr::new(parent_rptr, self.views.get_view_pi(&policy.parent)),
                            parent_table: "".to_string(),
                            parent_col_index: 0,
                        });
                    }
                    continue;
                }

                // otherwise, decorrelate/delete as necessary
                for (parent_eid, sensitive_children) in parent_eid_counts.iter() {
                    // get all children of this type with the same parent entity eid
                    let childrows = self.views.graph.get_children_of_parent(&policy.parent, *parent_eid).unwrap();
                    let total_count = childrows.get(&(poster_child.table_name.clone(), ci)).unwrap().len() as f64;
                    let sensitive_count = sensitive_children.len() as f64;
                    warn!("Found {} total and {} sensitive children of type {} with parent {}", 
                          total_count, sensitive_count, policy.parent, parent_eid);
                    
                    let number_to_desensitize = (((sensitive_count * (1.0-sensitivity)) 
                                                  - (total_count * sensitivity)) 
                                                 / (1.0-sensitivity)).ceil() as i64;
                    if number_to_desensitize > 0 {
                        let parent_rptr = self.views.get_row_of_id(&policy.parent, *parent_eid);
                        
                        if number_to_desensitize > sensitive_count as i64 {
                            unimplemented!("No support for decorrelated or removing non-sensitive children");
                        }
                        
                        // some children still remain, so original parent should be ghosted
                        if number_to_desensitize < total_count as i64 {
                            nodes_to_ghost.insert(TraversedEntity{
                                table_name: policy.parent.clone(),
                                eid: *parent_eid,
                                hrptr: HashedRowPtr::new(parent_rptr.clone(), self.views.get_view_pi(&policy.parent)),
                                parent_table: "".to_string(),
                                parent_col_index: 0,
                            });
                        }

                        for child in &sensitive_children[0..number_to_desensitize as usize] {
                            match policy.pc_policy {
                                Decorrelate(_) => {
                                    let gem = self.insert_ghost_for_template(
                                        &TemplateEntity {
                                            table: policy.parent.clone(),
                                            eid: *parent_eid,
                                            row: parent_rptr.clone(), 
                                            fixed_colvals: None,
                                        }, db)?;
                                    let gid = gem.root_gid;
                                    ghost_eid_mappings.push(gem);
                                    self.views.update_index_and_row_of_view(
                                        &child.table_name, child.hrptr.row().clone(), ci, Some(&Value::Number(gid.to_string())));
                                }
                                Delete(_) => {
                                    // add all the sensitive removed entities to return to the user 
                                    nodes_to_remove.append(&mut self.get_tree_from_child(&child).into_iter().collect());
                                }
                                _ => unimplemented!("Shouldn't have a retain policy with a positive number to desensitize")
                            }
                        }
                    }
                }
            }
        }
        warn!("UNSUB: Duration to look at and remove/desensitize child-parent edges: {}us", start.elapsed().as_micros());
        Ok(())
    }

    pub fn get_tree_from_child(&mut self, child: &TraversedEntity)
        -> Vec<TraversedEntity>
    {
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];
        let mut entities_to_remove: Vec<TraversedEntity> = vec![];
        children_to_traverse.push(child.clone());
        let mut node: TraversedEntity;

        while children_to_traverse.len() > 0 {
            node = children_to_traverse.pop().unwrap().clone();

            // see if any entity has a foreign key to this one; we'll need to remove those too
            // NOTE: because traversal was parent->child, all potential children down the line
            // SHOULD already been in seen_children
            if let Some(children) = self.views.graph.get_children_of_parent(&node.table_name, node.eid) {
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    let view_ptr = self.views.get_view(&child_table).unwrap();
                    for hrptr in child_hrptrs {
                        children_to_traverse.push(TraversedEntity {
                            table_name: child_table.clone(),
                            eid: helpers::parser_val_to_u64(&hrptr.row().borrow()[view_ptr.borrow().primary_index]),
                            hrptr: hrptr.clone(),
                            parent_table: node.table_name.clone(), 
                            parent_col_index: *child_ci,
                        });
                    }
                }
            }
            entities_to_remove.push(node.clone());
        }
        entities_to_remove 
    }

    fn remove_entities(&mut self, nodes: &Vec<TraversedEntity>, db: &mut mysql::Conn) -> Result<(), mysql::Error> {
        let id_col = Expr::Identifier(helpers::string_to_idents(ID_COL));
        let eid_exprs : Vec<Expr> = nodes.iter().map(|node| Expr::Value(Value::Number(node.eid.to_string()))).collect();
        let ids: Vec<u64> = nodes.iter().map(|node| node.eid).collect();
        let selection = Some(Expr::InList{
                expr: Box::new(id_col),
                list: eid_exprs,
                negated: false,
        });
     
        warn!("UNSUB remove: deleting {:?} {:?}", nodes, ids);
        self.views.delete_rptrs_with_ids(&nodes[0].table_name, &ids)?;

        let delete_eid_from_table = Statement::Delete(DeleteStatement {
            table_name: helpers::string_to_objname(&nodes[0].table_name),
            selection: selection.clone(),
        });
        warn!("UNSUB remove: {}", delete_eid_from_table);
        db.query_drop(format!("{}", delete_eid_from_table.to_string()))?;
        self.cur_stat.nqueries+=1;
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
        let mut root_gid = ghost_eid_mappings[0].root_gid;
        let mut ghosts : Vec<Vec<(String, u64)>> = vec![];
        for mapping in ghost_eid_mappings {
            // do all the work for this eid at once!
            if !(table == mapping.table && eid == mapping.eid) {
                self.replace_ghosts(&table, eid, root_gid, &ghosts, db)?;

                // reset 
                eid = mapping.eid.clone();
                root_gid = mapping.root_gid.clone();
                table = mapping.table.clone();
                ghosts = vec![mapping.ghosts.clone()];
            } else {
                ghosts.push(mapping.ghosts.clone());
            }
        }
        self.replace_ghosts(&table, eid, root_gid, &ghosts, db)?;

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
 

    fn replace_ghosts(&mut self, curtable: &str, eid: u64, root_gid: u64, ghosts: &Vec<Vec<(String, u64)>>, db: &mut mysql::Conn) -> Result<(), mysql::Error> 
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
        let eid_val = Value::Number(eid.to_string());
            
        // Note: all ghosts in families will be deleted from the MV, so we only need to restore
        // the EID value for the root level GID entries
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
                                warn!("Updating child row with GID {} to point to actual eid {}", root_gid, eid_val);
                                self.views.update_index_and_row_of_view(&child_table, hrptr.row().clone(), *ci, Some(&eid_val));
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
