use mysql::prelude::*;
use sql_parser::ast::*;
use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use std::*;
use msql_srv::{QueryResultWriter};
use log::{warn};
use ordered_float::*;

use crate::{policy, helpers, subscriber, query_simplifier};
use crate::views::*;

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
    pub sensitivity: OrderedFloat<f64>,
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
                        values = query_simplifier.insert_source_query_to_rptrs(&q)?;
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
                    assign_vals.push(self.expr_to_value_expr(&a.value)?);
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
        views::view_cols_rows_to_answer_rows(&view_res.0, view_res.1, &view_res.2, writer)
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
    pub fn unsubscribe<W: io::Write>(&mut self, uid: u64, db: &mut mysql::Conn, writer: QueryResultWriter<W>) 
        -> Result<(), mysql::Error> 
    {
        use policy::UnsubscribePolicy::*;

        warn!("Unsubscribing uid {}", uid);

        // table name of entity, eid, gids for eid
        let mut ghost_eid_mappings : Vec<GhostEidMapping> = vec![];

        // all entities to be replaced or removed, as they existed prior to unsubscription
        let mut nodes_to_remove : Vec<EntityData> = vec![];
       
        // all entities to be replaced by ghosted versions
        let mut nodes_to_ghost : Vec<EntityData> = vec![];

        // track all parent-children edges, may have repeat children
        let mut parent_child_edges : HashSet<TraversedEntity> = HashSet::new();

        // track traversed children (may come multiple times via different parents)
        let mut traversed_children : HashSet<(String, u64)> = HashSet::new();

        // queue of children to look at next
        let mut children_to_traverse: Vec<TraversedEntity> = vec![];

        // initialize with the entity specified by the uid
        let mut view_ptr = self.views.get_view(&self.policy.decor_etype).unwrap();
        let matching_row = HashedRowPtr::new(view_ptr.borrow().get_row_of_id(uid), view_ptr.borrow().primary_index);
        children_to_traverse.push(TraversedEntity{
                table_name: self.policy.decor_etype.clone(),
                eid : uid,
                hrptr: matching_row.clone(),
                from_table: "".to_string(),
                from_col_index: 0,
                sensitivity: OrderedFloat(-1.0),
        });

        /* 
         * Step 1: TRAVERSAL + DECORRELATION
         * TODO could parallelize to reduce time to traverse?
         */
        while children_to_traverse.len() > 0 {
            let start = time::Instant::now();

            let node = children_to_traverse.pop().unwrap();
            let nodedata = EntityData{
                table: node.table_name,
                row_strs: node.hrptr.row().borrow().iter().map(|v| v.to_string()).collect(),
            };
            nodes_to_remove.push(nodedata);
           
            // get children of this node
            let children : EntityTypeRows;
            match self.views.graph.get_children_of_parent(&node.table_name, node.eid) {
                None => continue,
                Some(cs) => children = cs,
            }
            warn!("Found children {:?} of {:?}", children, node);

            let is_leaf = true;
            let is_decorrelated = true;
            // TODO make a pointer so we don't have to clone
            for ((child_table, child_ci), child_hrptrs) in children.iter() {
                let child_table_epolicies = self.policy.table2policies.get(child_table).unwrap();
                view_ptr = self.views.get_view(&child_table).unwrap();

                for rptr in child_hrptrs {
                    for policy in child_table_epolicies {
                        let ci = helpers::get_col_index(&policy.column, &view_ptr.borrow().columns).unwrap();
                        
                        // skip any policies for edges not to this parent table type
                        if ci != *child_ci ||  policy.parent != node.table_name {
                            continue;
                        }
                        
                        let mut child = TraversedEntity {
                            table_name: child_table.clone(),
                            eid: helpers::parser_val_to_u64(&rptr.row().borrow()[view_ptr.borrow().primary_index]),
                            vals: rptr.clone(),
                            parent_table: node.table_name.clone(), 
                            parent_col_index: ci,
                            sensitivity: OrderedFloat(0.0),
                        };

                        // we decorrelate or delete *all* in the parent-child direction
                        match policy.pc_policy {
                            Decorrelate(f) => {
                                assert!(f < 1.0); 
                                                   
                                child.sensitivity = OrderedFloat(f);
                                assert!(gid_index < gid_values.len());
                                let val = &gid_values[gid_index];
                                warn!("UNSUB Decorrelate: updating {} {:?} with {}", child_table, rptr, val);

                                self.views.update_index_and_row_of_view(&child_table, rptr.row().clone(), ci, Some(&val));
                                gid_index += 1;

                                ghost_eid_mappings.push(GhostEidMapping{
                                    table: node.table_name.clone(), 
                                    eid2gidroot: Some((node.eid, ghost_family.root_gid)), 
                                    ghosts: family_ghost_names, 
                                });

                                // if child hasn't been seen yet, traverse
                                if traversed_children.insert((child.table_name.clone(), child.eid)) {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    children_to_traverse.push(child);
                                }
                            },
                            Delete(f) => {
                                assert!(f < 1.0); 
                                child.sensitivity = OrderedFloat(f);
                                // add all the sensitive removed entities to return to the user 
                                nodes_to_remove.append(&mut self.get_tree_from_child(&child).into_iter().collect());

                                // replace this node with a ghost node
                                nodes_to_ghost.insert(node);
                                
                                // don't add child to traversal queue
                            },
                            Retain => {
                                is_leaf = false;
                                is_decorrelated = false;
                                child.sensitivity = OrderedFloat(1.0);
                                // if child hasn't been seen yet, traverse
                                if traversed_children.insert((child.table_name.clone(), child.eid)) {
                                    warn!("Adding traversed child {}, {}", child.table_name, child.eid);
                                    children_to_traverse.push(child);
                                } 
                            }
                        }
                    }
                }
            }
            nodes_to_ghost.insert(node);
            warn!("UNSUB {}: Duration to traverse+decorrelate {}, {:?}: {}us", 
                      uid, node.table_name, node, start.elapsed().as_micros());
           
            // add edge to seen edges because we want to check their outgoing
            // child->parent edges for sensitivity
            parent_child_edges.insert(node.clone());
        }
        
        /* 
         * Step 3: Child->Parent Decorrelation. 
         * For all edges to the parent entity that need to reach a particular sensitivity
         * threshold, decorrelate or remove the children; if retained, ghost the parent. 
         */
        self.unsubscribe_child_parent_edges(&parent_child_edges, &mut ghost_eid_mappings, db)?;

        /*
         * Step 4: Change leaf entities to ghosts TODO
         */
        for entity in sensitive_entities {

        }
        self.remove_entities(&nodes_to_remove, db)?;

        /*
         * Step 5: Return data to user
         */
        self.subscriber.record_unsubbed_user_and_return_results(writer, uid, &mut ghost_eid_mappings, &mut sensitive_entities, db)
    }

    pub fn unsubscribe_child_parent_edges(&mut self, 
        children: &HashSet<TraversedEntity>, 
        ghost_eid_mappings: &mut Vec<GhostEidMapping>,
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
            let mut ghost_parent_keys_and_types : Vec<(String, String)> = vec![];
            let mut sensitive_cols_and_types: Vec<(String, String, f64)> = vec![];
            let ghost_parent_keys_and_types = self.policy.table2policies.get(table_name).unwrap();
            let poster_child = table_children[0];
            let child_columns = self.views.get_view_columns(&poster_child.table_name);
            
            // this table type has edges that should be decorrelated 
            for policy in ghost_parent_keys_and_types {
                let ci = child_columns.iter().position(|c| policy.column == c.to_string()).unwrap();
                for child in table_children {
                    // if parent is not the from_parent (which could be a ghost!),
                    if !ghosts::is_ghost_eidval(&child.vals.row().borrow()[ci]) {
                        let eid = helpers::parser_val_to_u64(&child.vals.row().borrow()[ci]);
                        
                        if let Some(family) = self.ghost_maps.take_one_ghost_family_for_eid(eid, db, &policy.parent)? {
                            // this parent already has ghosts! remove the mapping from the real parent 
                            // ensure that any parent ghosts of these ghost entities also become
                            // visible in the MVs for referential integrity
                            let mut ancestor_table_ghosts = vec![];
                            for ghost_entities in &family.family_members {
                                self.views.insert(&ghost_entities.table, None, &ghost_entities.rptrs)?;
                                for gid in &ghost_entities.gids {
                                    ancestor_table_ghosts.push((family.root_table.clone(), *gid));
                                }
                            }
                            
                            // changing child to point ghost of the real parent,
                            self.views.update_index_and_row_of_view(
                                &table_name, child.vals.row().clone(), 
                                ci, Some(&Value::Number(family.root_gid.to_string())));
                            
                            ghost_eid_mappings.push(GhostEidMapping{
                                table: policy.parent.clone(), 
                                eid2gidroot: Some((eid, family.root_gid)),
                                ghosts: ancestor_table_ghosts,
                            });
                        } else {
                            unimplemented!("Ghost entity must already exist for decorrelatable edges!");
                        }
                    }
                }
            }

            let mut removed = HashSet::new();
            // this table has sensitive parents! deal with accordingly
            for (col, parent_table, sensitivity) in sensitive_cols_and_types {
                if sensitivity == 0.0 {
                    // if sensitivity is 0, remove the child :-\
                    for child in table_children {
                        // TODO
                        /*if !removed.contains(*child) {
                            warn!("Unsub child-parent Removing {:?}", child);
                            removed.extend(self.recursive_remove(child, db)?);
                        }*/
                    }
                }
                if sensitivity == 1.0 {
                    // if sensitivity is 1, we don't need to do anything
                    continue
                } 
                // otherwise, collect all edges to measure sensitivity 
                let ci = child_columns.iter().position(|c| col == c.to_string()).unwrap();
                
                // don't re-add parents that were traversed...
                let mut parent_eid_counts : HashMap<u64, usize> = HashMap::new();
                
                // group all table children by EID
                for child in table_children {
                    // TODO
                    /*if removed.contains(*child) {
                        continue;
                    }*/
                    let parent_eid_val = &child.vals.row().borrow()[ci];
                    if !ghosts::is_ghost_eidval(parent_eid_val) {
                        let parent_eid = helpers::parser_val_to_u64(parent_eid_val);
                        if let Some(count) = parent_eid_counts.get_mut(&parent_eid) {
                            *count += 1;
                        } else {
                            parent_eid_counts.insert(parent_eid, 1);
                        }
                    }
                }

                for (parent_eid, sensitive_count) in parent_eid_counts.iter() {
                    // get all children of this type with the same parent entity eid
                    let childrows = self.views.graph.get_children_of_parent(&parent_table, *parent_eid).unwrap();
                    let total_count = childrows.get(&(poster_child.table_name.clone(), ci)).unwrap().len();
                    warn!("Found {} total and {} sensitive children of type {} with parent {}", 
                          total_count, sensitive_count, poster_child.table_name, parent_eid);
                    let needed = (*sensitive_count as f64 / sensitivity).ceil() as i64 - total_count as i64;

                    if needed > 0 && self.policy.ghost_policies.get(&poster_child.table_name).is_none() {
                        // TODO
                        // no ghost generation policy for this table; remove as many children as needed :-\
                        /*for i in 0..needed {
                            warn!("Unsub parent-child Removing {:?}", table_children[i as usize]);
                            removed.extend(self.recursive_remove(&table_children[i as usize], children, db)?);
                        }*/
                    } else if needed > 0 {
                        let gids = ghosts::generate_new_ghost_gids(needed as usize);
                        // TODO could choose a random child as the poster child 
                        warn!("Achieve child parent sensitivity: generating values for gids {:?}", gids);
                        let new_entities = ghosts::generate_new_ghosts_with_gids(
                            &self.views, &self.policy.ghost_policies, db, 
                            &TemplateEntity{
                                table: poster_child.table_name.clone(),
                                row: poster_child.vals.row().clone(), 
                                fixed_colvals: Some(vec![(ci, Value::Number(parent_eid.to_string()))]),
                            },
                            &gids,
                            &mut self.cur_stat.nqueries)?;

                        // insert all ghost ancestors created into the MV
                        let mut ancestor_table_ghosts = vec![];
                        for ghost_entities in &new_entities {
                            self.views.insert(&ghost_entities.table, None, &ghost_entities.rptrs)?;
                            for gid in &ghost_entities.gids {
                                ancestor_table_ghosts.push((ghost_entities.table.clone(), *gid));
                            }
                        }
                        ghost_eid_mappings.push(GhostEidMapping{
                            table: poster_child.table_name.clone(), 
                            eid2gidroot: None, 
                            ghosts: ancestor_table_ghosts,
                        });
                    }
                }
            }
        }
        warn!("UNSUB: Duration to look at and remove/desensitize child-parent edges: {}us", start.elapsed().as_micros());
        Ok(())
    }

    pub fn get_tree_from_child(&mut self, child: &TraversedEntity)
        -> HashSet<EntityData>
    {
        let mut seen_children : HashSet<EntityData> = HashSet::new();
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
                            vals: hrptr.clone(),
                            from_table: node.table_name.clone(), 
                            from_col_index: *child_ci,
                            sensitivity: OrderedFloat(0.0),
                        });
                    }
                }
            }
            entities_to_remove.push(node.clone());

            seen_children.insert(EntityData {
                table: node.table_name,
                row_strs: node.hrptr.row().borrow().iter().map(|v| v.to_string()).collect(),
            });
        }
        seen_children
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
     * refresh "materialized views"
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
            //warn!("processing {}, {:?}, {}", table, eid, gid);
            // do all the work for this table at once!
            if !(curtable == entity.table) {
                self.reinsert_view_rows(&curtable, &curvals, db)?;
                
                // reset 
                curtable = entity.table.clone();
                curvals = vec![entity.row_strs.clone()];
            } else {
                curvals.push(entity.row_strs.clone()); 
            }
        }
        self.reinsert_view_rows(&curtable, &curvals, db)?;

        // parse gids into table eids -> set of gids
        let mut table = ghost_eid_mappings[0].table.clone();
        let mut eid2gid = ghost_eid_mappings[0].eid2gidroot.clone();
        let mut ghosts : Vec<Vec<(String, u64)>> = vec![];
        for mapping in ghost_eid_mappings {
            // do all the work for this eid at once!
            if !(table == mapping.table && eid2gid == mapping.eid2gidroot) {
                self.resubscribe_ghosts_map(&table, &eid2gid, &ghosts, db)?;

                // reset 
                eid2gid = mapping.eid2gidroot.clone();
                table = mapping.table.clone();
                ghosts = vec![mapping.ghosts.clone()];
            } else {
                ghosts.push(mapping.ghosts.clone());
            }
        }
        self.resubscribe_ghosts_map(&table, &eid2gid, &ghosts, db)?;

        Ok(())
    }

    fn reinsert_view_rows(&mut self, curtable: &str, curvals: &Vec<Vec<String>>, db: &mut mysql::Conn) 
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
 

    fn resubscribe_ghosts_map(&mut self, curtable: &str, eid2gidroot: &Option<(u64, u64)>, ghosts: &Vec<Vec<(String, u64)>>, db: &mut mysql::Conn) -> Result<(), mysql::Error> 
    {
        let start = time::Instant::now();

        let mut ghost_families : Vec<GhostFamily> = vec![];
        
        // maps from tables to the gid/rptrs of ghost entities from that table
        let mut table_to_gid_rptrs: HashMap<String, Vec<(u64, RowPtr)>> = HashMap::new();
        for ancestor_group in ghosts {
            let mut family_members = vec![]; 
            let mut cur_ancestor_table = "";
            let mut cur_ancestor_rptrs = vec![]; 
            let mut cur_ancestor_gids= vec![]; 
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

                /*
                 * We only care about this next part of the loop if eid2gidroot is some
                 * This means that the rptrs need to be kept around because they were ancestors of
                 * some actual entity that was decorrelated
                 */
                if eid2gidroot.is_some() {
                    if cur_ancestor_table != ancestor_table {
                        if !cur_ancestor_table.is_empty() {
                            family_members.push(TableGhostEntities{
                                table: cur_ancestor_table.to_string(),
                                gids: cur_ancestor_gids,
                                rptrs: cur_ancestor_rptrs,
                            });
                        }
                        cur_ancestor_table = ancestor_table;
                        cur_ancestor_rptrs = vec![]; 
                        cur_ancestor_gids = vec![];
                    }
                    cur_ancestor_rptrs.push(ancestor_rptr.clone());
                    cur_ancestor_gids.push(*ancestor_gid);
                }
            }
            if let Some((_eid, gidroot)) = eid2gidroot {
                if !cur_ancestor_table.is_empty() {
                    family_members.push(TableGhostEntities{
                        table: cur_ancestor_table.to_string(),
                        gids: cur_ancestor_gids,
                        rptrs: cur_ancestor_rptrs,
                    });
                }
                ghost_families.push(GhostFamily{
                    root_table: cur_ancestor_table.to_string(),
                    root_gid: *gidroot,
                    family_members: family_members,
                });
            }
        }
        // these GIDs were stored in an actual non-ghost entity before decorrelation
        // we need to update child in the MV to now show the EID
        // and also put these GIDs back in the ghost map
        if let Some((eid, gidroot)) = eid2gidroot {
            let eid_val = Value::Number(eid.to_string());

            warn!("RESUB: actually restoring {} eid {}, gprtrs {:?}", curtable, eid, ghost_families);
            if !self.ghost_maps.resubscribe(*eid, &ghost_families, db, curtable)? {
                return Err(mysql::Error::IoError(io::Error::new(
                    io::ErrorKind::Other, format!("not unsubscribed {}", eid))));
            }             
            // Note: all ghosts in families will be deleted from the MV, so we only need to restore
            // the EID value for the root level GID entries
            if let Some(children) = self.views.graph.get_children_of_parent(curtable, *gidroot) {
                warn!("Get children of table {} GID {}: {:?}", curtable, gidroot, children);
                // for each child row
                for ((child_table, child_ci), child_hrptrs) in children.iter() {
                    let child_viewptr = self.views.get_view(&child_table).unwrap();
                    let ghost_parent_keys = helpers::get_ghost_parent_key_indices_of_datatable(
                        &self.policy, &child_table, &child_viewptr.borrow().columns);
                    // if the child has a column that is ghosted and the ghost ID matches this gid
                    for (ci, parent_table) in &ghost_parent_keys {
                        if ci == child_ci && parent_table == &curtable {
                            for hrptr in child_hrptrs {
                                if hrptr.row().borrow()[*ci].to_string() == gidroot.to_string() {
                                    // then update this child to use the actual real EID
                                    warn!("Updating child row with GID {} to point to actual eid {}", gidroot, eid_val);
                                    self.views.update_index_and_row_of_view(&child_table, hrptr.row().clone(), *ci, Some(&eid_val));
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
        if eid2gidroot.is_none() {
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
