use crate::helpers::*;
use crate::stats::*;
use crate::*;
use crate::{history};
use mysql::{Opts, Pool};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};

pub enum TransformArgs {
    Remove,
    Modify {
        // name of column
        col: String,
        // how to generate a modified value
        generate_modified_value: Box<dyn Fn(&str) -> String + Send + Sync>,
        // post-application check that value satisfies modification
        satisfies_modification: Box<dyn Fn(&str) -> bool + Send + Sync>,
    },
    Decor {
        referencer_col: String,
        fk_name: String,
        fk_col: String,
    },
}

#[derive(Clone)]
pub struct Transform {
    pub pred: String,
    pub trans: Arc<RwLock<TransformArgs>>,
    pub permanent: bool,
}

#[derive(Clone)]
pub struct TableDisguise {
    pub name: String,
    pub id_cols: Vec<String>,
    pub owner_cols: Vec<String>,
    pub transforms: Vec<Transform>,
}

pub struct GuiseGen {
    pub col_generation: Box<dyn Fn() -> Vec<String> + Send + Sync>,
    pub val_generation: Box<dyn Fn() -> Vec<Expr> + Send + Sync>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: u64,
    pub nonce: Vec<u8>,
    pub key: Vec<u8>,
}

pub struct Disguise {
    pub disguise_id: u64,
    pub user: Option<User>,
    pub table_disguises: HashMap<String, Arc<RwLock<TableDisguise>>>,
    pub guise_gen: HashMap<String, Arc<RwLock<GuiseGen>>>,
}

pub struct Disguiser {
    pub pool: mysql::Pool,
    pub stats: Arc<Mutex<QueryStat>>,
    //pdkstore: PDKStore,
    items: Arc<RwLock<HashMap<String, HashMap<Vec<RowVal>, Vec<Transform>>>>>,
    to_delete: Arc<Mutex<Vec<String>>>,
    to_insert: Arc<Mutex<Vec<Vec<Expr>>>>,
}

impl Disguiser {
    pub fn new(url: &str) -> Disguiser {
        let opts = Opts::from_url(&url).unwrap();
        let pool = Pool::new(opts).unwrap();

        Disguiser {
            pool: pool,
            stats: Arc::new(Mutex::new(stats::QueryStat::new())),
            //pdkstore: PDKStore::new(),
            to_insert: Arc::new(Mutex::new(vec![])),
            to_delete: Arc::new(Mutex::new(vec![])),
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn apply(&mut self, disguise: Arc<Disguise>) -> Result<(), mysql::Error> {
        let de = history::DisguiseEntry {
            user_id: match &disguise.user {
                Some(u) => u.id,
                None => 0,
            },
            disguise_id: disguise.disguise_id,
            reverse: false,
        };

        let mut conn = self.pool.get_conn()?;
        //let mut threads = vec![];

        /*
         * PHASE 0: Get PDK that can be utilized to expand the scope of this disguise
         *
         * TODO Optimizations:
         *  - only reclaim ownership from disguises that may conflict
         *  - don't reclaim ownership for objects that will again be disowned
         *  - separate out tokens for ownership from tokens for updates/deletes
         *  - lazily apply modifications to removed objects
         *
         *  TODO expand the predicate of the disguise to apply based on PDK
         */

        /*
         * PHASE 1: Predicate
         */
        // get all the objects, set all the objects to remove
        // integrate vault_transforms into disguise read + write phases
        self.select_predicate_objs(disguise.clone());//pdk);

        /*
         * PHASE 2: REMOVAL
         */
        self.execute_removes(&mut conn)?;

        /*
         * PHASE 3: UPDATE/DECOR
         */
        /*let fk_cols = Arc::new((disguise.guise_info.read().unwrap().col_generation)());
        for (_, table_disguise) in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items.clone();
            let my_fkcols = fk_cols.clone();

            let user_id = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let disguise_id = disguise.disguise_id;
            let guise_info = disguise.guise_info.clone();

            threads.push(thread::spawn(move || {
                let guise_info = guise_info.read().unwrap();
                let table = table_disguise.read().unwrap();
                let mut conn = pool.get_conn().unwrap();
                let my_items = my_items.read().unwrap();
                let my_items = my_items.get(&table.name).unwrap();
                warn!("Thread {:?} starting for table {}", thread::current().id(), table.name);
                // get and apply the transformations for each object
                let mut update_stmts = vec![];
                for (i, ts) in (*my_items).iter() {
                    let mut cols_to_update = vec![];
                    debug!(
                        "Get_Select_Of_Row: Getting ids of table {} for {:?}",
                        table.name, i
                    );
                    let i_select = get_select_of_row(&table.id_cols, &i);
                    for t in ts {
                        match &*t.trans.read().unwrap() {
                            TransformArgs::Decor {
                                referencer_col,
                                fk_name,
                                fk_col,
                            } => {
                                warn!(
                                    "Thread {:?} starting decor {}",
                                    thread::current().id(),
                                    table.name
                                );
                                let start = time::Instant::now();

                                /*
                                 * DECOR OBJECT MODIFICATIONS
                                 * A) insert guises for parents
                                 * B) update child to point to new guise
                                 * */
                                // get ID of parent
                                let old_uid =
                                    u64::from_str(&get_value_of_col(&i, &referencer_col).unwrap())
                                        .unwrap();
                                warn!(
                                    "decor_obj {}: Creating guises for fkids {:?} {:?}",
                                    table.name, fk_name, old_uid,
                                );

                                // skip already decorrelated users
                                if is_guise(old_uid) {
                                    warn!(
                                        "decor_obj: skipping decorrelation for {}.{}, already a guise",
                                        fk_name, old_uid
                                    );
                                    continue;
                                }

                                // Phase 3A: create new parent
                                let new_parent = (*guise_info.val_generation)();
                                let guise_id = new_parent[0].to_string();
                                warn!("decor_obj: inserted guise {}.{}", fk_name, guise_id);
                                let mut locked_insert = my_insert.lock().unwrap();
                                locked_insert.push(new_parent.clone());
                                drop(locked_insert);

                                // Phase 3B: update child guise
                                cols_to_update.push(Assignment {
                                    id: Ident::new(referencer_col.clone()),
                                    value: Expr::Value(Value::Number(guise_id.to_string())),
                                });

                                /*
                                 * Save PDK: 
                                 * A) inserted guises, associate with old parent uid
                                 * B) update to child to point to new guise
                                 * */
                                // Phase 3A: update the vault with new guise (calculating the uid from the last_insert_id)
                                let mut ix = 0;
                                let new_parent_rowvals: Vec<RowVal> = new_parent
                                    .iter()
                                    .map(|v| {
                                        let index = ix;
                                        ix += 1;
                                        RowVal {
                                            column: my_fkcols[index].to_string(),
                                            value: v.to_string(),
                                        }
                                    })
                                    .collect();
                                /*let pdk = PDKEntry::new_insert_guise(did, uid, table, 
                                    get_ids(&fk_name.id_cols, &new_parent), table.name);*/
                                //self.pdkstore.insert_pdk(pdk);

                                // Phase 3B: update the vault with the modification to children
                                let new_child: Vec<RowVal> = i
                                    .iter()
                                    .map(|v| {
                                        if &v.column == referencer_col {
                                            RowVal {
                                                column: v.column.clone(),
                                                value: guise_id.to_string(),
                                            }
                                        } else {
                                            v.clone()
                                        }
                                    })
                                    .collect();
                                // TODO 
                                // warn(Decor: Getting ids of table {} for {:?}", table.name, i);

                                let mut locked_stats = mystats.lock().unwrap();
                                locked_stats.decor_dur += start.elapsed();
                                drop(locked_stats);
                                warn!("Thread {:?} decor {}", thread::current().id(), table.name);
                            }

                            TransformArgs::Modify {
                                col,
                                generate_modified_value,
                                ..
                            } => {
                                warn!(
                                    "Thread {:?} starting mod {}",
                                    thread::current().id(),
                                    table.name
                                );
                                let start = time::Instant::now();

                                let old_val = get_value_of_col(&i, &col).unwrap();
                                let new_val = (*(generate_modified_value))(&old_val);

                                /*
                                 * PHASE 2: OBJECT MODIFICATIONS
                                 * */
                                cols_to_update.push(Assignment {
                                    id: Ident::new(col.clone()),
                                    value: Expr::Value(Value::String(new_val.clone())),
                                });

                                /*
                                 * PHASE 3: VAULT UPDATES
                                 * */
                                // XXX insert a vault entry for every owning user (every fk)
                                // should just update for the calling user, if there is one?
                                if !t.permanent {
                                    warn!("Modify: Getting ids of table {} for {:?}", table.name, i);
                                    let new_obj: Vec<RowVal> = i
                                        .iter()
                                        .map(|v| {
                                            if &v.column == col {
                                                RowVal {
                                                    column: v.column.clone(),
                                                    value: new_val.clone(),
                                                }
                                            } else {
                                                v.clone()
                                            }
                                        })
                                        .collect();

                                    let ids = get_ids(&table.id_cols, &i);
                                    // TODO 
                                    /*let mut myvv_locked = myvv.lock().unwrap();
                                    for owner_col in &table.owner_cols {
                                        let uid = get_value_of_col(&i, &owner_col).unwrap();
                                        let uid64 = u64::from_str(&uid).unwrap();
                                        let should_insert = user_id == 0 || user_id == uid64;
                                        if should_insert {
                                        };
                                    }*/
                                }

                                let mut locked_stats = mystats.lock().unwrap();
                                locked_stats.mod_dur += start.elapsed();
                                drop(locked_stats);
                                warn!("Thread {:?} modify {}", thread::current().id(), table.name);
                            }
                            _ => unimplemented!("Removes should already have been performed!"),
                        }
                    }

                    // updates are per-item
                    if !cols_to_update.is_empty() {
                        update_stmts.push(
                            Statement::Update(UpdateStatement {
                                table_name: string_to_objname(&table.name),
                                assignments: cols_to_update,
                                selection: Some(i_select),
                            })
                            .to_string(),
                        );
                    }
                }
                warn!("Thread {:?} updating", thread::current().id());
                for stmt in update_stmts {
                    query_drop(stmt, &mut conn, mystats.clone()).unwrap();
                }
                warn!("Thread {:?} exiting", thread::current().id());
            }));
        }
        // wait until all mysql queries are done
        for jh in threads.into_iter() {
            match jh.join() {
                Ok(_) => (),
                Err(_) => warn!("Join failed?"),
            }
        }
        let locked_insert = self.to_insert.lock().unwrap();
        if !locked_insert.is_empty() {
            query_drop(
                Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&disguise.guise_info.read().unwrap().name),
                    columns: fk_cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                    source: InsertSource::Query(Box::new(values_query(locked_insert.clone()))),
                })
                .to_string(),
                &mut conn,
                self.stats.clone(),
            )
            .unwrap();
        }
        drop(locked_insert);
        warn!("Disguiser: Performed Inserts");

        // TODO
        warn!("Disguiser: Inserted Vault Entries");

        self.record_disguise(&de, &mut conn)?;

        self.clear_disguise_records();*/
        Ok(())
    }

    fn execute_removes(&self, conn: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
        let start = time::Instant::now();
        for stmt in &*self.to_delete.lock().unwrap() {
            helpers::query_drop(stmt.to_string(), conn, self.stats.clone())?;
        }
        self.stats.lock().unwrap().remove_dur += start.elapsed();
        Ok(())
    }

    fn select_predicate_objs(
        &self,
        disguise: Arc<Disguise>,
    ) {
        /*let mut threads = vec![];
        for (table, table_disguise) in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_delete = self.to_delete.clone();
            let disguise_id = disguise.disguise_id;
            let user_id = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let my_items = self.items.clone();
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Transform>> = HashMap::new();

            /*let table_vts = match vault_transforms.get(&table) {
                Some(vts) => vts.clone(),
                None => Arc::new(RwLock::new(vec![])),
            };*/

            threads.push(thread::spawn(move || {
                let table = table_disguise.read().unwrap();
                let mut conn = pool.get_conn().unwrap();
                let mut removed_items: HashSet<Vec<RowVal>> = HashSet::new();
                let mut decorrelated_items: HashSet<(String, Vec<RowVal>)> = HashSet::new();

                // get transformations in order of disguise application
                // (so vault transforms are always first)
                let mut transforms = vec![];

                for t in transforms {
                    let pred_items = get_query_rows_str(
                        &str_select_statement(&table.name, &t.pred),
                        &mut conn,
                        mystats.clone(),
                    )
                    .unwrap();

                    // just remove item if it's supposed to be removed
                    match &*t.trans.read().unwrap() {
                        TransformArgs::Remove => {
                            // we're going to remove these, but may later restore them, remember in vault
                            for i in &pred_items {
                                if removed_items.contains(i) {
                                    continue;
                                }
                                debug!("Remove: Getting ids of table {} for {:?}", table.name, i);
                                if !t.permanent {
                                    let ids = get_ids(&table.id_cols, i);
                                    for owner_col in &table.owner_cols {
                                        let uid = get_value_of_col(&i, &owner_col).unwrap();
                                        let uid64 = u64::from_str(&uid).unwrap();
                                        let should_insert = user_id == uid64 || user_id == 0;
                                        if should_insert {
                                            // TODO
                                        }
                                    }
                                }
                                // EXTRA: save in removed so we don't transform this item further
                                items_of_table.remove(i);
                                removed_items.insert(i.to_vec());
                            }
                            // remember to delete this
                            my_delete
                                .lock()
                                .unwrap()
                                .push(format!("DELETE FROM {} WHERE {}", table.name, t.pred));
                        }
                        TransformArgs::Decor { referencer_col, .. } => {
                            for i in pred_items {
                                // don't decorrelate twice, or decorrelate if removed
                                if removed_items.contains(&i)
                                    || decorrelated_items
                                        .contains(&(referencer_col.clone(), i.clone()))
                                {
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(&i) {
                                    ts.push(t.clone());
                                } else {
                                    items_of_table.insert(i.clone(), vec![t.clone()]);
                                }
                                decorrelated_items.insert((referencer_col.clone(), i.clone()));
                            }
                        }
                        _ => {
                            for i in pred_items {
                                // don't modify if removed
                                if removed_items.contains(&i) {
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(&i) {
                                    ts.push(t.clone());
                                } else {
                                    items_of_table.insert(i, vec![t.clone()]);
                                }
                            }
                        }
                    }
                }
                let mut locked_items = my_items.write().unwrap();
                match locked_items.get_mut(&table.name) {
                    Some(hm) => hm.extend(items_of_table),
                    None => {
                        locked_items.insert(table.name.clone(), items_of_table);
                    }
                }
                drop(locked_items);
            }));
        }

        // wait until all mysql queries are done
        for jh in threads.into_iter() {
            match jh.join() {
                Ok(_) => (),
                Err(_) => warn!("Join failed?"),
            }
        }*/
    }

    pub fn undo(&self, user_id: Option<u64>, disguise_id: u64) -> Result<(), mysql::Error> {
        let de = history::DisguiseEntry {
            disguise_id: disguise_id,
            user_id: match user_id {
                Some(u) => u,
                None => 0,
            },
            reverse: true,
        };

        let mut conn = self.pool.get_conn()?;

        // only reverse if disguise has been applied
        if !history::is_disguise_reversed(&de, &mut conn, self.stats.clone())? {
            // TODO undo disguise

            self.record_disguise(&de, &mut conn)?;
        }
        Ok(())
    }

    pub fn record_disguise(
        &self,
        de: &history::DisguiseEntry,
        conn: &mut mysql::PooledConn,
    ) -> Result<(), mysql::Error> {
        history::insert_disguise_history_entry(de, conn, self.stats.clone());
        warn!("Disguiser: recorded disguise");
        Ok(())
    }

    fn clear_disguise_records(&self) {
        self.to_insert.lock().unwrap().clear();
        self.to_delete.lock().unwrap().clear();
        self.items.write().unwrap().clear();
        warn!("Disguiser: clear disguise records");
    }
}

fn is_guise(id: u64) -> bool {
    id > GUISE_ID_LB
}
