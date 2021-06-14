use crate::helpers::*;
use crate::stats::*;
use crate::{vaults, history};
use crate::*;
use mysql::{Opts, Pool};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformType {
    Remove, 
    Modify,
    Decor,
}

pub type Predicate = Option<Expr>;

pub enum Transform {
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

pub struct TableDisguise {
    pub name: String,
    pub id_cols: Vec<String>,
    pub owner_cols: Vec<String>,
    pub transforms: Vec<(Predicate, Arc<RwLock<Transform>>)>,
}

pub struct GuiseInfo {
    pub name: String,
    pub id_col: String, // XXX assume there's only one id col for a guise
    pub col_generation: Box<dyn Fn() -> Vec<String> + Send + Sync>,
    pub val_generation: Box<dyn Fn() -> Vec<Expr> + Send + Sync>,
    pub referencers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: u64,
    pub nonce: Vec<u8>,
    pub key: Vec<u8>,
}

pub struct Disguise {
    pub disguise_id: u64,
    pub table_disguises: Vec<Arc<RwLock<TableDisguise>>>,
    pub user: Option<User>, 
    // used to generate new guises
    pub guise_info: Arc<RwLock<GuiseInfo>>,
    pub is_reversible: bool,
}
pub struct Disguiser {
    pub pool: mysql::Pool,
    pub stats: Arc<Mutex<QueryStat>>,
    vault_vals: Arc<Mutex<HashMap<u64, Vec<vaults::VaultEntry>>>>,
    items: Arc<RwLock<HashMap<String, HashMap<Vec<RowVal>, Vec<Arc<RwLock<Transform>>>>>>>,
    to_delete: Arc<Mutex<Vec<String>>>,
    to_insert: Arc<Mutex<Vec<Vec<Expr>>>>,
    uvclient: vaults::UVClient,
}

impl Disguiser {
    pub fn new(url: &str) -> Disguiser {
        let opts = Opts::from_url(&url).unwrap();
        let pool = Pool::new(opts).unwrap();

        Disguiser {
            pool: pool,
            stats: Arc::new(Mutex::new(stats::QueryStat::new())),
            vault_vals: Arc::new(Mutex::new(HashMap::new())),
            to_insert: Arc::new(Mutex::new(vec![])),
            to_delete: Arc::new(Mutex::new(vec![])),
            items: Arc::new(RwLock::new(HashMap::new())),
            uvclient: vaults::UVClient::new(crate::BUCKET, crate::REGION),
        }
    }

    fn execute_removes(&self, conn: &mut mysql::PooledConn) -> Result<(), mysql::Error> {
        let start = time::Instant::now();
        for stmt in &*self.to_delete.lock().unwrap() {
            helpers::query_drop(stmt.to_string(), conn, self.stats.clone())?;
        }
        self.stats.lock().unwrap().remove_dur += start.elapsed();
        Ok(())
    }

    pub fn select_predicate_objs(&self, disguise: Arc<Disguise>) {
        let mut threads = vec![];
        for table in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let myvv = self.vault_vals.clone();
            let my_delete = self.to_delete.clone();
            let is_reversible = disguise.is_reversible;
            let disguise_id = disguise.disguise_id;
            let user_id = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let my_items = self.items.clone();
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Arc<RwLock<Transform>>>> =
                HashMap::new();

            threads.push(thread::spawn(move || {
                let table = table.read().unwrap();
                let mut conn = pool.get_conn().unwrap();
                let mut removed_items: HashSet<Vec<RowVal>> = HashSet::new();

                /*
                 * PHASE 1: OBJECT SELECTION
                 * Assign each object its assigned transformations
                 */
                for (pred, transform) in &table.transforms {
                    let pred_items = get_query_rows(
                        &select_statement(&table.name, &pred),
                        &mut conn,
                        mystats.clone(),
                    )
                    .unwrap();

                    // just remove item if it's supposed to be removed
                    match *transform.read().unwrap() {
                        Transform::Remove => {
                            // we're going to remove these, remember in vault
                            for i in &pred_items {
                                if is_reversible {
                                    debug!(
                                        "Remove: Getting ids of table {} for {:?}",
                                        table.name, i
                                    );
                                    let ids = get_ids(&table.id_cols, i);
                                    for owner_col in &table.owner_cols {
                                        let uid = get_value_of_col(&i, &owner_col).unwrap();
                                        let uid64 = u64::from_str(&uid).unwrap();
                                        let should_insert = user_id == uid64 || user_id == 0;
                                        if should_insert {     
                                            let ve = vaults::VaultEntry {
                                                vault_id: 0,
                                                disguise_id: disguise_id,
                                                user_id: uid64, 
                                                guise_name: table.name.clone(),
                                                guise_id_cols: table.id_cols.clone(),
                                                guise_ids: ids.clone(),
                                                referencer_name: "".to_string(),
                                                update_type: vaults::DELETE_GUISE,
                                                modified_cols: vec![],
                                                old_value: i.clone(),
                                                new_value: vec![],
                                                reverses: None,
                                            };
                                            let mut myvv_locked = myvv.lock().unwrap();
                                            match myvv_locked.get_mut(&uid64) {
                                                Some(vs) => vs.push(ve),
                                                None => {myvv_locked.insert(uid64, vec![ve]);
                                                }                                           
                                            }
                                        }
                                    }
                                }
                                // EXTRA: save in removed so we don't transform this item further
                                items_of_table.remove(i);
                                removed_items.insert(i.to_vec());
                            }
                            // remember to delete this
                            my_delete.lock().unwrap().push(
                                Statement::Delete(DeleteStatement {
                                    table_name: string_to_objname(&table.name),
                                    selection: pred.clone(),
                                })
                                .to_string(),
                            );
                        }
                        _ => {
                            for i in pred_items {
                                if removed_items.contains(&i) {
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(&i) {
                                    ts.push(transform.clone());
                                } else {
                                    items_of_table.insert(i, vec![transform.clone()]);
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
        let mut threads = vec![];

        /*//PHASE 0: REVERSE ANY PRIOR DECORRELATED ENTRIES
        for (ref_table, ref_col) in &disguise.guise_info.referencers {
            if ref_table == &table.name {
                vaults::reverse_vault_decor_referencer_entries(
                    user_id,
                    &ref_table,
                    &ref_col,
                    // assume just one id col for user
                    &table.name,
                    &table.id_cols[0],
                    txn,
                    stats,
                )?;
             }
        }
        */

        // get all the objects, set all the objects to remove
        self.select_predicate_objs(disguise.clone());

        // remove all the objects
        self.execute_removes(&mut conn)?;

        // actually go and perform modifications now
        let fk_cols = Arc::new((disguise.guise_info.read().unwrap().col_generation)());
        for table in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let myvv = self.vault_vals.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items.clone();
            let my_fkcols = fk_cols.clone();

            let is_reversible = disguise.is_reversible;
            let user_id = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let disguise_id = disguise.disguise_id;
            let guise_info = disguise.guise_info.clone();

            threads.push(thread::spawn(move || {
                let guise_info = guise_info.read().unwrap();
                let table = table.read().unwrap();
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
                        match &*t.read().unwrap() {
                            Transform::Decor {
                                referencer_col,
                                fk_name,
                                ..
                            } => {
                                warn!(
                                    "Thread {:?} starting decor {}",
                                    thread::current().id(),
                                    table.name
                                );
                                let start = time::Instant::now();

                                /*
                                 * PHASE 2: OBJECT MODIFICATIONS
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

                                // Phase 2A: create new parent
                                let new_parent = (*guise_info.val_generation)();
                                let guise_id = new_parent[0].to_string();
                                warn!("decor_obj: inserted guise {}.{}", fk_name, guise_id);
                                let mut locked_insert = my_insert.lock().unwrap();
                                locked_insert.push(new_parent.clone());
                                drop(locked_insert);

                                // Phase 2B: update child guise
                                cols_to_update.push(Assignment {
                                    id: Ident::new(referencer_col.clone()),
                                    value: Expr::Value(Value::Number(guise_id.to_string())),
                                });

                                /*
                                 * PHASE 3: VAULT UPDATES
                                 * A) insert guises, associate with old parent uid
                                 * B) record update to child to point to new guise
                                 * */
                                // Phase 3A: update the vault with new guise (calculating the uid from the last_insert_id)
                                if is_reversible {
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
                                    let mut myvv_locked = myvv.lock().unwrap();
                                    let ve = vaults::VaultEntry {
                                        vault_id: 0,
                                        disguise_id: disguise_id,
                                        user_id: old_uid,
                                        guise_name: fk_name.clone(),
                                        guise_id_cols: vec![guise_info.id_col.clone()],
                                        guise_ids: vec![guise_id.to_string()],
                                        referencer_name: table.name.clone(),
                                        update_type: vaults::INSERT_GUISE,
                                        modified_cols: vec![],
                                        old_value: vec![],
                                        new_value: new_parent_rowvals,
                                        reverses: None,
                                    };
                                    match myvv_locked.get_mut(&old_uid) {
                                        Some(vs) => vs.push(ve),
                                        None => {
                                            myvv_locked.insert(old_uid, vec![ve]);
                                        }
                                    }

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
                                    warn!("Decor: Getting ids of table {} for {:?}", table.name, i);
                                    let ve = vaults::VaultEntry {
                                        vault_id: 0,
                                        disguise_id: disguise_id,
                                        user_id: old_uid,
                                        guise_name: table.name.clone(),
                                        guise_id_cols: table.id_cols.clone(),
                                        guise_ids: get_ids(&table.id_cols, &i),
                                        referencer_name: "".to_string(),
                                        update_type: vaults::UPDATE_GUISE,
                                        modified_cols: vec![referencer_col.clone()],
                                        old_value: i.clone(),
                                        new_value: new_child,
                                        reverses: None,
                                    };
                                    match myvv_locked.get_mut(&old_uid) {
                                        Some(vs) => vs.push(ve),
                                        None => {myvv_locked.insert(old_uid, vec![ve]);}
                                    }
                                    drop(myvv_locked);
                                }

                                let mut locked_stats = mystats.lock().unwrap();
                                locked_stats.decor_dur += start.elapsed();
                                drop(locked_stats);
                                warn!("Thread {:?} decor {}", thread::current().id(), table.name);
                            }

                            Transform::Modify {
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
                                if is_reversible {
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
                                    let mut myvv_locked = myvv.lock().unwrap();
                                    for owner_col in &table.owner_cols {
                                        let uid = get_value_of_col(&i, &owner_col).unwrap();
                                        let uid64 = u64::from_str(&uid).unwrap();
                                        let should_insert = user_id == 0 || user_id == uid64;
                                        if should_insert {     
                                            let ve = vaults::VaultEntry {
                                                vault_id: 0,
                                                disguise_id: disguise_id,
                                                user_id: u64::from_str(&uid).unwrap(),
                                                guise_name: table.name.clone(),
                                                guise_id_cols: table.id_cols.clone(),
                                                guise_ids: ids.clone(),
                                                referencer_name: "".to_string(),
                                                update_type: vaults::UPDATE_GUISE,
                                                modified_cols: vec![col.clone()],
                                                old_value: i.clone(),
                                                new_value: new_obj.clone(),
                                                reverses: None,
                                            };
                                            match myvv_locked.get_mut(&uid64) {
                                                Some(vs) => vs.push(ve),
                                                None => {myvv_locked.insert(uid64, vec![ve]);}
                                            }
                                        }
                                    }
                                    drop(myvv_locked);
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
       
        let locked_vv = self.vault_vals.lock().unwrap();
        if let Some(u) = &disguise.user {
            // TODO when to insert into global, and when to insert into local?
            let vs = locked_vv.get(&u.id).unwrap();
            self.uvclient.insert_user_ves(&u.key, &u.nonce, &vs);
        } else {
            vaults::insert_global_ves(&locked_vv, &mut conn, self.stats.clone());
        }
        drop(locked_vv);
        warn!("Disguiser: Inserted Vault Entries");

        self.record_disguise(&de, &mut conn)?;

        self.clear_disguise_records();
        Ok(())
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
        self.vault_vals.lock().unwrap().clear();
        self.items.write().unwrap().clear();
        warn!("Disguiser: clear disguise records");
    }
}

fn is_guise(id: u64) -> bool {
    id > GUISE_ID_LB
}
