use crate::helpers::*;
use crate::history;
use crate::predicate::*;
use crate::stats::*;
use crate::tokens::*;
use crate::*;
use mysql::{Opts, Pool};
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
    pub pred: Vec<Vec<PredClause>>,
    pub trans: Arc<RwLock<TransformArgs>>,
    pub thirdparty_revealable: bool,
}

#[derive(Clone)]
pub struct TableInfo {
    pub name: String,
    pub id_cols: Vec<String>,
    pub owner_cols: Vec<String>,
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
    pub did: u64,
    pub user: Option<User>,
    pub table_disguises: HashMap<String, Arc<RwLock<Vec<Transform>>>>,
    pub table_info: Arc<RwLock<HashMap<String, TableInfo>>>,
    pub guise_gen: Arc<RwLock<HashMap<String, GuiseGen>>>,
}

pub struct Disguiser {
    pub pool: mysql::Pool,
    pub stats: Arc<Mutex<QueryStat>>,
    pub token_ctrler: Arc<Mutex<TokenCtrler>>,

    // table name to [(rows) -> transformations] map
    items: Arc<RwLock<HashMap<String, HashMap<Vec<RowVal>, Vec<Transform>>>>>,
    remove_tokens_to_modify: Arc<RwLock<HashMap<Token, Vec<Transform>>>>,
    to_delete: Arc<Mutex<Vec<String>>>,
    to_insert: Arc<Mutex<HashMap<(String, Vec<String>), Vec<Vec<Expr>>>>>,
}

impl Disguiser {
    pub fn new(url: &str) -> Disguiser {
        let opts = Opts::from_url(&url).unwrap();
        let pool = Pool::new(opts).unwrap();

        Disguiser {
            pool: pool,
            stats: Arc::new(Mutex::new(stats::QueryStat::new())),
            token_ctrler: Arc::new(Mutex::new(TokenCtrler::new())),
            remove_tokens_to_modify: Arc::new(RwLock::new(HashMap::new())),
            to_insert: Arc::new(Mutex::new(HashMap::new())),
            to_delete: Arc::new(Mutex::new(vec![])),
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_encrypted_symkeys_of_disguises(
        &mut self,
        uid: UID,
        dids: Vec<DID>,
    ) -> Vec<EncListSymKey> {
        let mut encsymkeys = vec![];
        let token_ctrler_locked = self.token_ctrler.lock().unwrap();
        for d in dids {
            if let Some(esk) = token_ctrler_locked.get_encrypted_symkey(uid, d) {
                encsymkeys.push(esk);
            }
        }
        encsymkeys
    }

    pub fn get_tokens_of_disguise_keys(&mut self, keys: HashSet<ListSymKey>) -> Vec<tokens::Token> {
        self.token_ctrler.lock().unwrap().get_tokens(&keys)
    }

    pub fn apply(
        &mut self,
        disguise: Arc<Disguise>,
        tokens: Vec<Token>,
    ) -> Result<(), mysql::Error> {
        let de = history::DisguiseEntry {
            uid: match &disguise.user {
                Some(u) => u.id,
                None => 0,
            },
            did: disguise.did,
            reverse: false,
        };

        let mut conn = self.pool.get_conn()?;
        let mut threads = vec![];

        /*
         * PHASE 1: PREDICATE
         */
        // get all the objects, set all the objects to remove
        // integrate vault_transforms into disguise read + write phases
        self.select_predicate_objs(disguise.clone(), tokens);

        /*
         * PHASE 2: REMOVAL
         */
        self.execute_removes(&mut conn)?;

        /*
         * PHASE 3: UPDATE/DECOR
         */
        for (table, transforms) in disguise.table_disguises.clone() {
            // clone disguiser fields
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items.clone();
            let my_token_ctrler = self.token_ctrler.clone();

            // clone disguise fields
            let my_table_info = disguise.table_info.clone();
            let my_guise_gen = disguise.guise_gen.clone();
            let uid = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let did = disguise.did;

            threads.push(thread::spawn(move || {
                let mut conn = pool.get_conn().unwrap();
                let my_items_locked = my_items.read().unwrap();
                let my_table_info_locked = my_table_info.read().unwrap();
                let curtable_info = my_table_info_locked.get(&table).unwrap();
                let my_items = my_items_locked.get(&table).unwrap();
                let guise_gen_locked = my_guise_gen.read().unwrap();

                warn!(
                    "Thread {:?} starting for table {}",
                    thread::current().id(),
                    table
                );

                // get and apply the transformations for each object
                let mut update_stmts = vec![];
                for (i, ts) in (*my_items).iter() {
                    let mut cols_to_update = vec![];
                    debug!(
                        "Get_Select_Of_Row: Getting ids of table {} for {:?}",
                        table, i
                    );
                    let i_select = get_select_of_row(&curtable_info.id_cols, &i);
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
                                    table
                                );
                                let start = time::Instant::now();

                                /*
                                 * DECOR OBJECT MODIFICATIONS
                                 * A) insert guises for parents
                                 * B) update child to point to new guise
                                 * */

                                // get ID of old parent
                                let old_uid =
                                    u64::from_str(&get_value_of_col(&i, &referencer_col).unwrap())
                                        .unwrap();
                                warn!(
                                    "decor_obj {}: Creating guises for fkids {:?} {:?}",
                                    table, fk_name, old_uid,
                                );

                                // Phase 3A: create new parent
                                let fk_table_info = my_table_info_locked.get(fk_name).unwrap();
                                let fk_gen = guise_gen_locked.get(fk_name).unwrap();
                                let new_parent_vals = (fk_gen.val_generation)();
                                let new_parent_cols = (fk_gen.col_generation)();
                                let mut ix = 0;
                                let new_parent_rowvals: Vec<RowVal> = new_parent_vals
                                    .iter()
                                    .map(|v| {
                                        let index = ix;
                                        ix += 1;
                                        RowVal {
                                            column: new_parent_cols[index].to_string(),
                                            value: v.to_string(),
                                        }
                                    })
                                    .collect();
                                let new_parent_ids =
                                    get_ids(&fk_table_info.id_cols, &new_parent_rowvals);
                                let guise_id = new_parent_ids[0].value.to_string();
                                warn!("decor_obj: inserted guise {}.{}", fk_name, guise_id);

                                // save guise to insert
                                let mut locked_insert = my_insert.lock().unwrap();
                                if let Some(vals) = locked_insert
                                    .get_mut(&(fk_name.to_string(), new_parent_cols.clone()))
                                {
                                    vals.push(new_parent_vals.clone());
                                } else {
                                    locked_insert.insert(
                                        (fk_name.clone(), new_parent_cols),
                                        vec![new_parent_vals.clone()],
                                    );
                                }
                                drop(locked_insert);

                                // save insert token
                                let mut insert_token = Token::new_insert_token(
                                    did,
                                    uid,
                                    fk_name.clone(),
                                    new_parent_ids.clone(),
                                    table.clone(),
                                    new_parent_rowvals.clone(),
                                );
                                let mut token_ctrler_lked = my_token_ctrler.lock().unwrap();
                                token_ctrler_lked
                                    .insert_user_token(TokenType::Data, &mut insert_token);
                                drop(token_ctrler_lked);

                                // Phase 3B: update child guise
                                cols_to_update.push(Assignment {
                                    id: Ident::new(referencer_col.clone()),
                                    value: Expr::Value(Value::Number(guise_id.to_string())),
                                });

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
                                let child_ids = get_ids(&curtable_info.id_cols, &new_child);
                                let mut decor_token = Token::new_decor_token(
                                    did, uid, table.clone(), child_ids, fk_name.clone(), i.clone(), new_child);
                                let mut token_ctrler_lked = my_token_ctrler.lock().unwrap();
                                token_ctrler_lked
                                    .insert_user_token(TokenType::Data, &mut decor_token);
                                drop(token_ctrler_lked);

                                let mut locked_stats = mystats.lock().unwrap();
                                locked_stats.decor_dur += start.elapsed();
                                drop(locked_stats);
                                warn!("Thread {:?} decor {}", thread::current().id(), table);
                            }

                            TransformArgs::Modify {
                                col,
                                generate_modified_value,
                                ..
                            } => {
                                warn!("Thread {:?} starting mod {}", thread::current().id(), table);
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
                                if !t.thirdparty_revealable {
                                    warn!("Modify: Getting ids of table {} for {:?}", table, i);
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
                                    let ids = get_ids(&curtable_info.id_cols, &i);
                                    // TODO
                                }

                                let mut locked_stats = mystats.lock().unwrap();
                                locked_stats.mod_dur += start.elapsed();
                                drop(locked_stats);
                                warn!("Thread {:?} modify {}", thread::current().id(), table);
                            }
                            _ => unimplemented!("Removes should already have been performed!"),
                        }
                    }

                    // updates are per-item
                    if !cols_to_update.is_empty() {
                        update_stmts.push(
                            Statement::Update(UpdateStatement {
                                table_name: string_to_objname(&table),
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
        for ((table, cols), vals) in locked_insert.iter() {
            query_drop(
                Statement::Insert(InsertStatement {
                    table_name: string_to_objname(&table),
                    columns: cols.iter().map(|c| Ident::new(c.to_string())).collect(),
                    source: InsertSource::Query(Box::new(values_query(vals.clone()))),
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

        self.clear_disguise_records();
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

    fn select_predicate_objs(&self, disguise: Arc<Disguise>, tokens: Vec<Token>) {
        let mut threads = vec![];
        for (table, transforms) in disguise.table_disguises.clone() {
            let my_table_info = disguise.table_info.clone();
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_delete = self.to_delete.clone();
            let did = disguise.did;
            let my_items = self.items.clone();
            let my_remove_tokens = self.remove_tokens_to_modify.clone();
            let my_tokens = tokens.clone();
            let my_token_ctrler = self.token_ctrler.clone();

            // hashmap from item value --> transform
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Transform>> = HashMap::new();
            let mut matching_remove_tokens: HashMap<Token, Vec<Transform>> = HashMap::new();

            threads.push(thread::spawn(move || {
                let mut conn = pool.get_conn().unwrap();
                let mut removed_items: HashSet<Vec<RowVal>> = HashSet::new();
                let mut decorrelated_items: HashSet<(String, Vec<RowVal>)> = HashSet::new();
                let my_transforms = transforms.read().unwrap();
                let my_table_info_lked = my_table_info.read().unwrap();
                let curtable_info = my_table_info_lked.get(&table).unwrap().clone();
                drop(my_table_info_lked);

                // get transformations in order of disguise application
                // (so vault transforms are always first)
                for t in &*my_transforms {
                    let selection = predicate::pred_to_sql_where(&t.pred);
                    let mut pred_items = get_query_rows_str(
                        &str_select_statement(&table, &selection),
                        &mut conn,
                        mystats.clone(),
                    )
                    .unwrap();

                    // TOKENS:
                    // get tokens that match the predicate
                    let pred_tokens = predicate::get_tokens_matching_pred(&t.pred, &my_tokens);

                    // move any tokens in global vault to user vault if this
                    // transformation is only 1p-revealable
                    if !t.thirdparty_revealable {
                        let mut token_ctrler_locked = my_token_ctrler.lock().unwrap();
                        token_ctrler_locked.move_global_tokens_to_user_vault(&pred_tokens);
                        drop(token_ctrler_locked);
                    }

                    // for tokens that decorrelated or updated a guise, we want to add the new
                    // value that should be transformed into the set of predicated items
                    //
                    // if the token records a removed item, we want to update the stored value of
                    // this token so that we only even restore the most up-to-date disguised data
                    // XXX note: during reversal, we'll have to reverse this token update
                    for pt in &pred_tokens {
                        match pt.update_type {
                            DECOR_GUISE | UPDATE_GUISE => {
                                pred_items.push(pt.new_value.clone());
                            }
                            REMOVE_GUISE => {
                                matching_remove_tokens.insert(pt.clone(), vec![]);
                            }
                            _ => (),
                        }
                    }

                    // just remove item if it's supposed to be removed
                    match &*t.trans.read().unwrap() {
                        TransformArgs::Remove => {
                            for i in &pred_items {
                                // don't remove an item that's already removed
                                if removed_items.contains(i) {
                                    continue;
                                }
                                debug!("Remove: Getting ids of table {} for {:?}", table, i);
                                let ids = get_ids(&curtable_info.id_cols, i);

                                // TOKENS: create remove token and insert into either global or user vault
                                let mut token = Token::new_delete_token(
                                    did,
                                    0,
                                    table.clone(),
                                    ids.clone(),
                                    i.clone(),
                                );
                                for owner_col in &curtable_info.owner_cols {
                                    let uid = get_value_of_col(&i, &owner_col).unwrap();
                                    token.uid = u64::from_str(&uid).unwrap();
                                    let mut token_ctrler_locked = my_token_ctrler.lock().unwrap();
                                    if t.thirdparty_revealable {
                                        token_ctrler_locked.insert_global_token(&mut token);
                                    } else {
                                        token_ctrler_locked
                                            .insert_user_token(TokenType::Data, &mut token);
                                    }
                                    drop(token_ctrler_locked);
                                }

                                // EXTRA: save in removed so we don't transform this item further
                                items_of_table.remove(i);
                                removed_items.insert(i.to_vec());
                            }
                            // remember to delete the item
                            my_delete
                                .lock()
                                .unwrap()
                                .push(format!("DELETE FROM {} WHERE {}", table, selection));
                        }
                        TransformArgs::Decor { referencer_col, .. } => {
                            for i in pred_items {
                                // don't decorrelate if removed
                                if removed_items.contains(&i) {
                                    // remove if we'd accidentally added it before
                                    items_of_table.remove(&i);
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items.contains(&(referencer_col.clone(), i.clone()))
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

                            // TOKENS we need to modify any matching removed data tokens from prior
                            // disguises
                            let matching_tokens: Vec<Token> =
                                matching_remove_tokens.keys().map(|k| k.clone()).collect();
                            for token in matching_tokens {
                                // don't decorrelate token if removed
                                if removed_items.contains(&token.old_value) {
                                    matching_remove_tokens.remove(&token);
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items
                                    .contains(&(referencer_col.clone(), token.old_value.clone()))
                                {
                                    continue;
                                }
                                // save the transformation to perform on this token
                                let ts = matching_remove_tokens.get_mut(&token).unwrap();
                                ts.push(t.clone());
                                decorrelated_items
                                    .insert((referencer_col.clone(), token.old_value.clone()));
                            }
                        }
                        _ => {
                            for i in pred_items {
                                // don't modify if removed
                                if removed_items.contains(&i) {
                                    items_of_table.remove(&i);
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(&i) {
                                    ts.push(t.clone());
                                } else {
                                    items_of_table.insert(i, vec![t.clone()]);
                                }
                            }

                            // TOKENS we need to modify any matching removed data tokens from prior
                            // disguises
                            let matching_tokens: Vec<Token> =
                                matching_remove_tokens.keys().map(|k| k.clone()).collect();
                            for token in matching_tokens {
                                // don't modify decorrelate token if removed
                                if removed_items.contains(&token.old_value) {
                                    matching_remove_tokens.remove(&token);
                                    continue;
                                }
                                // save the transformation to perform on this token
                                let ts = matching_remove_tokens.get_mut(&token).unwrap();
                                ts.push(t.clone());
                            }
                        }
                    }
                }
                let mut locked_items = my_items.write().unwrap();
                match locked_items.get_mut(&table) {
                    Some(hm) => hm.extend(items_of_table),
                    None => {
                        locked_items.insert(table.clone(), items_of_table);
                    }
                }
                drop(locked_items);
                let mut locked_tokens = my_remove_tokens.write().unwrap();
                locked_tokens.extend(matching_remove_tokens);
                drop(locked_tokens);
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

    pub fn reverse(&self, disguise: Arc<Disguise>, tokens: Vec<Token>) -> Result<(), mysql::Error> {
        let de = history::DisguiseEntry {
            uid: match &disguise.user {
                Some(u) => u.id,
                None => 0,
            },
            did: disguise.did,
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
