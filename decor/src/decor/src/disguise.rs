use crate::helpers::*;
use crate::history;
use crate::predicate::*;
use crate::stats::*;
use crate::tokens::*;
use crate::*;
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
    pub pred: Vec<Vec<PredClause>>,
    pub trans: Arc<RwLock<TransformArgs>>,
    pub thirdparty_revealable: bool,
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
    pub did: u64,
    pub user: Option<User>,
    pub table_disguises: HashMap<String, Arc<RwLock<TableDisguise>>>,
    pub guise_gen: HashMap<String, Arc<RwLock<GuiseGen>>>,
}

pub struct Disguiser {
    pub pool: mysql::Pool,
    pub stats: Arc<Mutex<QueryStat>>,
    pub token_ctrler: TokenCtrler,
    // table name to [(rows) -> transformations] map
    items: Arc<RwLock<HashMap<String, HashMap<Vec<RowVal>, Vec<Transform>>>>>,
    remove_tokens_to_modify: Arc<RwLock<HashMap<Token, Vec<Transform>>>>,
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
            token_ctrler: TokenCtrler::new(),
            remove_tokens_to_modify: Arc::new(RwLock::new(HashMap::new())),
            to_insert: Arc::new(Mutex::new(vec![])),
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
        for d in dids {
            if let Some(esk) = self.token_ctrler.get_encrypted_symkey(uid, d) {
                encsymkeys.push(esk);
            }
        }
        encsymkeys
    }

    pub fn get_tokens_of_disguise_keys(&mut self, keys: HashSet<ListSymKey>) -> Vec<tokens::Token> {
        self.token_ctrler.get_tokens(&keys)
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
        //let mut threads = vec![];

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
        /*let fk_cols = Arc::new((disguise.guise_info.read().unwrap().col_generation)());
        for (_, table_disguise) in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items.clone();
            let my_fkcols = fk_cols.clone();

            let uid = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let did = disguise.did;
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
                                if !t.thirdparty_revealable {
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
            let should_insert = uid == 0 || uid == uid64;
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

    fn select_predicate_objs(&self, disguise: Arc<Disguise>, tokens: Vec<Token>) {
        let mut threads = vec![];
        for (table, table_disguise) in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_delete = self.to_delete.clone();
            let did = disguise.did;
            let uid = match &disguise.user {
                Some(u) => u.id,
                None => 0,
            };
            let my_items = self.items.clone();
            let my_remove_tokens = self.remove_tokens_to_modify.clone();

            // hashmap from item value --> transform
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Transform>> = HashMap::new();
            let mut matching_remove_tokens: HashMap<Token, Vec<Transform>>;

            threads.push(thread::spawn(move || {
                let td = table_disguise.read().unwrap();
                let mut conn = pool.get_conn().unwrap();
                let mut removed_items: HashSet<Vec<RowVal>> = HashSet::new();
                let mut decorrelated_items: HashSet<(String, Vec<RowVal>)> = HashSet::new();

                // get transformations in order of disguise application
                // (so vault transforms are always first)
                for t in td.transforms {
                    let selection = predicate::pred_to_sql_where(t.pred);
                    let pred_items = get_query_rows_str(
                        &str_select_statement(&td.name, &selection),
                        &mut conn,
                        mystats.clone(),
                    )
                    .unwrap();

                    // TOKENS:
                    // get tokens that match the predicate
                    let pred_tokens = predicate::get_tokens_matching_pred(t.pred, tokens);

                    // move any tokens in global vault to user vault if this
                    // transformation is only 1p-revealable
                    if !t.thirdparty_revealable {
                        self.token_ctrler
                            .move_global_tokens_to_user_vault(pred_tokens);
                    }

                    // for tokens that decorrelated or updated a guise, we want to add the new
                    // value that should be transformed into the set of predicated items
                    //
                    // if the token records a removed item, we want to update the stored value of
                    // this token so that we only even restore the most up-to-date disguised data
                    // XXX note: during reversal, we'll have to reverse this token update
                    for pt in pred_tokens {
                        match pt.update_type {
                            DECOR_GUISE | UPDATE_GUISE => {
                                pred_items.push(pt.new_value);
                            }
                            REMOVE_GUISE => {
                                matching_remove_tokens.insert(pt, vec![]);
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
                                debug!("Remove: Getting ids of table {} for {:?}", td.name, i);
                                let ids = get_ids(&td.id_cols, i);

                                // TOKENS: create remove token and insert into either global or user vault
                                let mut token = Token::new_delete_token(
                                    did,
                                    0,
                                    td.name.clone(),
                                    ids.clone(),
                                    i.clone(),
                                );
                                for owner_col in &td.owner_cols {
                                    let uid = get_value_of_col(&i, &owner_col).unwrap();
                                    token.uid = u64::from_str(&uid).unwrap();
                                    if t.thirdparty_revealable {
                                        self.token_ctrler.insert_global_token(&mut token);
                                    } else {
                                        self.token_ctrler
                                            .insert_user_token(TokenType::Data, &mut token);
                                    }
                                }

                                // EXTRA: save in removed so we don't transform this item further
                                items_of_table.remove(i);
                                removed_items.insert(i.to_vec());
                            }
                            // remember to delete the item
                            my_delete
                                .lock()
                                .unwrap()
                                .push(format!("DELETE FROM {} WHERE {}", td.name, selection));
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
                                if decorrelated_items
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

                            for (token, ts) in matching_remove_tokens.iter() {
                                // don't decorrelate token if removed
                                if removed_items.contains(&token.old_value) {
                                    matching_remove_tokens.remove(token);
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items
                                        .contains(&(referencer_col.clone(), token.old_value))
                                {
                                    continue;
                                }
                                // save the transformation to perform on this token
                                ts.push(t.clone());
                                decorrelated_items.insert((referencer_col.clone(), token.old_value.clone()));
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
                            for (token, ts) in matching_remove_tokens.iter() {
                                // don'modify decorrelate token if removed
                                if removed_items.contains(&token.old_value) {
                                    matching_remove_tokens.remove(token);
                                    continue;
                                }
                                // save the transformation to perform on this token
                                ts.push(t.clone());
                            }
                        }
                    }
                }
                let mut locked_items = my_items.write().unwrap();
                match locked_items.get_mut(&td.name) {
                    Some(hm) => hm.extend(items_of_table),
                    None => {
                        locked_items.insert(td.name.clone(), items_of_table);
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
