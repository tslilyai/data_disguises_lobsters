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
        fk_col: String,
        fk_name: String,
    },
}

#[derive(Clone)]
pub struct Transform {
    pub pred: Vec<Vec<PredClause>>,
    pub trans: Arc<RwLock<TransformArgs>>,
    pub global: bool,
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
    tokens_to_modify: Arc<RwLock<HashMap<Token, Vec<Transform>>>>,
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
            tokens_to_modify: Arc::new(RwLock::new(HashMap::new())),
            to_insert: Arc::new(Mutex::new(HashMap::new())),
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_encrypted_symkeys_of_disguises(
        &mut self,
        uid: UID,
        dids: Vec<DID>,
    ) -> Vec<EncListSymKey> {
        let mut encsymkeys = vec![];
        let locked_token_ctrler = self.token_ctrler.lock().unwrap();
        for d in dids {
            if let Some(esk) = locked_token_ctrler.get_encrypted_symkey(uid, d) {
                encsymkeys.push(esk);
            }
        }
        encsymkeys
    }

    pub fn get_tokens_of_disguise_keys(
        &mut self,
        keys: HashSet<ListSymKey>,
        for_disguise_action: bool,
    ) -> Vec<tokens::Token> {
        self.token_ctrler
            .lock()
            .unwrap()
            .get_tokens(&keys, for_disguise_action)
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
        let uid = match &disguise.user {
            Some(u) => u.id,
            None => 0,
        };
        let did = disguise.did;

        /*
         * PREDICATE AND REMOVE
         */
        // get all the objects, remove the objects to remove
        // integrate vault_transforms into disguise read + write phases
        self.select_predicate_objs_and_execute_removes(disguise.clone(), tokens);

        /*
         * UPDATE/DECOR
         */
        for (table, _) in disguise.table_disguises.clone() {
            // clone disguiser fields
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items.clone();
            let my_token_ctrler = self.token_ctrler.clone();

            // clone disguise fields
            let my_table_info = disguise.table_info.clone();
            let my_guise_gen = disguise.guise_gen.clone();

            threads.push(thread::spawn(move || {
                let mut conn = pool.get_conn().unwrap();
                let locked_items = my_items.read().unwrap();
                let locked_table_info = my_table_info.read().unwrap();
                let curtable_info = locked_table_info.get(&table).unwrap();
                let table_items = locked_items.get(&table).unwrap();
                let locked_guise_gen = my_guise_gen.read().unwrap();

                warn!(
                    "Thread {:?} starting for table {}",
                    thread::current().id(),
                    table
                );

                // get and apply the transformations for each object
                let mut update_stmts = vec![];
                for (i, ts) in (*table_items).iter() {
                    let mut cols_to_update = vec![];
                    debug!(
                        "Get_Select_Of_Row: Getting ids of table {} for {:?}",
                        table, i
                    );
                    let i_select = get_select_of_row(&curtable_info.id_cols, &i);
                    for t in ts {
                        match &*t.trans.read().unwrap() {
                            TransformArgs::Decor { fk_col, fk_name } => {
                                let fk_table_info = locked_table_info.get(fk_name).unwrap();
                                let locked_fk_gen = locked_guise_gen.get(fk_name).unwrap();
                                let mut locked_insert = my_insert.lock().unwrap();
                                let mut locked_token_ctrler = my_token_ctrler.lock().unwrap();
                                let mut locked_stats = mystats.lock().unwrap();

                                decor_item(
                                    // disguise and per-thread state
                                    did,
                                    uid,
                                    t.global,
                                    &mut locked_insert,
                                    &mut locked_token_ctrler,
                                    &mut cols_to_update,
                                    &mut locked_stats,
                                    // info needed for decorrelation
                                    &table,
                                    curtable_info,
                                    fk_name,
                                    fk_col,
                                    fk_table_info,
                                    locked_fk_gen,
                                    i,
                                );
                            }

                            TransformArgs::Modify {
                                col,
                                generate_modified_value,
                                ..
                            } => {
                                let mut locked_token_ctrler = my_token_ctrler.lock().unwrap();
                                let mut locked_stats = mystats.lock().unwrap();
                                let old_val = get_value_of_col(&i, &col).unwrap();

                                modify_item(
                                    did,
                                    uid,
                                    t.global,
                                    &mut locked_token_ctrler,
                                    &mut cols_to_update,
                                    &mut locked_stats,
                                    &table,
                                    curtable_info,
                                    col,
                                    (*(generate_modified_value))(&old_val),
                                    i,
                                );
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
                // actually execute all updates
                warn!("Thread {:?} updating", thread::current().id());
                for stmt in update_stmts {
                    query_drop(stmt, &mut conn, mystats.clone()).unwrap();
                }
                warn!("Thread {:?} exiting", thread::current().id());
            }));
        }

        // apply updates to each token (for now do sequentially)
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        for (token, ts) in self.tokens_to_modify.write().unwrap().iter() {
            for t in ts {
                // don't apply updates if disguise is global (and token can be global)
                if t.global {
                    continue;
                }
                let mut cols_to_update = vec![];
                match &*t.trans.read().unwrap() {
                    TransformArgs::Decor { fk_col, fk_name } => {
                        let locked_table_info = disguise.table_info.read().unwrap();
                        let locked_guise_gen = disguise.guise_gen.read().unwrap();

                        decor_item(
                            // disguise and per-thread state
                            did,
                            uid,
                            t.global,
                            &mut self.to_insert.lock().unwrap(),
                            &mut locked_token_ctrler,
                            &mut cols_to_update,
                            &mut self.stats.lock().unwrap(),
                            // info needed for decorrelation
                            &token.guise_name,
                            locked_table_info.get(&token.guise_name).unwrap(),
                            fk_name,
                            fk_col,
                            locked_table_info.get(fk_name).unwrap(),
                            locked_guise_gen.get(fk_name).unwrap(),
                            &token.old_value,
                        );
                    }
                    TransformArgs::Modify {
                        col,
                        generate_modified_value,
                        ..
                    } => {
                        let locked_table_info = disguise.table_info.read().unwrap();
                        let old_val = get_value_of_col(&token.old_value, &col).unwrap();

                        modify_item(
                            did,
                            uid,
                            t.global,
                            &mut locked_token_ctrler,
                            &mut cols_to_update,
                            &mut self.stats.lock().unwrap(),
                            &token.guise_name,
                            locked_table_info.get(&token.guise_name).unwrap(),
                            col,
                            (*(generate_modified_value))(&old_val),
                            &token.old_value,
                        );
                    }
                    TransformArgs::Remove => {
                        // remove token from vault (if global)
                        if token.is_global {
                            if !locked_token_ctrler.remove_token(uid, did, &token) {
                                warn!("Could not remove old disguise token!! {:?}", token);
                            }
                        }
                    }
                }
                // apply cols_to_update
                if token.is_global {
                    let mut new_token = token.clone();
                    new_token.old_value = token
                        .old_value
                        .iter()
                        .map(|rv| {
                            let mut new_rv = rv.clone();
                            for a in &cols_to_update {
                                if rv.column == a.id.to_string() {
                                    new_rv = RowVal {
                                        column: rv.column.clone(),
                                        value: a.value.to_string(),
                                    };
                                }
                            }
                            new_rv
                        })
                        .collect();
                    if !locked_token_ctrler.update_token_from_old_to(uid, did, &token, &new_token) {
                        warn!("Could not update old disguise token!! {:?}", token);
                    }
                }
            }
        }
        drop(locked_token_ctrler);

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

        self.record_disguise(&de, &mut conn)?;

        self.clear_disguise_records();
        Ok(())
    }

    fn select_predicate_objs_and_execute_removes(
        &self,
        disguise: Arc<Disguise>,
        tokens: Vec<Token>,
    ) {
        let mut threads = vec![];
        for (table, transforms) in disguise.table_disguises.clone() {
            let my_table_info = disguise.table_info.clone();
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let did = disguise.did;
            let my_items = self.items.clone();
            let my_tokens_to_modify = self.tokens_to_modify.clone();
            let my_tokens = tokens.clone();
            let my_token_ctrler = self.token_ctrler.clone();

            // hashmap from item value --> transform
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Transform>> = HashMap::new();
            let mut token_transforms: HashMap<Token, Vec<Transform>> = HashMap::new();

            threads.push(thread::spawn(move || {
                let mut conn = pool.get_conn().unwrap();
                let mut removed_items: HashSet<Vec<RowVal>> = HashSet::new();
                let mut decorrelated_items: HashSet<(String, Vec<RowVal>)> = HashSet::new();
                let my_transforms = transforms.read().unwrap();
                let locked_table_info = my_table_info.read().unwrap();
                let curtable_info = locked_table_info.get(&table).unwrap().clone();
                drop(locked_table_info);

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

                    // TOKENS RETRIEVAL: get tokens that match the predicate
                    let pred_tokens = predicate::get_tokens_matching_pred(&t.pred, &my_tokens);
                    for pt in &pred_tokens {
                        // for tokens that decorrelated or updated a guise, we want to add the new
                        // value that should be transformed into the set of predicated items
                        match pt.update_type {
                            DECOR_GUISE | MODIFY_GUISE => {
                                pred_items.push(pt.new_value.clone());
                            }
                            _ => (),
                        }
                        // for all global tokens, we want to update the
                        // stored value of this token so that we only ever restore the most
                        // up-to-date disguised data and the token doesn't leak any data
                        // NOTE: during reversal, we'll have to reverse this token update
                        if pt.is_global {
                            token_transforms.insert(pt.clone(), vec![]);
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

                                // TOKEN INSERT
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
                                    let mut locked_token_ctrler = my_token_ctrler.lock().unwrap();
                                    if t.global {
                                        locked_token_ctrler.insert_global_token(&mut token);
                                    } else {
                                        locked_token_ctrler
                                            .insert_user_token(TokenType::Data, &mut token);
                                    }
                                    drop(locked_token_ctrler);
                                }

                                // EXTRA: save in removed so we don't transform this item further
                                items_of_table.remove(i);
                                removed_items.insert(i.to_vec());
                            }
                            // delete the item
                            let delstmt = format!("DELETE FROM {} WHERE {}", table, selection);
                            helpers::query_drop(delstmt, &mut conn, mystats.clone()).unwrap();

                            // save that these tokens should be removed
                            for (_, ts) in &mut token_transforms {
                                // save the transformation to perform on this token
                                ts.push(t.clone());
                            }
                        }
                        TransformArgs::Decor { fk_col, .. } => {
                            for i in pred_items {
                                // don't decorrelate if removed
                                if removed_items.contains(&i) {
                                    // remove if we'd accidentally added it before
                                    items_of_table.remove(&i);
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items.contains(&(fk_col.clone(), i.clone())) {
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(&i) {
                                    ts.push(t.clone());
                                } else {
                                    items_of_table.insert(i.clone(), vec![t.clone()]);
                                }
                                decorrelated_items.insert((fk_col.clone(), i.clone()));
                            }

                            // TOKENS modify any matching data tokens from prior disguises
                            let token_keys: Vec<Token> =
                                token_transforms.keys().map(|k| k.clone()).collect();
                            for token in token_keys {
                                // don't decorrelate token if removed
                                if removed_items.contains(&token.new_value) {
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items
                                    .contains(&(fk_col.clone(), token.new_value.clone()))
                                {
                                    continue;
                                }
                                // save the transformation to perform on this token
                                let ts = token_transforms.get_mut(&token).unwrap();
                                ts.push(t.clone());
                                decorrelated_items
                                    .insert((fk_col.clone(), token.new_value.clone()));
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

                            // TOKENS modify any matching removed data tokens from prior disguises
                            let token_keys: Vec<Token> =
                                token_transforms.keys().map(|k| k.clone()).collect();
                            for token in token_keys {
                                // don't modify token if removed
                                if removed_items.contains(&token.new_value) {
                                    continue;
                                }
                                // save the transformation to perform on this token
                                let ts = token_transforms.get_mut(&token).unwrap();
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
                let mut locked_tokens = my_tokens_to_modify.write().unwrap();
                locked_tokens.extend(token_transforms);
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
        self.items.write().unwrap().clear();
        warn!("Disguiser: clear disguise records");
    }
}

fn modify_item(
    did: DID,
    uid: UID,
    global: bool,
    token_ctrler: &mut TokenCtrler,
    cols_to_update: &mut Vec<Assignment>,
    stats: &mut QueryStat,
    table: &str,
    table_info: &TableInfo,
    col: &str,
    new_val: String,
    i: &Vec<RowVal>,
) {
    warn!("Thread {:?} starting mod {}", thread::current().id(), table);
    let start = time::Instant::now();

    // save the column to update for this item
    cols_to_update.push(Assignment {
        id: Ident::new(col.clone()),
        value: Expr::Value(Value::String(new_val.clone())),
    });

    // TOKEN INSERT
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
    let ids = get_ids(&table_info.id_cols, &i);
    let mut update_token =
        Token::new_update_token(did, uid, table.to_string(), ids, i.clone(), new_obj);
    for owner_col in &table_info.owner_cols {
        let uid = get_value_of_col(&i, &owner_col).unwrap();
        update_token.uid = u64::from_str(&uid).unwrap();
        if !global {
            token_ctrler.insert_user_token(TokenType::Data, &mut update_token);
        } else {
            token_ctrler.insert_global_token(&mut update_token);
        }
    }

    stats.mod_dur += start.elapsed();
    warn!("Thread {:?} modify {}", thread::current().id(), table);
}

fn decor_item(
    did: DID,
    uid: UID,
    global: bool,
    to_insert: &mut HashMap<(String, Vec<String>), Vec<Vec<Expr>>>,
    token_ctrler: &mut TokenCtrler,
    cols_to_update: &mut Vec<Assignment>,
    stats: &mut QueryStat,
    child_table: &str,
    child_table_info: &TableInfo,
    fk_name: &str,
    fk_col: &str,
    fk_table_info: &TableInfo,
    fk_gen: &GuiseGen,
    i: &Vec<RowVal>,
) {
    warn!(
        "Thread {:?} starting decor {}",
        thread::current().id(),
        child_table
    );
    let start = time::Instant::now();

    /*
     * DECOR OBJECT MODIFICATIONS
     * A) insert guises for parents
     * B) update child to point to new guise
     * */

    // get ID of old parent
    let old_uid = u64::from_str(&get_value_of_col(&i, &fk_col).unwrap()).unwrap();
    warn!(
        "decor_obj {}: Creating guises for fkids {:?} {:?}",
        child_table, fk_name, old_uid,
    );

    // A. CREATE NEW PARENT
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
    let new_parent_ids = get_ids(&fk_table_info.id_cols, &new_parent_rowvals);
    let guise_id = new_parent_ids[0].value.to_string();
    warn!("decor_obj: inserted guise {}.{}", fk_name, guise_id);

    // save guise to insert
    if let Some(vals) = to_insert.get_mut(&(fk_name.to_string(), new_parent_cols.clone())) {
        vals.push(new_parent_vals.clone());
    } else {
        to_insert.insert(
            (fk_name.to_string(), new_parent_cols),
            vec![new_parent_vals.clone()],
        );
    }

    // B. UPDATE CHILD FOREIGN KEY
    cols_to_update.push(Assignment {
        id: Ident::new(fk_col.clone()),
        value: Expr::Value(Value::Number(guise_id.to_string())),
    });

    // TOKEN INSERT
    let new_child: Vec<RowVal> = i
        .iter()
        .map(|v| {
            if &v.column == fk_col {
                RowVal {
                    column: v.column.clone(),
                    value: guise_id.to_string(),
                }
            } else {
                v.clone()
            }
        })
        .collect();
    let child_ids = get_ids(&child_table_info.id_cols, &new_child);
    let mut decor_token = Token::new_decor_token(
        did,
        uid,
        child_table.to_string(),
        child_ids,
        fk_name.to_string(),
        i.clone(),
        new_child,
    );
    for owner_col in &child_table_info.owner_cols {
        let uid = get_value_of_col(&i, &owner_col).unwrap();
        decor_token.uid = u64::from_str(&uid).unwrap();
        if !global {
            token_ctrler.insert_user_token(TokenType::Data, &mut decor_token);
        } else {
            token_ctrler.insert_global_token(&mut decor_token);
        }
    }
    stats.decor_dur += start.elapsed();
    warn!("Thread {:?} decor {}", thread::current().id(), child_table);
}
