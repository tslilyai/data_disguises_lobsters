use crate::helpers::*;
use crate::predicate::*;
use crate::stats::*;
use crate::tokens::*;
use crate::*;
use mysql::{Opts, Pool};
use rsa::RsaPublicKey;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
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

pub struct Disguise {
    pub did: u64,
    pub user: String,
    pub table_disguises: HashMap<String, Arc<RwLock<Vec<Transform>>>>,
    pub table_info: Arc<RwLock<HashMap<String, TableInfo>>>,
    pub guise_gen: Arc<RwLock<HashMap<String, GuiseGen>>>,
}

pub struct Disguiser {
    pub pool: mysql::Pool,
    pub stats: Arc<Mutex<QueryStat>>,
    pub token_ctrler: Arc<Mutex<TokenCtrler>>,

    // table name to [(rows) -> transformations] map
    items_to_modify: Arc<RwLock<HashMap<String, HashMap<Vec<RowVal>, Vec<Transform>>>>>,
    global_diff_tokens_to_modify: Arc<RwLock<HashMap<DiffToken, Vec<Transform>>>>,
    to_insert: Arc<Mutex<HashMap<(String, Vec<String>), Vec<Vec<Expr>>>>>,
}

impl Disguiser {
    pub fn new(url: &str) -> Disguiser {
        let opts = Opts::from_url(&url).unwrap();
        let pool = Pool::new(opts).unwrap();
        let stats = Arc::new(Mutex::new(stats::QueryStat::new()));

        Disguiser {
            pool: pool.clone(),
            stats: stats.clone(),
            token_ctrler: Arc::new(Mutex::new(TokenCtrler::new(&mut pool.get_conn().unwrap(), stats.clone()))),
            global_diff_tokens_to_modify: Arc::new(RwLock::new(HashMap::new())),
            to_insert: Arc::new(Mutex::new(HashMap::new())),
            items_to_modify: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_principal(&mut self, uid: &UID, email: String, pubkey: &RsaPublicKey) {
        let mut conn = self.pool.get_conn().unwrap();
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        locked_token_ctrler.register_principal(uid, email, pubkey, &mut conn);
    }

    pub fn reverse(
        &mut self,
        disguise: Arc<disguise::Disguise>,
        data_cap: tokens::DataCap,
        diff_loc_caps: Vec<tokens::LocCap>,
        own_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<(), mysql::Error> {
        let mut conn = self.pool.get_conn()?;
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();

        // XXX revealing all global tokens when a disguise is reversed
        let mut diff_tokens = locked_token_ctrler.get_global_diff_tokens_of_disguise(disguise.did);
        let (dts, own_tokens) = locked_token_ctrler.get_user_tokens(
            disguise.did,
            &data_cap,
            &diff_loc_caps,
            &own_loc_caps,
        );
        diff_tokens.extend(dts.iter().cloned());

        // reverse REMOVE tokens first
        for d in &diff_tokens {
            if d.did == disguise.did && !d.revealed && d.update_type == REMOVE_GUISE {
                warn!("Reversing remove token {:?}\n", d);
                let revealed = d.reveal(&mut locked_token_ctrler, &mut conn, self.stats.clone())?;
                if revealed {
                    warn!("Remove Token reversed!\n");
                    locked_token_ctrler.mark_diff_token_revealed(
                        disguise.did,
                        d,
                        &data_cap,
                        &diff_loc_caps,
                        &own_loc_caps,
                    );
                }
            }
        }

        for d in &diff_tokens {
            // only reverse tokens of disguise if not yet revealed
            if d.did == disguise.did && !d.revealed && d.update_type != REMOVE_GUISE {
                warn!("Reversing token {:?}\n", d);
                let revealed = d.reveal(&mut locked_token_ctrler, &mut conn, self.stats.clone())?;
                if revealed {
                    warn!("NonRemove Diff Token reversed!\n");
                    locked_token_ctrler.mark_diff_token_revealed(
                        disguise.did,
                        d,
                        &data_cap,
                        &diff_loc_caps,
                        &own_loc_caps,
                    );
                }
            }
        }

        for d in &own_tokens {
            if d.did == disguise.did {
                warn!("Reversing token {:?}\n", d);
                let revealed = d.reveal(&mut locked_token_ctrler, &mut conn, self.stats.clone())?;
                if revealed {
                    warn!("Decor Ownership Token reversed!\n");
                    locked_token_ctrler.mark_ownership_token_revealed(
                        disguise.did,
                        d,
                        &data_cap,
                        &own_loc_caps,
                    );
                }
            }
        }

        drop(locked_token_ctrler);
        self.end_disguise_action();
        Ok(())
    }

    pub fn apply(
        &mut self,
        disguise: Arc<disguise::Disguise>,
        data_cap: tokens::DataCap,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<(HashMap<(UID, DID), tokens::LocCap>, HashMap<(UID, DID), tokens::LocCap>), mysql::Error> {
        let mut conn = self.pool.get_conn()?;
        let mut threads = vec![];
        let did = disguise.did;
        let locked_token_ctrler = self.token_ctrler.lock().unwrap();
        let global_diff_tokens = locked_token_ctrler.get_all_global_diff_tokens();
        let (_, ownership_tokens) = locked_token_ctrler.get_user_tokens(
            disguise.did,
            &data_cap,
            &vec![],
            &ownership_loc_caps,
        );
        drop(locked_token_ctrler);

        /*
         * REMOVE
         */
        // WE ONLY NEED GLOBAL DIFF TOKENS because we need to potentially modify them
        self.execute_removes(disguise.clone(), &global_diff_tokens, &ownership_tokens);

        /*
         * PREDICATE
         */
        // get all the objects, remove the objects to remove
        // integrate vault_transforms into disguise read + write phases
        self.select_predicate_objs(disguise.clone(), &global_diff_tokens, &ownership_tokens);

        /*
         * UPDATE/DECOR
         */
        for (table, _) in disguise.table_disguises.clone() {
            // clone disguiser fields
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items_to_modify.clone();
            let my_token_ctrler = self.token_ctrler.clone();

            // clone disguise fields
            let my_table_info = disguise.table_info.clone();
            let my_guise_gen = disguise.guise_gen.clone();

            threads.push(thread::spawn(move || {
                let mut conn = pool.get_conn().unwrap();
                let locked_table_info = my_table_info.read().unwrap();
                let curtable_info = locked_table_info.get(&table).unwrap();

                let locked_items = my_items.read().unwrap();
                let table_items = locked_items.get(&table).unwrap().clone();
                drop(locked_items);

                let locked_guise_gen = my_guise_gen.read().unwrap();

                warn!(
                    "Thread {:?} starting for table {}\n",
                    thread::current().id(),
                    table
                );

                // get and apply the transformations for each object
                let mut update_stmts = vec![];
                for (i, ts) in table_items.iter() {
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

                                warn!("Decor item of table {} in apply disguise: {:?}\n", table, i);
                                decor_item(
                                    // disguise and per-thread state
                                    did,
                                    &mut locked_insert,
                                    &mut locked_token_ctrler,
                                    &mut cols_to_update,
                                    &mut locked_stats,
                                    // info needed for decorrelation
                                    &table,
                                    curtable_info,
                                    &fk_name,
                                    &fk_col,
                                    fk_table_info,
                                    locked_fk_gen,
                                    i,
                                    &mut conn
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

        self.modify_global_diff_tokens(disguise);

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

        // any capabilities generated during disguise should be emailed
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        let loc_caps = locked_token_ctrler.save_and_clear(did, &mut conn);
        drop(locked_token_ctrler);
        self.end_disguise_action();
        Ok(loc_caps)
    }

    fn modify_global_diff_tokens(&mut self, disguise: Arc<Disguise>) {
        let did = disguise.did;
        let uid = disguise.user.clone();
        
        // apply updates to each token (for now do sequentially)
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        for (token, ts) in self.global_diff_tokens_to_modify.write().unwrap().iter() {
            for t in ts {
                // we don't update global tokens if they've been disguised by a global
                // disguise---no information leakage here
                if t.global {
                    continue;
                }
                let mut cols_to_update = vec![];
                match &*t.trans.read().unwrap() {
                    TransformArgs::Decor { .. } => {
                        unimplemented!("No global decor tokens allowed!")
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
                            t.global,
                            &mut locked_token_ctrler,
                            &mut cols_to_update,
                            &mut self.stats.lock().unwrap(),
                            &token.guise_name,
                            locked_table_info.get(&token.guise_name).unwrap(),
                            &col,
                            (*(generate_modified_value))(&old_val),
                            &token.old_value,
                        );
                    }
                    TransformArgs::Remove => {
                        // remove token from vault if token is global, and the new transformation is
                        // private (although we already check this above)
                        if token.is_global && !t.global {
                            if !locked_token_ctrler.remove_global_diff_token(&uid, did, &token) {
                                warn!("Could not remove old disguise token!! {:?}", token);
                            }
                            // continue onto the next token, don't modify it!
                            continue;
                        }
                    }
                }
                // apply cols_to_update if token is global, and the new transformation is
                // private (although we already check this above)
                if token.is_global && !t.global {
                    // update both old and new values so that no data leaks
                    let mut new_token = token.clone();
                    new_token.new_value = token
                        .new_value
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
                    if !locked_token_ctrler.update_global_diff_token_from_old_to(
                        &token,
                        &new_token,
                        Some((uid.clone(), did)),
                    ) {
                        warn!("Could not update old disguise token!! {:?}", token);
                    }
                }
            }
        }
        drop(locked_token_ctrler);
    }

    fn execute_removes(
        &self,
        disguise: Arc<Disguise>,
        diff_tokens: &Vec<DiffToken>,
        own_tokens: &Vec<OwnershipToken>,
    ) {
        warn!(
            "ApplyRemoves: removing objs for disguise {} with {} own_tokens\n",
            disguise.did,
            own_tokens.len()
        );
        for (table, transforms) in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let did = disguise.did;
            let mut conn = pool.get_conn().unwrap();
            let mut locked_diff_tokens = self.global_diff_tokens_to_modify.write().unwrap();

            let locked_table_info = disguise.table_info.read().unwrap();
            let curtable_info = locked_table_info.get(&table).unwrap().clone();
            drop(locked_table_info);

            // REMOVES: do one loop to handle removes
            for t in &*transforms.read().unwrap() {
                if let TransformArgs::Remove = *t.trans.read().unwrap() {
                    let preds = predicate::get_all_preds_with_owners(&t.pred, own_tokens);
                    warn!("Got preds {:?} with own_tokens {:?}\n", preds, own_tokens);
                    for p in &preds {
                        let selection = predicate::pred_to_sql_where(p);
                        let selected_rows = get_query_rows_str(
                            &str_select_statement(&table, &selection),
                            &mut conn,
                            mystats.clone(),
                        )
                        .unwrap();
                        let pred_items: HashSet<&Vec<RowVal>> = HashSet::from_iter(selected_rows.iter());
                        warn!(
                            "ApplyPred: Got {} selected rows matching predicate {:?}\n",
                            pred_items.len(),
                            p  
                        );

                        // BATCH REMOVE ITEMS
                        let delstmt = format!("DELETE FROM {} WHERE {}", table, selection);
                        helpers::query_drop(delstmt, &mut conn, mystats.clone()).unwrap();

                        // ITEM REMOVAL: ADD TOKEN RECORDS
                        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
                        let locked_guise_gen = disguise.guise_gen.read().unwrap();
                        for i in &pred_items {
                            let ids = get_ids(&curtable_info.id_cols, i);

                            // TOKEN INSERT FOR REMOVAL
                            let mut token =
                                DiffToken::new_delete_token(did, table.to_string(), ids.clone(), i.to_vec());
                            for owner_col in &curtable_info.owner_cols {
                                token.uid = get_value_of_col(&i, &owner_col).unwrap();
                                if t.global {
                                    locked_token_ctrler.insert_global_diff_token(&mut token);
                                } else {
                                    locked_token_ctrler.insert_user_diff_token(&mut token);
                                }
                                // if we're working on a guise table (e.g., a users table)
                                // remove the user
                                if locked_guise_gen.contains_key(&table) {
                                    locked_token_ctrler.mark_remove_principal(&token.uid);
                                }
                            }
                        }
                        drop(locked_guise_gen);
                        drop(locked_token_ctrler);
                        
                        // MARK MATCHING GLOBAL DIFF TOKENS FOR REMOVAL 
                        for dt in diff_tokens {
                            if dt.is_global && predicate::diff_token_matches_pred(&p, &table, &dt) {
                                warn!("ApplyRemoves: Inserting global token {:?} to update\n", dt);
                                match locked_diff_tokens.get_mut(&dt) {
                                    Some(vs) => vs.push(t.clone()),
                                    None => {
                                        locked_diff_tokens.insert(dt.clone(), vec![t.clone()]);
                                    }
                                }
                            }
                        }
                        // we already removed the actual user/principal for any global tokens
                    }
                }
            }
        }
    }

    fn select_predicate_objs(
        &self,
        disguise: Arc<Disguise>,
        diff_tokens: &Vec<DiffToken>,
        own_tokens: &Vec<OwnershipToken>,
    ) {
        warn!(
            "ApplyPred: Selecting predicated objs for disguise {} with {} tokens",
            disguise.did,
            diff_tokens.len()
        );
        let mut threads = vec![];

        for (table, transforms) in disguise.table_disguises.clone() {
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_items = self.items_to_modify.clone();
            let my_global_diff_tokens_to_modify = self.global_diff_tokens_to_modify.clone();
            let my_diff_tokens = diff_tokens.clone();
            let my_own_tokens = own_tokens.clone();

            // hashmap from item value --> transform
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Transform>> = HashMap::new();
            let mut token_transforms: HashMap<DiffToken, Vec<Transform>> = HashMap::new();

            threads.push(thread::spawn(move || {
                // XXX note: not tracking if we remove or decorrelate twice
                let mut conn = pool.get_conn().unwrap();
                let my_transforms = transforms.read().unwrap();

                // handle Decor and Modify
                for t in &*my_transforms {
                    // skip removes
                    if let TransformArgs::Remove = *t.trans.read().unwrap() {
                        continue;
                    }
                    let preds = predicate::get_all_preds_with_owners(&t.pred, &my_own_tokens);
                    for p in &preds {
                        let selection = predicate::pred_to_sql_where(p);
                        let selected_rows = get_query_rows_str(
                            &str_select_statement(&table, &selection),
                            &mut conn,
                            mystats.clone(),
                        )
                        .unwrap();

                        warn!(
                            "ApplyPred: Got {} selected rows matching predicate {:?}\n",
                            selected_rows.len(),
                            p
                        );
                        // ADD TRANSFORMATIONS TO PERFORM ON PREDICATED ITEMS
                        for i in &selected_rows {
                            if let Some(ts) = items_of_table.get_mut(i) {
                                ts.push(t.clone());
                            } else {
                                items_of_table.insert(i.clone(), vec![t.clone()]);
                            }
                        }

                        // ensure that matching diff tokens are updated
                        // TODO separate out predicating on global diff tokens completely?
                        for dt in &my_diff_tokens {
                            if dt.is_global && predicate::diff_token_matches_pred(p, &table, dt) {
                                warn!("ApplyRemoves: Inserting global token {:?} to update\n", dt);
                                match token_transforms.get_mut(&dt) {
                                    Some(vs) => vs.push(t.clone()),
                                    None => {
                                        token_transforms.insert(dt.clone(), vec![t.clone()]);
                                    }
                                }
                            }
                        }
                    }
                }
                // there should only be one thread working on items from this table, so just insert
                let mut locked_items = my_items.write().unwrap();
                assert!(locked_items.insert(table.clone(), items_of_table).is_none());
                drop(locked_items);

                // same thing for token transforms
                let mut locked_tokens = my_global_diff_tokens_to_modify.write().unwrap();
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

    fn end_disguise_action(&self) {
        self.to_insert.lock().unwrap().clear();
        self.items_to_modify.write().unwrap().clear();
        self.global_diff_tokens_to_modify.write().unwrap().clear();
        warn!("Disguiser: clear disguise records");
    }
}

fn modify_item(
    did: DID,
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
        DiffToken::new_modify_token(did, table.to_string(), ids, i.clone(), new_obj);
    for owner_col in &table_info.owner_cols {
        let owner_uid = get_value_of_col(&i, &owner_col).unwrap();
        update_token.uid = owner_uid.clone();
        if !global {
            token_ctrler.insert_user_diff_token(&mut update_token);
        } else {
            token_ctrler.insert_global_diff_token(&mut update_token);
        }
    }

    stats.mod_dur += start.elapsed();
    warn!("Thread {:?} modify {}", thread::current().id(), table);
}

fn decor_item(
    did: DID,
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
    conn: &mut mysql::PooledConn,
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
    let old_uid = get_value_of_col(&i, &fk_col).unwrap();
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
        value: Expr::Value(Value::Number(guise_id.clone())),
    });

    // TOKEN INSERT
    let new_child: Vec<RowVal> = i
        .iter()
        .map(|v| {
            if &v.column == fk_col {
                RowVal {
                    column: v.column.clone(),
                    value: guise_id.clone(),
                }
            } else {
                v.clone()
            }
        })
        .collect();
    let child_ids = get_ids(&child_table_info.id_cols, &new_child);

    // actually register the anon principal, including saving an ownership token for the old uid
    // token is always inserted ``privately''
    token_ctrler.register_anon_principal(
        &old_uid,
        &guise_id,
        did,
        child_table.to_string(),
        child_ids,
        fk_name.to_string(),
        new_parent_ids[0].column.clone(),
        fk_col.to_string(),
        conn
    );

    stats.decor_dur += start.elapsed();
}
