use crate::helpers::*;
use crate::predicate::*;
use crate::stats::*;
use crate::diffs::*;
use crate::*;
use mysql::{Opts, Pool};
use rsa::RsaPublicKey;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
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
    pub diff_ctrler: Arc<Mutex<DiffCtrler>>,

    // table name to [(rows) -> transformations] map
    items: Arc<RwLock<HashMap<String, HashMap<Vec<RowVal>, Vec<Transform>>>>>,
    diffs_to_modify: Arc<RwLock<HashMap<Diff, Vec<Transform>>>>,
    to_insert: Arc<Mutex<HashMap<(String, Vec<String>), Vec<Vec<Expr>>>>>,
}

impl Disguiser {
    pub fn new(url: &str) -> Disguiser {
        let opts = Opts::from_url(&url).unwrap();
        let pool = Pool::new(opts).unwrap();

        Disguiser {
            pool: pool,
            stats: Arc::new(Mutex::new(stats::QueryStat::new())),
            diff_ctrler: Arc::new(Mutex::new(DiffCtrler::new())),
            diffs_to_modify: Arc::new(RwLock::new(HashMap::new())),
            to_insert: Arc::new(Mutex::new(HashMap::new())),
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_principal(&mut self, uid: u64, email: String, pubkey: &RsaPublicKey) {
        let mut locked_diff_ctrler = self.diff_ctrler.lock().unwrap();
        locked_diff_ctrler.register_principal(uid, email, pubkey);
    }

    fn get_diffs_at_loc (
        &mut self,
        uid: UID,
        did: DID,
        data_cap: DataCap,
        loc_cap: LocCap,
    ) -> Vec<diffs::Diff> {
        let locked_diff_ctrler = self.diff_ctrler.lock().unwrap();
        let diffs = locked_diff_ctrler.get_diffs(uid, did, &data_cap, loc_cap);
        warn!("Got {} diffs", diffs.len());
        drop(locked_diff_ctrler);
        diffs
    }

    pub fn reverse(
        &mut self,
        uid: UID,
        disguise: Arc<disguise::Disguise>,
        data_cap: diffs::DataCap,
        loc_cap: diffs::LocCap,
    ) -> Result<(), mysql::Error> {
        let mut conn = self.pool.get_conn()?;
        let mut diffs_to_mark_revealed: Vec<Diff> = vec![];
        let mut locked_diff_ctrler = self.diff_ctrler.lock().unwrap();
        let diffs = locked_diff_ctrler.get_diffs(uid, disguise.did, &data_cap, loc_cap);

        for t in diffs {
            // only reverse diffs of disguise if not yet revealed
            if t.did == disguise.did && !t.revealed {
                warn!("Reversing diff {:?}", t);
                let revealed = t.reveal(&mut locked_diff_ctrler, &mut conn, self.stats.clone())?;
                if revealed {
                    diffs_to_mark_revealed.push(t);
                }
            } else {
                warn!("Skipping reversal of diff {:?}", t);
            }
        }

        // mark all revealed diffs as revealed
        for t in &diffs_to_mark_revealed {
            locked_diff_ctrler.mark_diff_revealed(t, &data_cap, loc_cap);
        }
        drop(locked_diff_ctrler);
        self.end_disguise_action();
        Ok(())
    }

    pub fn apply(
        &mut self,
        uid: UID,
        disguise: Arc<disguise::Disguise>,
        data_cap: diffs::DataCap,
        loc_caps: Vec<diffs::LocCap>
    ) -> Result<HashMap<(UID, DID), diffs::LocCap>, mysql::Error> {
        let mut conn = self.pool.get_conn()?;
        let mut threads = vec![];
        let did = disguise.did;
        let mut locked_diff_ctrler = self.diff_ctrler.lock().unwrap();
        let mut diffs = vec![];
        for lc in loc_caps {
            diffs.extend(locked_diff_ctrler.get_diffs(uid, did, &data_cap, lc).iter().cloned());
        }
        drop(locked_diff_ctrler);

        /*
         * PREDICATE AND REMOVE
         */
        // get all the objects, remove the objects to remove
        // integrate vault_transforms into disguise read + write phases
        self.select_predicate_objs_and_execute_removes(disguise.clone(), diffs);

        /*
         * UPDATE/DECOR
         */
        for (table, _) in disguise.table_disguises.clone() {
            // clone disguiser fields
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let my_insert = self.to_insert.clone();
            let my_items = self.items.clone();
            let my_diff_ctrler = self.diff_ctrler.clone();

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
                                let mut locked_diff_ctrler = my_diff_ctrler.lock().unwrap();
                                let mut locked_stats = mystats.lock().unwrap();

                                warn!("Decor item of table {} in apply disguise: {:?}", table, i);
                                decor_item(
                                    // disguise and per-thread state
                                    did,
                                    t.global,
                                    &mut locked_insert,
                                    &mut locked_diff_ctrler,
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
                                let mut locked_diff_ctrler = my_diff_ctrler.lock().unwrap();
                                let mut locked_stats = mystats.lock().unwrap();
                                let old_val = get_value_of_col(&i, &col).unwrap();

                                modify_item(
                                    did,
                                    t.global,
                                    &mut locked_diff_ctrler,
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

        self.modify_global_diffs(disguise);

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
        let mut locked_diff_ctrler = self.diff_ctrler.lock().unwrap();
        let loc_caps = locked_diff_ctrler.save_and_clear_loc_caps();
        drop(locked_diff_ctrler);
        self.end_disguise_action();
        Ok(loc_caps)
    }

    fn modify_global_diffs(&mut self, disguise: Arc<Disguise>) {
        let did = disguise.did;
        let uid = match &disguise.user {
            Some(u) => u.id,
            None => 0,
        };
        // apply updates to each diff (for now do sequentially)
        let mut locked_diff_ctrler = self.diff_ctrler.lock().unwrap();
        for (diff, ts) in self.diffs_to_modify.write().unwrap().iter() {
            for t in ts {
                // we don't update global diffs if they've been disguised by a global
                // disguise---no information leakage here
                if t.global {
                    continue;
                }
                let mut cols_to_update = vec![];
                match &*t.trans.read().unwrap() {
                    TransformArgs::Decor { fk_col, fk_name } => {
                        let locked_table_info = disguise.table_info.read().unwrap();
                        let locked_guise_gen = disguise.guise_gen.read().unwrap();

                        warn!("Decor item in modify global diffs: {:?}", diff.old_value);
                        decor_item(
                            // disguise and per-thread state
                            did,
                            t.global,
                            &mut self.to_insert.lock().unwrap(),
                            &mut locked_diff_ctrler,
                            &mut cols_to_update,
                            &mut self.stats.lock().unwrap(),
                            // info needed for decorrelation
                            &diff.guise_name,
                            locked_table_info.get(&diff.guise_name).unwrap(),
                            fk_name,
                            fk_col,
                            locked_table_info.get(fk_name).unwrap(),
                            locked_guise_gen.get(fk_name).unwrap(),
                            &diff.old_value,
                        );
                    }
                    TransformArgs::Modify {
                        col,
                        generate_modified_value,
                        ..
                    } => {
                        let locked_table_info = disguise.table_info.read().unwrap();
                        let old_val = get_value_of_col(&diff.old_value, &col).unwrap();

                        modify_item(
                            did,
                            t.global,
                            &mut locked_diff_ctrler,
                            &mut cols_to_update,
                            &mut self.stats.lock().unwrap(),
                            &diff.guise_name,
                            locked_table_info.get(&diff.guise_name).unwrap(),
                            col,
                            (*(generate_modified_value))(&old_val),
                            &diff.old_value,
                        );
                    }
                    TransformArgs::Remove => {
                        // remove diff from vault if diff is global, and the new transformation is
                        // private (although we already check this above)
                        if diff.is_global && !t.global {
                            assert!(uid != 0);
                            if !locked_diff_ctrler.remove_global_diff(uid, did, &diff) {
                                warn!("Could not remove old disguise diff!! {:?}", diff);
                            }
                            // continue onto the next diff, don't modify it!
                            continue;
                        }
                    }
                }
                // apply cols_to_update if diff is global, and the new transformation is
                // private (although we already check this above)
                if diff.is_global && !t.global {
                    // update both old and new values so that no data leaks
                    let mut new_diff = diff.clone();
                    new_diff.new_value = diff
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
                    new_diff.old_value = diff
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
                    assert!(uid != 0);
                    if !locked_diff_ctrler.update_global_diff_from_old_to(
                        &diff,
                        &new_diff,
                        Some((uid, did)),
                    ) {
                        warn!("Could not update old disguise diff!! {:?}", diff);
                    }
                }
            }
        }
        drop(locked_diff_ctrler);
    }

    fn select_predicate_objs_and_execute_removes(
        &self,
        disguise: Arc<Disguise>,
        diffs: Vec<Diff>,
    ) {
        let mut threads = vec![];
        for (table, transforms) in disguise.table_disguises.clone() {
            let my_table_info = disguise.table_info.clone();
            let pool = self.pool.clone();
            let mystats = self.stats.clone();
            let did = disguise.did;
            let my_items = self.items.clone();
            let my_diffs_to_modify = self.diffs_to_modify.clone();
            let my_diffs = diffs.clone();
            let my_diff_ctrler = self.diff_ctrler.clone();

            // hashmap from item value --> transform
            let mut items_of_table: HashMap<Vec<RowVal>, Vec<Transform>> = HashMap::new();
            let mut diff_transforms: HashMap<Diff, Vec<Transform>> = HashMap::new();

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
                    let selected_rows = get_query_rows_str(
                        &str_select_statement(&table, &selection),
                        &mut conn,
                        mystats.clone(),
                    )
                    .unwrap();
                    let mut pred_items: HashSet<&Vec<RowVal>> =
                        HashSet::from_iter(selected_rows.iter());

                    // TOKENS RETRIEVAL: get diffs that match the predicate
                    let pred_diffs = predicate::get_diffs_matching_pred(&t.pred, &table, &my_diffs);
                    for pt in &pred_diffs {
                        // for diffs that decorrelated or updated a guise, we want to add the new
                        // value that should be transformed into the set of predicated items
                        match pt.update_type {
                            DECOR_GUISE | MODIFY_GUISE => {
                                pred_items.insert(&pt.new_value);
                            }
                            _ => (),
                        }
                        // for all global diffs, we want to update the
                        // stored value of this diff so that we only ever restore the most
                        // up-to-date disguised data and the diff doesn't leak any data
                        // NOTE: during reversal, we'll have to reverse this diff update
                        if pt.is_global {
                            diff_transforms.insert(pt.clone(), vec![]);
                        }
                    }

                    // just remove item if it's supposed to be removed
                    match &*t.trans.read().unwrap() {
                        TransformArgs::Remove => {
                            for i in pred_items {
                                // don't remove an item that's already removed
                                if removed_items.contains(i) {
                                    continue;
                                }
                                debug!("Remove: Getting ids of table {} for {:?}", table, i);
                                let ids = get_ids(&curtable_info.id_cols, i);

                                // TOKEN INSERT
                                let mut diff = Diff::new_delete_diff(
                                    did,
                                    table.clone(),
                                    ids.clone(),
                                    i.clone(),
                                );
                                for owner_col in &curtable_info.owner_cols {
                                    let owner_uid = get_value_of_col(&i, &owner_col).unwrap();
                                    diff.uid = u64::from_str(&owner_uid).unwrap();
                                    let mut locked_diff_ctrler = my_diff_ctrler.lock().unwrap();
                                    if t.global {
                                        locked_diff_ctrler.insert_global_diff(&mut diff);
                                    } else {
                                        locked_diff_ctrler
                                            .insert_user_data_diff(&mut diff);
                                    }
                                    drop(locked_diff_ctrler);
                                }

                                // EXTRA: save in removed so we don't transform this item further
                                items_of_table.remove(i);
                                removed_items.insert(i.to_vec());
                            }
                            // delete the item
                            let delstmt = format!("DELETE FROM {} WHERE {}", table, selection);
                            helpers::query_drop(delstmt, &mut conn, mystats.clone()).unwrap();

                            // save that these diffs should be removed
                            for (_, ts) in &mut diff_transforms {
                                // save the transformation to perform on this diff
                                ts.push(t.clone());
                            }
                        }
                        TransformArgs::Decor { fk_col, .. } => {
                            for i in pred_items {
                                // don't decorrelate if removed
                                if removed_items.contains(i) {
                                    // remove if we'd accidentally added it before
                                    items_of_table.remove(i);
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items.contains(&(fk_col.clone(), i.clone())) {
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(i) {
                                    ts.push(t.clone());
                                } else {
                                    items_of_table.insert(i.clone(), vec![t.clone()]);
                                }
                                decorrelated_items.insert((fk_col.clone(), i.clone()));
                            }

                            // TOKENS modify any matching data diffs from prior disguises
                            let diff_keys: Vec<Diff> =
                                diff_transforms.keys().map(|k| k.clone()).collect();
                            for diff in diff_keys {
                                // don't decorrelate diff if removed
                                if removed_items.contains(&diff.new_value) {
                                    continue;
                                }
                                // don't decorrelate twice
                                if decorrelated_items
                                    .contains(&(fk_col.clone(), diff.new_value.clone()))
                                {
                                    continue;
                                }
                                // save the transformation to perform on this diff
                                let ts = diff_transforms.get_mut(&diff).unwrap();
                                ts.push(t.clone());
                                decorrelated_items
                                    .insert((fk_col.clone(), diff.new_value.clone()));
                            }
                        }
                        _ => {
                            for i in pred_items {
                                // don't modify if removed
                                if removed_items.contains(i) {
                                    items_of_table.remove(i);
                                    continue;
                                }
                                if let Some(ts) = items_of_table.get_mut(i) {
                                    ts.push(t.clone());
                                } else {
                                    items_of_table.insert(i.clone(), vec![t.clone()]);
                                }
                            }

                            // TOKENS modify any matching removed data diffs from prior disguises
                            let diff_keys: Vec<Diff> =
                                diff_transforms.keys().map(|k| k.clone()).collect();
                            for diff in diff_keys {
                                // don't modify diff if removed
                                if removed_items.contains(&diff.new_value) {
                                    continue;
                                }
                                // save the transformation to perform on this diff
                                let ts = diff_transforms.get_mut(&diff).unwrap();
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
                let mut locked_diffs = my_diffs_to_modify.write().unwrap();
                locked_diffs.extend(diff_transforms);
                drop(locked_diffs);
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
        self.items.write().unwrap().clear();
        self.diffs_to_modify.write().unwrap().clear();
        warn!("Disguiser: clear disguise records");
    }
}

fn modify_item(
    did: DID,
    global: bool,
    diff_ctrler: &mut DiffCtrler,
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
    let mut update_diff = Diff::new_modify_diff(did, table.to_string(), ids, i.clone(), new_obj);
    for owner_col in &table_info.owner_cols {
        let owner_uid = get_value_of_col(&i, &owner_col).unwrap();
        update_diff.uid = u64::from_str(&owner_uid).unwrap();
        if !global {
            diff_ctrler.insert_user_data_diff(&mut update_diff);
        } else {
            diff_ctrler.insert_global_diff(&mut update_diff);
        }
    }

    stats.mod_dur += start.elapsed();
    warn!("Thread {:?} modify {}", thread::current().id(), table);
}

fn decor_item(
    did: DID,
    global: bool,
    to_insert: &mut HashMap<(String, Vec<String>), Vec<Vec<Expr>>>,
    diff_ctrler: &mut DiffCtrler,
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
    let guise_id = u64::from_str(&new_parent_ids[0].value).unwrap();
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
    let mut decor_diff = Diff::new_decor_diff(
        did,
        child_table.to_string(),
        child_ids,
        fk_name.to_string(),
        new_parent_ids[0].column.to_string(),
        i.clone(),
        new_child,
    );
    for owner_col in &child_table_info.owner_cols {
        let old_uid = get_value_of_col(&i, &owner_col).unwrap();
        decor_diff.uid = u64::from_str(&old_uid).unwrap();
        if !global {
            diff_ctrler.insert_user_data_diff(&mut decor_diff);
        } else {
            diff_ctrler.insert_global_diff(&mut decor_diff);
        }
        // actually register the anon principal, including saving its privkey for the old uid
        diff_ctrler.register_anon_principal(u64::from_str(&old_uid).unwrap(), guise_id, did);
    }
    stats.decor_dur += start.elapsed();
}
