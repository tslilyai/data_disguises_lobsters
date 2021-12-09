use crate::helpers::*;
use crate::spec::*;
use crate::stats::*;
use crate::tokens::*;
use crate::*;
use log::warn;
use mysql::{Opts, Pool};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::{Arc, Mutex, RwLock};

pub struct Disguiser {
    pub pool: mysql::Pool,
    pub stats: Arc<Mutex<QueryStat>>,
    pub token_ctrler: Arc<Mutex<TokenCtrler>>,
    pub guise_gen: Arc<RwLock<GuiseGen>>,
    global_diff_tokens_to_modify: Arc<RwLock<HashMap<DiffTokenWrapper, Vec<ObjectTransformation>>>>,
    pseudoprincipal_data_pool: Vec<(UID, Vec<RowVal>)>,
    poolsize: usize,
}

impl Disguiser {
    /**************************************************
     * Functions for lower-level disguising
     *************************************************/
    pub fn new(
        dbserver: &str,
        url: &str,
        keypool_size: usize,
        guise_gen: Arc<RwLock<GuiseGen>>,
        batch: bool,
    ) -> Disguiser {
        let opts = Opts::from_url(&url).unwrap();
        let pool = Pool::new(opts).unwrap();
        let stats = Arc::new(Mutex::new(stats::QueryStat::new()));

        let mut d = Disguiser {
            pool: pool.clone(),
            stats: stats.clone(),
            token_ctrler: Arc::new(Mutex::new(TokenCtrler::new(
                keypool_size,
                dbserver,
                &mut pool.get_conn().unwrap(),
                stats.clone(),
                batch,
            ))),
            guise_gen: guise_gen.clone(),
            global_diff_tokens_to_modify: Arc::new(RwLock::new(HashMap::new())),
            pseudoprincipal_data_pool: vec![],
            poolsize: keypool_size,
        };
        d.repopulate_pseudoprincipals_pool();
        d
    }

    pub fn repopulate_pseudoprincipals_pool(&mut self) {
        let start = time::Instant::now();
        let guise_gen = self.guise_gen.read().unwrap();
        for _ in 0..self.poolsize {
            let new_parent_vals = (guise_gen.val_generation)();
            let new_parent_cols = (guise_gen.col_generation)();
            let mut ix = 0;
            let mut uid_ix = 0;
            let rowvals = new_parent_cols
                .iter()
                .map(|c| {
                    if c == &guise_gen.guise_id_col {
                        uid_ix = ix;
                    }
                    let rv = RowVal {
                        value: new_parent_vals[ix].to_string(),
                        column: c.to_string(),
                    };
                    ix += 1;
                    rv
                })
                .collect();
            let new_uid = new_parent_vals[uid_ix].to_string();
            self.pseudoprincipal_data_pool.push((new_uid, rowvals));
        }
        warn!(
            "Repopulated pseudoprincipal data pool of size {}: {}",
            self.poolsize,
            start.elapsed().as_micros()
        );
    }

    pub fn create_new_pseudoprincipal(&mut self) -> (UID, Vec<RowVal>) {
        match self.pseudoprincipal_data_pool.pop() {
            Some(vs) => vs,
            None => {
                // XXX todo queue up to run later, but just generate one key first
                self.repopulate_pseudoprincipals_pool();
                self.pseudoprincipal_data_pool.pop().unwrap()
            }
        }
    }

    pub fn get_pseudoprincipals(
        &self,
        decrypt_cap: &DecryptCap,
        ownership_loc_caps: &Vec<LocCap>,
        reveal: bool,
    ) -> Vec<UID> {
        let locked_token_ctrler = self.token_ctrler.lock().unwrap();
        let uids =
            locked_token_ctrler.get_user_pseudoprincipals(decrypt_cap, &HashSet::from_iter(ownership_loc_caps.iter().cloned()), reveal);
        drop(locked_token_ctrler);
        uids
    }

    /**************************************************
     * Functions that use higher-level disguise specs
     *************************************************/
    // Note: Decorrelations are not reversed if not using EdnaOwnershipTokens
    pub fn reverse(
        &mut self,
        did: DID,
        decrypt_cap: tokens::DecryptCap,
        diff_loc_caps: Vec<tokens::LocCap>,
        own_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<(), mysql::Error> {
        let mut db = self.pool.get_conn()?;
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();

        // XXX revealing all global tokens when a disguise is reversed
        let start = time::Instant::now();
        let mut diff_tokens = locked_token_ctrler.get_global_diff_tokens_of_disguise(did);
        let (dts, own_tokens) = locked_token_ctrler.get_user_tokens(
            did,
            &decrypt_cap,
            &HashSet::from_iter(diff_loc_caps.iter().cloned()),
            &HashSet::from_iter(own_loc_caps.iter().cloned()),
        );
        diff_tokens.extend(dts.iter().cloned());
        warn!(
            "Edna: Get tokens for reveal: {}",
            start.elapsed().as_micros()
        );
        let mut failed = false;

        // reverse REMOVE tokens first
        let start = time::Instant::now();
        for dwrapper in &diff_tokens {
            let d = edna_diff_token_from_bytes(&dwrapper.token_data);
            if dwrapper.did == did && !dwrapper.revealed && (d.update_type == REMOVE_GUISE || d.update_type == REMOVE_PRINCIPAL) {
                warn!("Reversing remove token {:?}\n", d);
                let revealed = d.reveal(&mut locked_token_ctrler, &mut db, self.stats.clone())?;
                if revealed {
                    warn!("Remove Token reversed!\n");
                    /*locked_token_ctrler.mark_diff_token_revealed(
                        did,
                        dwrapper,
                        &decrypt_cap,
                        &diff_loc_caps,
                        &own_loc_caps,
                    );*/
                } else {
                    failed = true;
                    warn!("Failed to reverse remove token");
                }
            }
        }

        for dwrapper in &diff_tokens {
            // only reverse tokens of disguise if not yet revealed
            let d = edna_diff_token_from_bytes(&dwrapper.token_data);
            if dwrapper.did == did && !dwrapper.revealed && d.update_type != REMOVE_GUISE && d.update_type != REMOVE_PRINCIPAL {
                warn!("Reversing token {:?}\n", d);
                let revealed = d.reveal(&mut locked_token_ctrler, &mut db, self.stats.clone())?;
                if revealed {
                    warn!("NonRemove Diff Token reversed!\n");
                    /*locked_token_ctrler.mark_diff_token_revealed(
                        did,
                        dwrapper,
                        &decrypt_cap,
                        &diff_loc_caps,
                        &own_loc_caps,
                    );*/
                } else {
                    failed = true;
                    warn!("Failed to reverse non-remove token");
                }
            }
        }

        for owrapper in &own_tokens {
            // XXX if we're not using ownership tokens from edna, then ignore reversal of ownership
            // links...
            match edna_own_token_from_bytes(&owrapper.token_data) {
                Err(_) => continue,
                Ok(d) => {
                    if d.did == did {
                        warn!("Reversing token {:?}\n", d);
                        let revealed =
                            d.reveal(&mut locked_token_ctrler, &mut db, self.stats.clone())?;
                        if revealed {
                            warn!("Decor Ownership Token reversed!\n");
                            /*locked_token_ctrler.mark_ownership_token_revealed(
                                did,
                                owrapper,
                                &decrypt_cap,
                                &own_loc_caps,
                            );*/
                        } else {
                            failed = true;
                        }
                    }
                }
            }
        }
        if !failed {
            locked_token_ctrler.cleanup_user_tokens(
                did,
                &decrypt_cap,
                &HashSet::from_iter(diff_loc_caps.iter().cloned()),
                &HashSet::from_iter(own_loc_caps.iter().cloned()),
                &mut db,
            );
        }

        warn!("Reveal tokens: {}", start.elapsed().as_micros());

        drop(locked_token_ctrler);
        self.end_disguise_action();
        Ok(())
    }

    pub fn apply(
        &mut self,
        disguise: Arc<disguise::Disguise>,
        decrypt_cap: tokens::DecryptCap,
        ownership_loc_caps: Vec<tokens::LocCap>,
    ) -> Result<
        (
            HashMap<(UID, DID), tokens::LocCap>,
            HashMap<(UID, DID), tokens::LocCap>,
        ),
        mysql::Error,
    > {
        let mut db = self.pool.get_conn()?;

        //let mut threads = vec![];
        let did = disguise.did;
        let start = time::Instant::now();
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        let global_diff_tokens = locked_token_ctrler.get_all_global_diff_tokens();
        let (_, ownership_tokens) = locked_token_ctrler.get_user_tokens(
            disguise.did,
            &decrypt_cap,
            &HashSet::new(),
            &HashSet::from_iter(ownership_loc_caps.iter().cloned()),
        );
        drop(locked_token_ctrler);
        warn!(
            "Edna: Get all user tokens for disguise: {}",
            start.elapsed().as_micros()
        );

        /*
         * Decor and modify
         */
        let start = time::Instant::now();
        for (table, transforms) in disguise.table_disguises.clone() {
            let mystats = self.stats.clone();
            let my_global_diff_tokens_to_modify = self.global_diff_tokens_to_modify.clone();
            let my_diff_tokens = global_diff_tokens.clone();
            let my_own_tokens = ownership_tokens.clone();
            let my_token_ctrler = self.token_ctrler.clone();

            // clone disguise fields
            let my_table_info = disguise.table_info.clone();
            let my_guise_gen = self.guise_gen.clone();

            // hashmap from item value --> transform
            let mut token_transforms: HashMap<DiffTokenWrapper, Vec<ObjectTransformation>> =
                HashMap::new();

            //threads.push(thread::spawn(move || {
            // XXX note: not tracking if we remove or decorrelate twice
            let locked_table_info = my_table_info.read().unwrap();
            let curtable_info = locked_table_info.get(&table).unwrap();
            let locked_guise_gen = my_guise_gen.read().unwrap();
            let my_transforms = transforms.read().unwrap();

            // handle Decor and Modify
            for t in &*my_transforms {
                let transargs = &*t.trans.read().unwrap();
                if let TransformArgs::Remove = transargs {
                    continue;
                }
                let preds = predicate::get_all_preds_with_owners(
                    &t.pred,
                    &curtable_info.owner_cols, // assume only one fk
                    &my_own_tokens,
                );
                for p in &preds {
                    let selection = predicate::pred_to_sql_where(p);
                    let selected_rows = get_query_rows_str(
                        &str_select_statement(&table, &selection),
                        &mut db,
                        mystats.clone(),
                    )
                    .unwrap();
                    warn!(
                        "ApplyPredDecor: Got {} selected rows matching predicate {:?}\n",
                        selected_rows.len(),
                        p
                    );
                    match transargs {
                        TransformArgs::Decor { fk_col, fk_name } => {
                            let mut new_parents = vec![];
                            for _ in 0..selected_rows.len() {
                                new_parents.push(self.create_new_pseudoprincipal());
                            }
                            let mut locked_token_ctrler = my_token_ctrler.lock().unwrap();
                            decor_items(
                                // disguise and per-thread state
                                did,
                                &mut locked_token_ctrler,
                                mystats.clone(),
                                // info needed for decorrelation
                                &table,
                                curtable_info,
                                &fk_name,
                                &fk_col,
                                &selected_rows,
                                new_parents,
                                &locked_guise_gen,
                                &mut db,
                            );
                            drop(locked_token_ctrler);
                        }
                        TransformArgs::Modify {
                            col,
                            generate_modified_value,
                            ..
                        } => {
                            let mut locked_token_ctrler = my_token_ctrler.lock().unwrap();
                            modify_items(
                                did,
                                t.global,
                                &mut locked_token_ctrler,
                                mystats.clone(),
                                &table,
                                curtable_info,
                                col,
                                (*(generate_modified_value))(""),
                                &selected_rows,
                                selection,
                                &mut db,
                            );
                            drop(locked_token_ctrler);
                        }
                        _ => (),
                    }

                    // ensure that matching diff tokens are updated
                    // TODO separate out predicating on global diff tokens completely?
                    for dwrapper in &my_diff_tokens {
                        let d = edna_diff_token_from_bytes(&dwrapper.token_data);
                        if dwrapper.is_global && predicate::diff_token_matches_pred(p, &table, &d) {
                            warn!("Apply: Inserting global token {:?} to update\n", d);
                            match token_transforms.get_mut(&dwrapper) {
                                Some(vs) => vs.push(t.clone()),
                                None => {
                                    token_transforms.insert(dwrapper.clone(), vec![t.clone()]);
                                }
                            }
                        }
                    }
                }
            }

            // save token transforms to perform
            let mut locked_tokens = my_global_diff_tokens_to_modify.write().unwrap();
            locked_tokens.extend(token_transforms);
            drop(locked_tokens);

            //warn!("Thread {:?} exiting", thread::current().id());
            //}));
        }
        warn!(
            "Edna: Execute modify/decor total: {}",
            start.elapsed().as_micros()
        );

        /*
         * REMOVE
         */
        // WE ONLY NEED GLOBAL DIFF TOKENS because we need to potentially modify them
        let start = time::Instant::now();
        self.execute_removes(
            disguise.clone(),
            &global_diff_tokens,
            &ownership_tokens,
            &mut db,
        );
        warn!(
            "Edna: Execute removes total: {}",
            start.elapsed().as_micros()
        );

        // wait until all mysql queries are done
        /*for jh in threads.into_iter() {
            match jh.join() {
                Ok(_) => (),
                Err(_) => warn!("Join failed?"),
            }
        }*/

        // modify global diff tokens all at once
        self.modify_global_diff_tokens(disguise);
        warn!("Disguiser: Performed Inserts");

        // any capabilities generated during disguise should be emailed
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        let loc_caps = locked_token_ctrler.save_and_clear(&mut db);
        drop(locked_token_ctrler);
        self.end_disguise_action();
        warn!("Edna: apply disguise: {}", start.elapsed().as_micros());
        Ok(loc_caps)
    }

    fn modify_global_diff_tokens(&mut self, disguise: Arc<Disguise>) {
        let start = time::Instant::now();
        let did = disguise.did;
        let uid = disguise.user.clone();

        // apply updates to each token (for now do sequentially)
        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
        let mut cols_to_update = vec![];
        for (twrapper, ts) in self.global_diff_tokens_to_modify.write().unwrap().iter() {
            let token = edna_diff_token_from_bytes(&twrapper.token_data);
            for t in ts {
                // we don't update global tokens if they've been disguised by a global
                // disguise---no information leakage here
                if t.global {
                    continue;
                }
                match &*t.trans.read().unwrap() {
                    TransformArgs::Decor { .. } => {
                        unimplemented!("No global decor tokens allowed!")
                    }
                    TransformArgs::Modify {
                        col,
                        generate_modified_value,
                        ..
                    } => {
                        let old_val = get_value_of_col(&token.old_value, &col).unwrap();
                        let new_val = (*(generate_modified_value))(&old_val);
                        // save the column to update for this item
                        cols_to_update.push(Assignment {
                            id: Ident::new(col.clone()),
                            value: Expr::Value(Value::String(new_val)),
                        });
                    }
                    TransformArgs::Remove => {
                        if !locked_token_ctrler
                            .remove_global_diff_token_wrapper(&uid, did, &twrapper)
                        {
                            warn!("Could not remove old disguise token!! {:?}", token);
                        }
                        // continue onto the next token, don't modify it!
                        continue;
                    }
                }
            }
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
            let mut new_token_wrapper = twrapper.clone();
            new_token_wrapper.token_data = edna_diff_token_to_bytes(&new_token);
            if !locked_token_ctrler.update_global_diff_token_from_old_to(
                &twrapper,
                &new_token_wrapper,
                Some((uid.clone(), did)),
            ) {
                warn!("Could not update old disguise token!! {:?}", token);
            }
        }
        drop(locked_token_ctrler);
        warn!(
            "Edna: modify global diff tokens: {}",
            start.elapsed().as_micros()
        );
    }

    fn execute_removes(
        &self,
        disguise: Arc<Disguise>,
        diff_tokens: &Vec<DiffTokenWrapper>,
        own_tokens: &Vec<OwnershipTokenWrapper>,
        db: &mut mysql::PooledConn,
    ) {
        warn!(
            "ApplyRemoves: removing objs for disguise {} with {} own_tokens\n",
            disguise.did,
            own_tokens.len()
        );
        let mut drop_me_later = vec![];
        for (table, transforms) in disguise.table_disguises.clone() {
            let mystats = self.stats.clone();
            let did = disguise.did;
            let mut locked_diff_tokens = self.global_diff_tokens_to_modify.write().unwrap();

            let locked_table_info = disguise.table_info.read().unwrap();
            let curtable_info = locked_table_info.get(&table).unwrap().clone();
            drop(locked_table_info);

            // REMOVES: do one loop to handle removes
            for t in &*transforms.read().unwrap() {
                if let TransformArgs::Remove = *t.trans.read().unwrap() {
                    let preds = predicate::get_all_preds_with_owners(
                        &t.pred,
                        &curtable_info.owner_cols,
                        &own_tokens,
                    );
                    warn!("Got preds {:?} with own_tokens {:?}\n", preds, own_tokens);
                    for p in &preds {
                        let start = time::Instant::now();
                        let selection = predicate::pred_to_sql_where(p);
                        let selected_rows = get_query_rows_str(
                            &str_select_statement(&table, &selection),
                            db,
                            mystats.clone(),
                        )
                        .unwrap();
                        let pred_items: HashSet<Vec<RowVal>> =
                            HashSet::from_iter(selected_rows.iter().cloned());
                        warn!(
                            "Edna: select items for remove {}: {:?}",
                            selection, pred_items
                        );

                        warn!(
                            "Edna: select items for remove {}: {}",
                            selection,
                            start.elapsed().as_micros()
                        );
                        warn!(
                            "ApplyPred: Got {} selected rows matching table {} predicate {:?}\n",
                            pred_items.len(),
                            table,
                            p
                        );

                        // BATCH REMOVE ITEMS
                        let start = time::Instant::now();
                        // XXX hack to delete user last...
                        let delstmt = format!("DELETE FROM {} WHERE {}", table, selection);
                        let locked_guise_gen = self.guise_gen.read().unwrap();
                        if locked_guise_gen.guise_name == table {
                            drop_me_later.push(delstmt);
                        } else {
                            helpers::query_drop(delstmt.to_string(), db, mystats.clone()).unwrap();
                            warn!(
                                "Edna: delete items {}: {}",
                                delstmt,
                                start.elapsed().as_micros()
                            );
                        }

                        // ITEM REMOVAL: ADD TOKEN RECORDS
                        let start = time::Instant::now();
                        let mut locked_token_ctrler = self.token_ctrler.lock().unwrap();
                        for i in &pred_items {
                            let ids = get_ids(&curtable_info.id_cols, i);

                            // TOKEN INSERT FOR REMOVAL
                            let mut token = new_delete_token_wrapper(
                                did,
                                table.to_string(),
                                ids.clone(),
                                i.to_vec(),
                                t.global,
                            );
                            for owner_col in &curtable_info.owner_cols {
                                token.uid = get_value_of_col(&i, &owner_col).unwrap();
                                if t.global {
                                    locked_token_ctrler.insert_global_diff_token_wrapper(&token);
                                } else {
                                    locked_token_ctrler.insert_user_diff_token_wrapper(&token);
                                }
                                // if we're working on a guise table (e.g., a users table)
                                // remove the user
                                if locked_guise_gen.guise_name == table {
                                    warn!(
                                        "Found item to delete from table {} that is guise",
                                        table
                                    );
                                    locked_token_ctrler
                                        .mark_principal_to_be_removed(&token.uid, token.did);
                                }
                            }
                        }
                        warn!(
                            "Edna: insert {} remove tokens: {}",
                            pred_items.len(),
                            start.elapsed().as_micros()
                        );
                        drop(locked_guise_gen);
                        drop(locked_token_ctrler);

                        // MARK MATCHING GLOBAL DIFF TOKENS FOR REMOVAL
                        let start = time::Instant::now();
                        for dwrapper in diff_tokens {
                            let dt = edna_diff_token_from_bytes(&dwrapper.token_data);
                            if dwrapper.is_global
                                && predicate::diff_token_matches_pred(&p, &table, &dt)
                            {
                                warn!("ApplyRemoves: Inserting global token {:?} to update\n", dt);
                                match locked_diff_tokens.get_mut(&dwrapper) {
                                    Some(vs) => vs.push(t.clone()),
                                    None => {
                                        locked_diff_tokens
                                            .insert(dwrapper.clone(), vec![t.clone()]);
                                    }
                                }
                            }
                        }
                        warn!(
                            "get matching global tokens to remove: {}",
                            start.elapsed().as_micros()
                        );
                        // we already removed the actual user/principal for any global tokens
                    }
                }
            }
        }

        let start = time::Instant::now();
        for delstmt in drop_me_later {
            helpers::query_drop(delstmt.to_string(), db, self.stats.clone()).unwrap();
            warn!(
                "Edna: delete user {}: {}",
                delstmt,
                start.elapsed().as_micros()
            );
        }
    }

    fn end_disguise_action(&self) {
        self.global_diff_tokens_to_modify.write().unwrap().clear();
        warn!("Disguiser: clear disguise records");
    }
}

/*
 * Also only used by higher-level disguise specs
 */
fn decor_items(
    did: DID,
    token_ctrler: &mut TokenCtrler,
    stats: Arc<Mutex<QueryStat>>,
    child_name: &str,
    child_name_info: &TableInfo,
    fk_name: &str,
    fk_col: &str,
    items: &Vec<Vec<RowVal>>,
    pseudoprincipals: Vec<(UID, Vec<RowVal>)>,
    guise_gen: &GuiseGen,
    db: &mut mysql::PooledConn,
) {
    warn!(
        "Thread {:?} starting decor {}",
        thread::current().id(),
        child_name
    );
    for (index, i) in items.iter().enumerate() {
        // TODO sort items by shared parent
        // then group by group-by-cols
        // for each group, create new PP and rewrite FKs

        /*
         * DECOR OBJECT MODIFICATIONS
         * A) insert guises for parents
         * B) update child to point to new guise
         * */

        // get ID of old parent
        let old_uid = get_value_of_col(&i, &fk_col).unwrap();
        warn!(
            "decor_obj {}: Creating guises for fkids {:?} {:?}",
            child_name, fk_name, old_uid,
        );

        let child_ids = get_ids(&child_name_info.id_cols, &i);

        // A. CREATE NEW PARENT
        let (new_uid, new_parent) = &pseudoprincipals[index];

        let start = time::Instant::now();
        // actually insert pseudoprincipal
        query_drop(
            format!(
                "INSERT INTO {} ({}) VALUES ({});",
                guise_gen.guise_name,
                new_parent
                    .iter()
                    .map(|rv| rv.column.clone())
                    .collect::<Vec<String>>()
                    .join(","),
                new_parent
                    .iter()
                    .map(|rv| rv.value.clone())
                    .collect::<Vec<String>>()
                    .join(","),
            ),
            db,
            stats.clone(),
        )
        .unwrap();
        warn!("Insert PP: {}", start.elapsed().as_micros());

        // actually register the anon principal, including saving an ownership token for the old uid
        // token is always inserted ``privately''
        let start = time::Instant::now();
        let own_token_bytes = edna_own_token_to_bytes(&new_edna_ownership_token(
            did,
            child_name.to_string(),
            child_ids,
            fk_name.to_string(),
            guise_gen.guise_id_col.clone(),
            fk_col.to_string(),
            old_uid.to_string(),
            new_uid.to_string(),
        ));
        token_ctrler.register_anon_principal(&old_uid, &new_uid, did, own_token_bytes, db);
        warn!("Register anon principal: {}", start.elapsed().as_micros());

        // B. UPDATE CHILD FOREIGN KEY
        let start = time::Instant::now();
        let i_select = get_select_of_row(&child_name_info.id_cols, &i);
        query_drop(
            Statement::Update(UpdateStatement {
                table_name: string_to_objname(&child_name),
                assignments: vec![Assignment {
                    id: Ident::new(fk_col.clone()),
                    value: Expr::Value(Value::Number(new_uid.clone())),
                }],
                selection: Some(i_select),
            })
            .to_string(),
            db,
            stats.clone(),
        )
        .unwrap();
        warn!("Update decor fk: {}", start.elapsed().as_micros());
    }
}

fn modify_items(
    did: DID,
    global: bool,
    token_ctrler: &mut TokenCtrler,
    stats: Arc<Mutex<QueryStat>>,
    table: &str,
    table_info: &TableInfo,
    col: &str,
    new_val: String,
    items: &Vec<Vec<RowVal>>,
    selection: String,
    db: &mut mysql::PooledConn,
) {
    warn!("Thread {:?} starting mod {}", thread::current().id(), table);

    let start = time::Instant::now();
    // update column for this item
    //error!("UPDATE {} SET {} = '{}' WHERE {}", table, col, new_val, selection);
    query_drop(
        format!(
            "UPDATE {} SET {} = '{}' WHERE {}",
            table, col, new_val, selection
        ),
        db,
        stats.clone(),
    )
    .unwrap();
    warn!("Update column for modify: {}", start.elapsed().as_micros());

    // TOKEN INSERT
    let start = time::Instant::now();
    for i in items {
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
            new_modify_token_wrapper(did, table.to_string(), ids, i.to_vec(), new_obj, global);
        for owner_col in &table_info.owner_cols {
            let owner_uid = get_value_of_col(&i, &owner_col).unwrap();
            update_token.uid = owner_uid.clone();
            if !global {
                token_ctrler.insert_user_diff_token_wrapper(&update_token);
            } else {
                token_ctrler.insert_global_diff_token_wrapper(&update_token);
            }
        }
    }
    warn!("Update token inserted: {}", start.elapsed().as_micros());
}
