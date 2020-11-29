use crate::views::{View, TableColumnDef, HashedRowPtrs, HashedRowPtr, RowPtrs};
use crate::{helpers, predicates, predicates::IndexedPredicate, predicates::NamedPredicate};
use log::{warn, debug};
use std::collections::{HashSet, BTreeMap};
use std::cmp::Ordering;
use std::time;
use ordered_float::*;

pub fn get_ordered_rptrs_of_view(v: &View, order_by_indices: &Vec<usize>) -> RowPtrs {
    debug!("{}: getting ordered rptrs of view {:?}", v.name, order_by_indices);
    let mut rptrs = vec![];
    let mut btree : BTreeMap<OrderedFloat<f64>, HashedRowPtrs>  = BTreeMap::new();
    for (_, rptr) in v.rows.borrow().iter() {
        let hrptr = HashedRowPtr::new(rptr.clone(), v.primary_index);
        let key = OrderedFloat(helpers::parser_val_to_f64(&rptr.borrow()[order_by_indices[0]]));
        if let Some(treeptrs) = btree.get_mut(&key) {
            treeptrs.insert(hrptr.clone());
        } else {
            let mut hs = HashSet::new();
            hs.insert(hrptr.clone());
            btree.insert(key, hs);
        }
    }
    // TODO asc vs desc
    for (_, hrptrs) in btree.iter() {
        let mut unhashed : RowPtrs = hrptrs.iter().map(|rptr| rptr.row().clone()).collect();
        if order_by_indices.len() > 1 {
            unhashed.sort_by(|r1, r2| {
                for obi in order_by_indices {
                    match helpers::parser_vals_cmp(&r1.borrow()[*obi], &r2.borrow()[*obi]) {
                        Ordering::Equal => continue,
                        o => return o,
                    }
                }
                Ordering::Equal
            });
        }
        warn!("unhashed is {:?}", unhashed);
        rptrs.append(&mut unhashed);
    }
    rptrs
}

/*
 * Returns matching rows and any predicates which have not yet been applied
 */
pub fn get_ordered_rptrs_matching_preds(v: &View, columns: &Vec<TableColumnDef>, predsets: &Vec<Vec<NamedPredicate>>, order_by_indices: &Vec<usize>) 
    -> RowPtrs
{
    debug!("{}: getting ordered rptrs of preds {:?}", v.name, predsets);
    let start = time::Instant::now();

    let mut matching = BTreeMap::new();
    let mut failed_predsets = vec![];
    for preds in predsets {
        let mut failed = vec![];
        let mut indexed_preds = vec![]; 
        for p in preds {
            if let Some(ip) = p.to_indexed_predicate(columns) {
                indexed_preds.push(ip);
            } else {
                failed.push(p.clone());
            }
        }
        if !(failed.is_empty()) {
            failed_predsets.push(failed);
        }
        let btree = get_predicated_rptrs(&indexed_preds, v, order_by_indices[0]);
        if matching.is_empty() {
            matching = btree;
        } else {
            for (key, hrptrs) in btree.iter() {
                if let Some(treeptrs) = matching.get_mut(key) {
                    treeptrs.extend(hrptrs.clone());
                } else {
                    matching.insert(*key, hrptrs.clone());
                }
            }
        }    
    }
    
    let mut rptrs = vec![];
    // TODO asc vs desc
    for (_, hrptrs) in matching.iter() {
        let mut unhashed : RowPtrs = hrptrs.iter().map(|rptr| rptr.row().clone()).collect();
        if order_by_indices.len() > 1 {
            unhashed.sort_by(|r1, r2| {
                for obi in order_by_indices {
                    match helpers::parser_vals_cmp(&r1.borrow()[*obi], &r2.borrow()[*obi]) {
                        Ordering::Equal => continue,
                        o => return o,
                    }
                }
                Ordering::Equal
            });
        }
        rptrs.append(&mut unhashed);
    }

    let dur = start.elapsed();
    warn!("get ordered rptrs matching preds duration {}us", dur.as_micros());
    rptrs
}

pub fn get_predicated_rptrs(preds: &Vec<IndexedPredicate>, v: &View, order_by_index: usize) -> BTreeMap<OrderedFloat<f64>, HashedRowPtrs>
{
    use IndexedPredicate::*;

    let mut not_applied = vec![];
    let mut matching = None; 

    // first try to narrow down by a single index select
    for pred in preds {
        if let ColValEq{index, val, neg} = pred {
            // we scan all pointers if it's negated anyway...
            // don't do more than one intiial select at first
            if *neg || matching.is_some() {
                not_applied.push(pred);
                continue;
            } 
            if let Some(hrptrs) = v.get_indexed_rptrs_of_col(*index, &val.to_string()) {
                matching = Some(hrptrs);
                continue;
            } 
        }
        not_applied.push(pred);
    }
    // next narrow down by InList select
    if matching.is_none() {
        not_applied.clear();
        for pred in preds {
            if let ColValsEq{index, vals, neg} = pred {
                if *neg || matching.is_some() {
                    not_applied.push(pred);
                    continue;
                } 
                if v.is_indexed_col(*index) {
                    let mut hrptrs = HashSet::new();
                    for lv in vals {
                        hrptrs.extend(v.get_indexed_rptrs_of_col(*index, &lv.to_string()).unwrap());
                    }
                    matching = Some(hrptrs);
                    continue;
                } 
            } 
            not_applied.push(pred);
        }
    }
    if let Some(matching) = matching {
        get_predicated_rptrs_from_matching(&not_applied, &matching, order_by_index)
    } else {
        // if we got to this point we have to linear scan and apply all predicates :\
        get_predicated_rptrs_from_view(&not_applied, v, order_by_index)
    }
}

pub fn get_predicated_rptrs_from_view(preds: &Vec<&IndexedPredicate>, v: &View, order_by_index: usize) -> BTreeMap<OrderedFloat<f64>, HashedRowPtrs> 
{
    warn!("Applying predicates {:?} to all view rows", preds);
    let mut btree = BTreeMap::new();
    if preds.is_empty() {
        return btree;
    }
    'rowloop: for (_, rptr) in v.rows.borrow().iter() {
        let row = rptr.borrow();
        for p in preds {
            if !predicates::pred_matches_row(&row, p) {
                continue 'rowloop;
            }
        } 
        let hrptr = HashedRowPtr::new(rptr.clone(), v.primary_index);
        let key = OrderedFloat(helpers::parser_val_to_f64(&row[order_by_index]));
        if let Some(treeptrs) = btree.get_mut(&key) {
            treeptrs.insert(hrptr.clone());
        } else {
            let mut hs = HashSet::new();
            hs.insert(hrptr.clone());
            btree.insert(key, hs);
        }
    }
    btree
}

pub fn get_predicated_rptrs_from_matching(preds: &Vec<&IndexedPredicate>, matching: &HashedRowPtrs, order_by_index: usize) -> BTreeMap<OrderedFloat<f64>, HashedRowPtrs>
{
    warn!("Applying predicates {:?} to {} matching rows", preds, matching.len());
    let mut btree = BTreeMap::new();
    if preds.is_empty() {
        return btree;
    }
    'rowloop: for hrp in matching.iter() {
        let row = hrp.row().borrow();
        for p in preds {
            if !predicates::pred_matches_row(&row, p) {
                continue 'rowloop
            }
        }
        let key = OrderedFloat(helpers::parser_val_to_f64(&row[order_by_index]));
        if let Some(treeptrs) = btree.get_mut(&key) {
            treeptrs.insert(hrp.clone());
        } else {
            let mut hs = HashSet::new();
            hs.insert(hrp.clone());
            btree.insert(key, hs);
        }
    }
    btree
}
