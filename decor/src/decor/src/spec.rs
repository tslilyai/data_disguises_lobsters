use crate::{helpers, types::*};
use log::warn;
use sql_parser::ast::*;

/*
 * Given:
 * - a set of disguises
 * - the application privacy goals of each disguise
 * - the reversibility of each disguise
 *
 * Determine:
 * - the "merge order" of each disguise's predicated transformation
 * - which orderings result in unresolveable conflicts (e.g., irreversible confanon before GDPR)
 */

pub fn order_transforms(
    transforms: &mut Vec<Transform>,
) -> Result<Vec<&mut Transform>, &'static str> {
    use Transform::*;
    let mut sorted = vec![];
    let mut sorted_rem = vec![];
    let mut sorted_mod = vec![];
    let mut sorted_dec = vec![];

    // if a predicate between a modify and a decor conflicts, 
    // we want to *both* decorrelate and modify the object
    // developer can tell us if it's ok to just do one and not the other?
    //  e.g., change timestamps to 0 and anonymize --> they're two forms of modification, really
    //  decor WHERE timestamp < x
    //  modify timestamp = 0 WHERE author = UID
    //  ==> decor + modify timestamp WHERE author = UID and timestamp < x
    //
    // we need to have some way to apply more than one transformation to objects...
    // get all objects, transformations on objects that are the "same", update them together?
    for t in transforms {
        match t {
            // Any remove transformations are performed first in order
            // Note: predicates on the same attributes could technically be disrupted (i.e., if
            // predicated on the count of number of matching elements)
            Remove => {
                sorted_rem.push(t);
            }
            Modify { col, ..} => {
                sorted_mod.push(t);
            }
            Decor { referencer_col, fk_name, fk_col } => {
                sorted_dec.push(t);
            }
        }
    }
    sorted.append(&mut sorted_rem);
    sorted.append(&mut sorted_mod);
    sorted.append(&mut sorted_dec);
    Ok(sorted) 
}

pub fn merge_disguises(
    disguises: &mut Vec<Disguise>,
) -> bool {
    false
}
 
