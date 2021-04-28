use std::collections::HashMap;

pub fn merge_vector_hashmaps(
    h1: &mut HashMap<String, Vec<String>>,
    h2: &mut HashMap<String, Vec<String>>,
) {
    for (k, vs1) in h1.iter_mut() {
        if let Some(mut vs2) = h2.get_mut(k) {
            vs1.append(&mut vs2);
        }
    }
    for (k, vs2) in h2.iter_mut() {
        if let Some(vs1) = h1.get_mut(k) {
            vs1.append(vs2);
        } else {
            h1.insert(k.to_string(), vs2.clone());
        }
    }
}

