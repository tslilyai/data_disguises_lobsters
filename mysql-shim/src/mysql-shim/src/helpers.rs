use sql_parser::ast::*;

fn trim_quotes(s: &str) -> &str {
    let mut s = s;
    if s.ends_with('"') && s.starts_with('"') {
        s = &s[1..s.len() - 1]
    } 
    s
}

pub fn string_to_objname(s: &str) -> ObjectName {
    let idents = s
        .split(".")
        .into_iter()
        .map(|i| Ident::new(trim_quotes(i)))
        .collect();
    let obj = ObjectName(idents);
    obj
}

pub fn objname_subset_match_range(obj: &Vec<Ident>, dt: &str) -> Option<(usize, usize)> {
    let dt_split : Vec<&str> = dt.split(".").collect();
  
    let mut i = 0;
    let mut j = 0;
    while j < obj.len() {
        if i < dt_split.len() {
            if dt_split[i] == obj[j].to_string() {
                i+=1;
            } else {
                // reset comparison from beginning of dt
                i = 0; 
            }
            j+=1;
        } else {
            break;
        }
    }
    if i == dt_split.len() {
        return Some((j-i, j-1));
    } 
    None
}
