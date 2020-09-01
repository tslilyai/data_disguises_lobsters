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
