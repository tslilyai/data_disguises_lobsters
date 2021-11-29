use crate::helpers::*;
use log::warn;
use mysql::prelude::*;
use mysql::Opts;
use rand::rngs::OsRng;
use rsa::pkcs1::{FromRsaPrivateKey, FromRsaPublicKey, ToRsaPublicKey};
use rsa::{RsaPrivateKey, RsaPublicKey};

const RSA_BITS: usize = 2048;
const KEY_PAIRS_TABLE: &'static str = "KEY_PAIRS_TABLE";
const KEY_PAIRS_DB: &'static str = "KEY_PAIRS_DB";

fn generate_keys() -> mysql::Conn {
    let mut rng = OsRng;
    let nkeys = 100000;
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
    let mut db = mysql::Conn::new(Opts::from_url(&url).unwrap()).unwrap();
    db.query_drop(&format!("DROP DATABASE IF EXISTS {};", KEY_PAIRS_DB))
        .unwrap();
    db.query_drop(&format!("CREATE DATABASE {};", KEY_PAIRS_DB))
        .unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db(&format!("{}", KEY_PAIRS_DB)), true);
    db.query_drop("SET max_heap_table_size = 4294967295;")
        .unwrap();
    let createq = format!(
        "CREATE TABLE IF NOT EXISTS {} (pubkey varchar(1024), privkey varchar(1024)) ENGINE = InnoDB;",
        KEY_PAIRS_TABLE);
    db.query_drop(&createq).unwrap();

    let mut values = vec![];
    for _ in 0..nkeys {
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&private_key);
        let pubkey_vec = pub_key.to_pkcs1_der().unwrap().as_der().to_vec();
        let privkey_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
        values.push(format!(
            "(\'{}\', \'{}\')",
            serde_json::to_string(&pubkey_vec).unwrap(),
            serde_json::to_string(&privkey_vec).unwrap(),
        ));
    }
    let insert_q = format!(
        "INSERT INTO {} VALUES {};",
        KEY_PAIRS_TABLE,
        values.join(", ")
    );
    warn!("Insert q {}", insert_q);
    db.query_drop(&insert_q).unwrap();
    db
}

pub fn get_keys() -> Result<Vec<(RsaPublicKey, RsaPrivateKey)>, mysql::Error> {
    let mut keys = vec![];
    let url = format!("mysql://tslilyai:pass@127.0.0.1/{}", KEY_PAIRS_DB);
    let mut db = match mysql::Conn::new(Opts::from_url(&url).unwrap()) {
        Ok(db) => db,
        Err(_) => generate_keys(),
    };

    let res = db.query_iter(&format!("SELECT * FROM {}", KEY_PAIRS_TABLE))?;
    for row in res {
        let vals = row.unwrap().unwrap();
        let pubkey_bytes: Vec<u8> = serde_json::from_str(&mysql_val_to_string(&vals[0])).unwrap();
        let privkey_bytes: Vec<u8> = serde_json::from_str(&mysql_val_to_string(&vals[1])).unwrap();
        let pubkey = RsaPublicKey::from_pkcs1_der(&pubkey_bytes).unwrap();
        let privkey = RsaPrivateKey::from_pkcs1_der(&privkey_bytes).unwrap();
        keys.push((pubkey, privkey));
    }
    Ok(keys)
}
