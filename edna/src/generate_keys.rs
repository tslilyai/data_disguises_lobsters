use crate::helpers::*;
use log::warn;
use mysql::prelude::*;
use mysql::Opts;
use rsa::pkcs1::{FromRsaPrivateKey, FromRsaPublicKey, ToRsaPublicKey, ToRsaPrivateKey};
use rsa::{RsaPrivateKey, RsaPublicKey};

const RSA_BITS: usize = 2048;
const KEY_PAIRS_TABLE: &'static str = "KeyPairsTable";
const KEY_PAIRS_DB: &'static str = "KeyPairsDB";

pub fn generate_keys() -> Result<Vec<(RsaPrivateKey, RsaPublicKey)>, mysql::Error> {
    let mut rng = rand::thread_rng();
    let nkeys = 10000;
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
    let mut db = mysql::Conn::new(Opts::from_url(&url).unwrap())?;
    db.query_drop(&format!("DROP DATABASE IF EXISTS {};", KEY_PAIRS_DB))
        .unwrap();
    db.query_drop(&format!("CREATE DATABASE {};", KEY_PAIRS_DB))
        .unwrap();
    assert_eq!(db.ping(), true);
    assert_eq!(db.select_db(&format!("{}", KEY_PAIRS_DB)), true);
    db.query_drop("SET max_heap_table_size = 4294967295;")
        .unwrap();
    let createq = format!(
        "CREATE TABLE IF NOT EXISTS {} (pubkey varchar(2048), privkey varchar(2048)) ENGINE = InnoDB;",
        KEY_PAIRS_TABLE);
    db.query_drop(&createq)?;

    let mut keys = vec![];
    let mut values = vec![];
    for i in 0..nkeys {
        let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
        let privkey_vec = base64::encode(private_key.to_pkcs1_der().unwrap().as_der().to_vec());
        warn!("Insert privkey bytes {:?}", privkey_vec);
        let pub_key = RsaPublicKey::from(&private_key);
        let pubkey_vec = base64::encode(pub_key.to_pkcs1_der().unwrap().as_der().to_vec());
        keys.push((private_key, pub_key));
        warn!("Inserting pubkey bytes {:?}", pubkey_vec);
        values.push(format!(
            "(\'{}\', \'{}\')",
            pubkey_vec,
            privkey_vec,
        ));
        if i % 1000 == 0 {
            let insert_q = format!(
                "INSERT INTO {} VALUES {};",
                KEY_PAIRS_TABLE,
                values.join(", ")
            );
            warn!("Insert q {}", insert_q);
            db.query_drop(&insert_q)?;
            values = vec![];
        }
    }
    Ok(keys)
}

pub fn get_keys() -> Result<Vec<(RsaPrivateKey, RsaPublicKey)>, mysql::Error> {
    let mut keys = vec![];
    let url = format!("mysql://tslilyai:pass@127.0.0.1");
    let mut db = mysql::Conn::new(Opts::from_url(&url).unwrap()).unwrap();
    if !db.select_db(&format!("{}", KEY_PAIRS_DB)) {
        return generate_keys();
    }
    let res = db.query_iter(&format!("SELECT * FROM {}", KEY_PAIRS_TABLE))?;
    for row in res {
        let vals = row.unwrap().unwrap();
        let pubkey_bytes: Vec<u8> = base64::decode(&mysql_val_to_string(&vals[0])).unwrap();
        let privkey_bytes: Vec<u8> = base64::decode(&mysql_val_to_string(&vals[1])).unwrap();
        warn!("Got pubkey bytes {:?}", pubkey_bytes);
        warn!("Got privkey bytes {:?}", privkey_bytes);
        let pubkey = RsaPublicKey::from_pkcs1_der(&pubkey_bytes).unwrap();
        let privkey = RsaPrivateKey::from_pkcs1_der(&privkey_bytes).unwrap();
        keys.push((privkey, pubkey));
    }
    if keys.is_empty() {
        warn!("Generating keys");
        return generate_keys();
    }
    warn!("Got {} keys", keys.len());
    Ok(keys)
}
