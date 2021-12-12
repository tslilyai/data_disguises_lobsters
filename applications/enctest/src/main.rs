use edna::tokens::*;
use rand::{RngCore};
use rsa::pkcs1::{ToRsaPrivateKey};
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::time;
use std::iter::repeat;
use std::fs::{File};
use std::io::Write;

const RSA_BITS: usize = 2048;

fn init_logger() {
    let _ = env_logger::builder()
        // Include all events in tests
        .filter_level(log::LevelFilter::Error)
        // Ensure events are captured by `cargo test`
        .is_test(true)
        // Ignore errors initializing the logger if tests race to configure it
        .try_init();
}


fn main() {
    init_logger();
    let mut rng = rand::thread_rng();
    let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
    let privkey_vec = private_key.to_pkcs1_der().unwrap().as_der().to_vec();
    let pub_key = RsaPublicKey::from(&private_key);
    
    let filename = format!("enc_stats.csv");
    let mut file = File::create(filename).unwrap();
 
    let size = 100;
    for i in 1..1001 {
        let mut bytes: Vec<u8> = repeat(0u8).take(i*size).collect();
        rng.fill_bytes(&mut bytes[..]);
        let start = time::Instant::now();
        let enc = EncData::encrypt_with_pubkey(&pub_key, &bytes);
        let enc_elapsed = start.elapsed().as_micros();

        let start = time::Instant::now();
        enc.decrypt_encdata(&privkey_vec);
        let dec_elapsed = start.elapsed().as_micros();
        file.write(format!("{},{},{}\n", i*size, enc_elapsed, dec_elapsed).as_bytes()).unwrap();
    }
}
