use edna::tokens::*;
use rand::{rngs::OsRng, RngCore};
use rsa::pkcs1::{FromRsaPrivateKey, FromRsaPublicKey, ToRsaPublicKey};
use rsa::{PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey};
use std::time;
use log::error;

fn main() {
    let private_key = RsaPrivateKey::new(&mut rng, RSA_BITS).expect("failed to generate a key");
    let privkey_vec = base64::encode(private_key.to_pkcs1_der().unwrap().as_der().to_vec());
    let pub_key = RsaPublicKey::from(&private_key);
    let pubkey_vec = base64::encode(pub_key.to_pkcs1_der().unwrap().as_der().to_vec());

    let size = 2048;
    for i in range(10000) {
        let bytes = rand::thread_rng().gen::<[u8; i*size]>();
        let start = time::Instant::now();
        let enc = EncData::encrypt_with_pubkey(&pub_key, &bytes);
        let enc_elapsed = start.elapsed().as_micros();

        let start = time::Instant::now();
        edata.decrypt_encdata(&privkey_vec);
        let dec_elapsed = start.elapsed().as_micros();
        error!("{},{},{}", i*size, enc_elapsed, dec_elapsed)
    }
}
