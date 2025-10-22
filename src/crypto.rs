use ed25519_dalek::{SigningKey};
use rand::rngs::OsRng;

pub fn signing_key() -> SigningKey {
    let mut rng = OsRng;
    SigningKey::generate(&mut rng)
}
