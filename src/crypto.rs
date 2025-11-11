use ed25519_dalek::{SigningKey, VerifyingKey, ed25519::signature::SignerMut};
use rand::{rngs::OsRng, Rng};
use crate::{message::ConnectMessage, node::Peer, utils::now, Nonce};

pub fn signing_key() -> SigningKey {
    let mut rng = OsRng;
    SigningKey::generate(&mut rng)
}

pub fn nonce() -> Nonce {
    let mut nonce = Nonce::default();
    let mut rng = OsRng;
    rng.fill(&mut nonce.0);
    nonce
}

pub fn hash_sign_connect_msg(peer: Peer, nonce: Nonce, sk: &mut SigningKey) -> ConnectMessage {
    let addr_string = peer.addr.to_string();
    let ts = now();
    let mut hasher = blake3::Hasher::new();
    hasher.update(&peer.id);
    hasher.update(addr_string.as_bytes());
    hasher.update(&ts.to_be_bytes());
    hasher.update(&nonce.0);
    let hash = hasher.finalize();
    let sig = sk.sign(hash.as_bytes());
    ConnectMessage { peer: peer, timestamp: ts, nonce: nonce, hash: hash, sig: sig }
}

pub fn verify_connect_msg(msg: ConnectMessage) -> bool {
    let peer = msg.peer;
    let pk = peer.id;
    let addr_string = peer.addr.to_string();
    let nonce = msg.nonce;
    let hash = msg.hash;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&pk);
    hasher.update(addr_string.as_bytes());
    hasher.update(&msg.timestamp.to_be_bytes());
    hasher.update(&nonce.0);
    let computed_hash = hasher.finalize();
    if computed_hash != hash {
        return false;
    }

    let pk = VerifyingKey::from_bytes(&pk).unwrap();
    pk.verify_strict(hash.as_bytes(), &msg.sig).is_ok()
}
