use tokio::net::TcpListener;
use std::io;
use ed25519_dalek::VerifyingKey;
use tokio::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref KNOWN_PEERS: Mutex<Vec<Node>> = Mutex::new(vec![]);
    pub static ref CONNECTED_PEERS: Mutex<Vec<Node>> = Mutex::new(vec![]);
}

#[derive(Debug)]
pub struct Node {
    pub id: VerifyingKey,
    pub addr: String
}

impl Node {
    pub async fn join_network(&self, listener: TcpListener) -> io::Result<()> {
        loop {
            println!("tié le goat");
            let (socket, addr) = listener.accept().await?;
        }
    }
}
