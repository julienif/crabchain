use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::io;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::{message::*, KEEP_ALIVE};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: VerifyingKey,
    pub addr: SocketAddr
}

#[derive(Debug)]
pub struct State {
    pub known_peers: RwLock<HashSet<Node>>,
    pub connected_peers: RwLock<HashMap<Node, u64>>
}

impl Default for State {
    fn default() -> Self {
        State {
            known_peers: RwLock::new(HashSet::new()),
            connected_peers: RwLock::new(HashMap::new())

        }
    }
}

pub type SharedState = Arc<State>;

impl Node {
    pub async fn join_network(&self, listener: TcpListener, sk: SigningKey) -> io::Result<()> {
        let state = Arc::new(State::default());
        let state_clone = state.clone();
        let sk_clone = sk.clone();

        // p2p communication
        let _listener_task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let state = state_clone.clone();
                let sk = sk_clone.clone();
                let _connection_task = tokio::spawn(async move {
                    handle_connection(&mut socket, state, sk).await
                }).await?;
            }
            #[allow(unreachable_code)]
            Ok::<_, io::Error>(())
        }).await?;

        // Keep Alive
        loop {
            keep_alive(state.clone(), sk.clone()).await?;
        }
        
        #[allow(unreachable_code)]
        Ok(())
    }
}

pub async fn handle_connection(socket: &mut TcpStream, state: SharedState, sk: SigningKey)
-> io::Result<()> {
    let data = read_socket(socket).await?;

    match deserialize(data) {
        Ok(msg) => process(msg, socket, state, sk).await,
        Err(e) => Err(e)
    }
}

pub async fn keep_alive(state: SharedState, sk: SigningKey) -> io::Result<()> {
    tokio::time::sleep(KEEP_ALIVE).await;
    let state = state.clone();
    let peers: Vec<Node> = {
        let peers = state.connected_peers.read()
            .map_err(|_| io::Error::other("Connected peers poisoined"))?;
        peers.keys().cloned().collect()
    };
    for node in peers.iter() {
        ping(node.addr, state.clone(), sk.clone()).await?;
    }
    Ok(())
}

async fn process(msg: Message, socket: &mut TcpStream, state: SharedState, sk: SigningKey) -> io::Result<()> {
    //TODO
    match msg {
        Message::Ping => {
            println!("ping!");
            pong(socket).await
        },
        Message::Pong => {
            println!("pong!");
            Ok(())
        },
        Message::Hello(node) => Ok(()),
        Message::GossipTx(tx) => Ok(()),
        Message::GossipBlock(block_header) => Ok(()),
        Message::NewBlock(block) => Ok(()),
        Message::GetBlock(block_digest) => Ok(()),
        Message::GetBlocks(range) => Ok(()),
    }
}
