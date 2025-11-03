use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::io;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::message::*;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Node {
    pub id: VerifyingKey,
    pub addr: SocketAddr
}

#[derive(Debug, Clone)]
pub struct SharedState {
    pub known_peers: Arc<RwLock<HashSet<Node>>>,
    pub connected_peers: Arc<RwLock<HashMap<Node, u64>>>
}

impl Node {
    pub async fn join_network(&self, listener: TcpListener, sk: SigningKey) -> io::Result<()> {
        let state = SharedState {
            known_peers: Arc::new(RwLock::new(HashSet::new())),
            connected_peers: Arc::new(RwLock::new(HashMap::new()))
        };
        let state_clone = state.clone();

        // p2p communication
        let _listener_task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let state = state_clone.clone();
                let sk = sk.clone();
                let _connection_task = tokio::spawn(async move {
                    handle_connection(&mut socket, state, sk).await
                }).await?;
            }
            #[allow(unreachable_code)]
            Ok::<_, io::Error>(())
        }).await?;

        // Keep Alive
        loop {
            let state = state.clone();
            keep_alive(state.clone()).await?;
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

async fn keep_alive(state: SharedState) -> io::Result<()> {
    //TODO
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
        Message::Connection(node) => Ok(()),
        Message::GossipTx(tx) => Ok(()),
        Message::GossipBlock(block_header) => Ok(()),
        Message::NewBlock(block) => Ok(()),
        Message::GetBlock(block_digest) => Ok(()),
        Message::GetBlocks(range) => Ok(()),
    }
}
