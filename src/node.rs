use std::clone;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::{io, time};
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::crypto::{signing_key, verify_connect_msg};
use crate::utils::{now, since};
use crate::{message::*, Nonce, NonceType, CHALLENGED, KEEP_ALIVE, TS_VALID};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub id: VerifyingKey,
    pub addr: SocketAddr
}

impl Peer {
    fn new(id: VerifyingKey, addr: SocketAddr) -> Self {
        Peer {
            id: id,
            addr: addr
        }
    }
}

#[derive(Debug)]
pub struct State {
    pub known_peers: RwLock<HashSet<Peer>>,
    pub connected_peers: RwLock<HashMap<Peer, u64>>
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

#[derive(Debug, Clone)]
pub struct Node {
    pub sk: SigningKey,
    pub peer: Peer,
    pub state: SharedState,
    nonces: Arc<RwLock<HashSet<(Nonce, u64)>>>,
    pub sent_nonces: Arc<RwLock<HashMap<SocketAddr, (Nonce, u64)>>>,
    pub recv_nonces: Arc<RwLock<HashMap<SocketAddr, (Nonce, u64)>>>,
}

impl Node {
    pub fn new(addr: SocketAddr) -> Self {
        let sk = signing_key();
        Node { 
            sk: sk.clone(),
            peer: Peer::new(sk.verifying_key(), addr),
            state: Arc::new(State::default()),
            nonces: Arc::new(RwLock::new(HashSet::new())),
            sent_nonces: Arc::new(RwLock::new(HashMap::new())),
            recv_nonces: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    #[allow(unreachable_code)]
    pub async fn join_network(self, listener: TcpListener) -> io::Result<()> {
        let self_node_clone = self.clone();

        // p2p communication
        let _listener_task = tokio::spawn(async move {
            loop {
                let self_node = self_node_clone.clone();
                let (mut socket, _) = listener.accept().await?;
                let _connection_task = tokio::spawn(async move {
                    self_node.handle_connection(&mut socket).await
                }).await?;
            }
            Ok::<_, io::Error>(())
        });

        let self_node_clone = self.clone();
        // Keep Alive
        let _keepalive_task = tokio::spawn(async move {
            loop {
                let self_node = self_node_clone.clone();
                let _keep_alive = self_node.keep_alive().await;
            }
        });

        let self_node_clone = self.clone();
        let _challenged_task = tokio::spawn(async move {
            loop {
                let self_node = self_node_clone.clone();
                let _challenged_task = self_node.challenged().await;
            }
        });

        //TODO Hello loop to known_peers if connected_peers_len < MAX and init known_peers from seed
        
        Ok(())
    }

    pub async fn handle_connection(self, socket: &mut TcpStream)
    -> io::Result<()> {
        let data = read_socket(socket).await?;
        time::sleep(Duration::from_secs(1)).await;
    
        match deserialize(data) {
            Ok(msg) => self.process(msg, socket).await,
            Err(e) => Err(e)
        }
    }
    
    async fn keep_alive(self) -> io::Result<()> {
        tokio::time::sleep(KEEP_ALIVE).await;
        let state = self.state.clone();
        let peers: Vec<Peer> = {
            let peers = state.connected_peers.read()
                .map_err(|_| io::Error::other("Connected peers poisoined"))?;
            peers.keys().cloned().collect()
        };
        for peer in peers.iter() {
            ping(self.clone(), peer.addr).await?;
        }
        Ok(())
    }

    async fn challenged(self) ->io::Result<()> {
        tokio::time::sleep(CHALLENGED).await;
        let nonces_and_targets: Vec<(Nonce, SocketAddr)> = {
            let mut recv_nonces = self.recv_nonces.write()
                .map_err(|_| io::Error::other("Sent nonces poisoined"))?;
            recv_nonces.retain(|_, (_, ts)| since(*ts) < TS_VALID);
            recv_nonces.iter()
                .map(|(addr, (nonce, _))| (*nonce, *addr))
                .collect()
        };
        for (nonce, target) in nonces_and_targets.iter() {
            let node_self_clone = self.clone();
            let connected_peers_len = {
                let node_self = node_self_clone.clone();
                let state = node_self.state.clone();
                let connected_peers = state.connected_peers.read()
                    .map_err(|_| io::Error::other("Connected peers poisoined"))?;
                connected_peers.len()
            };
            if connected_peers_len < crate::MAX_PEERS {
                accept(node_self_clone, *nonce, *target).await?;
            }
        }
        Ok(())
    }

    async fn process(self, msg: Message, socket: &mut TcpStream) -> io::Result<()> {
        //TODO
        // self is needed when we expect a response
        match msg {
            Message::Ping => {
                println!("ping!");
                pong(socket).await
            },
            Message::Pong => {
                println!("pong!");
                Ok(())
            },
            Message::Hello(peer) => {
                println!("hello! {:?}", peer.addr);
                challenge(self, socket, *peer).await
            },
            Message::Challenge(nonce_peer_tuple) => {
                let nonce = nonce_peer_tuple.0;
                let peer = nonce_peer_tuple.1;
                println!("challenge: {:?}", peer.addr);
                self.add_sent_nonce(nonce, peer.addr, NonceType::Received).await
                //accept(self, nonce, peer).await
            },
            Message::Accept(connect_message) => {
                println!("accept");
                let connect_message = *connect_message;
                let peer = connect_message.peer;
                let ts = connect_message.timestamp;
                let nonce = connect_message.nonce;
                if verify_connect_msg(connect_message)
                    && since(ts) < TS_VALID 
                        && self.clone().match_nonce(peer, nonce, NonceType::Sent).await? {
                    connect(self, peer, socket, nonce).await
                } else {
                    Ok(()) // corrupted msg ignored
                }
            },
            Message::Connect(connect_message) => {
                println!("connect");
                let connect_message = *connect_message;
                let peer = connect_message.peer;
                let ts = connect_message.timestamp;
                let nonce = connect_message.nonce;
                if verify_connect_msg(connect_message)
                    && since(ts) < TS_VALID 
                        && self.clone().match_nonce(peer, nonce, NonceType::Received).await? {
                    self.add_peer(peer).await
                } else {
                    Ok(()) // corrupted msg ignored
                }
            },
            Message::GossipTx(tx) => Ok(()),
            Message::GossipBlock(block_header) => Ok(()),
            Message::GossipPeer(peer) => Ok(()),
            Message::NewBlock(block) => Ok(()),
            Message::GetBlock(block_digest) => Ok(()),
            Message::GetBlocks(range) => Ok(()),
        }
    }

    async fn add_peer(self, new_peer: Peer) -> io::Result<()> {
        let state = self.state.clone();
        let mut peers = {
            state.connected_peers.write()
                .map_err(|_| io::Error::other("Connected peers poisoined"))?
        };
        peers.insert(new_peer, now());
        Ok(())
    }

    pub async fn seen_nonce(self, nonce: Nonce) -> io::Result<()> {
        let seen_nonces = self.nonces.clone();
        let mut seen_nonces = seen_nonces.write()
            .map_err(|_| io::Error::other("Sent nonces poisoined"))?;
        seen_nonces.insert((nonce, now()));
        Ok(())
    }

    pub async fn add_sent_nonce(self, nonce: Nonce, peer_addr: SocketAddr, nonce_type: NonceType) -> io::Result<()> {
        let sent_nonces = {
            match nonce_type {
                NonceType::Sent => self.sent_nonces.clone(),
                NonceType::Received => self.recv_nonces.clone()
            }
        };
        let mut sent_nonces = sent_nonces.write()
            .map_err(|_| io::Error::other("Sent nonces poisoined"))?;
        sent_nonces.insert(peer_addr, (nonce, now()));
        Ok(())
    }

    pub async fn match_nonce(self, peer: Peer, nonce: Nonce, nonce_type: NonceType) -> io::Result<bool> {
        let nonces = {
            match nonce_type {
                NonceType::Sent => self.sent_nonces.clone(),
                NonceType::Received => self.recv_nonces.clone()
            }
        };
        let mut expected_nonce = Nonce::default();
        {
            let nonces = nonces.read()
                .map_err(|_| io::Error::other("Sent nonces poisoined"))?;
            if !nonces.contains_key(&peer.addr) {
                return Ok(false);       
            } else {      
                expected_nonce = match nonces.get(&peer.addr) {
                    Some((nonce, _)) => *nonce,
                    None => Nonce::default()
                };
            }
        };

        if expected_nonce == nonce && expected_nonce != Nonce::default() {
            let mut nonces = nonces.write()
                .map_err(|_| io::Error::other("Sent nonces poisoined"))?;
            nonces.remove(&peer.addr);
            return Ok(true);
        }

        Ok(false)
    }
}


