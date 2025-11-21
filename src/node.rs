use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::time::Duration;
use blake3::Hash;
use tokio::net::{TcpListener, TcpStream};
use tokio::{io, time};
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use ed25519_dalek::{PUBLIC_KEY_LENGTH, SigningKey, VerifyingKey};

use crate::crypto::{signing_key, verify_connect_msg};
use crate::utils::{now, since};
use crate::{CHALLENGED, DISCONNECTED, HELLO, KEEP_ALIVE, MAX_PEERS, Nonce, NonceType, TS_VALID, message::*};
use crate::Encode;

//TODO node need to disconnect to inactive peer and randomly
// connect to new periodically

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub id: [u8; PUBLIC_KEY_LENGTH],
    pub addr: SocketAddr
}

impl Encode for Peer {}

impl Peer {
    fn new(id: VerifyingKey, addr: SocketAddr) -> Self {
        Peer {
            id: id.to_bytes(),
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
    nonces: Arc<RwLock<HashMap<Nonce, u64>>>,
    pub sent_nonces: Arc<RwLock<HashMap<SocketAddr, (Nonce, u64)>>>,
    pub recv_nonces: Arc<RwLock<HashMap<SocketAddr, (Nonce, u64)>>>,
    pub gossiped: Arc<RwLock<HashMap<Hash, u64>>>
}

impl Node {
    pub fn new(addr: SocketAddr) -> Self {
        let sk = signing_key();
        Node { 
            sk: sk.clone(),
            peer: Peer::new(sk.verifying_key(), addr),
            state: Arc::new(State::default()),
            nonces: Arc::new(RwLock::new(HashMap::new())),
            sent_nonces: Arc::new(RwLock::new(HashMap::new())),
            recv_nonces: Arc::new(RwLock::new(HashMap::new())),
            gossiped: Arc::new(RwLock::new(HashMap::new()))
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
                });
            }
            Ok::<_, io::Error>(())
        });

        let self_node_clone = self.clone();
        let _bootstrap_task = tokio::spawn(async move {
            time::sleep(Duration::from_millis(100)).await;
            let _bootstrap = self_node_clone.bootstrap().await;
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
                let _challenged = self_node.challenged().await;
            }
        });

        let self_node_clone = self.clone();
        let _hello_task = tokio::spawn(async move {
            loop {
                let self_node = self_node_clone.clone();
                let _hello = self_node.hello_known().await;
            }
        });

        let self_node_clone = self.clone();
        let _wash_task = tokio::spawn(async move {
            loop {
                let self_node = self_node_clone.clone();
                let _wash = self_node.wash().await;
            }
        });

        Ok(())
    }

    pub async fn handle_connection(self, socket: &mut TcpStream)
    -> io::Result<()> {
        let data = read_socket(socket).await?;
        time::sleep(Duration::from_secs(1)).await;
    
        match Encode::deserialize(data) {
            Ok(msg) => self.process(msg, socket).await,
            Err(e) => Err(e)
        }
    }
    
    pub async fn bootstrap(self) -> io::Result<()> {
        println!("bootstraping");
        let file = File::open("res/peers.txt")?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            println!("addr: {:?}", line);
            let addr: SocketAddr = line.parse().unwrap();
            if addr == self.peer.addr {
                println!("skipping");
                continue;
            } else {
                let _ = hello(self.clone(), addr).await;
            }
        }
        Ok(())
    }

    async fn keep_alive(self) -> io::Result<()> {
        tokio::time::sleep(KEEP_ALIVE).await;
        let state = self.state.clone();
        let peers: HashMap<Peer, u64> = {
            let peers = state.connected_peers.read()
                .expect("connected_peers poisoined (read)");
            peers.clone()
        };

        for peer in peers.iter() {
            let self_clone = self.clone();
            let addr = peer.0.addr;
            let _ping_task = tokio::spawn(async move {
                let _ = ping(self_clone.clone(), addr).await;
            });

            println!("{:?}", peers.keys());
            if since(*peer.1) > DISCONNECTED.as_secs() {
                let mut peers_updated = state.connected_peers.write()
                    .expect("connected_peers poisoined (write)");
                peers_updated.remove(peer.0);
                println!("{:?}", peers_updated.keys());
            }
        }
        Ok(())
    }

    async fn challenged(self) -> io::Result<()> {
        tokio::time::sleep(CHALLENGED).await;
        let nonces_and_targets: Vec<(Nonce, SocketAddr)> = {
            let mut recv_nonces = self.recv_nonces.write()
                .expect("recv_nonces poisoined (write)");
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
                    .expect("connected_peers poisoined (read)");
                connected_peers.len()
            };
            if connected_peers_len < crate::MAX_PEERS {
                let _ = accept(node_self_clone, *nonce, *target).await;
            }
        }
        Ok(())
    }

    async fn hello_known(self) -> io::Result<()> {
        tokio::time::sleep(HELLO).await;
        let node_self_clone = self.clone();
        let connected_peers_len = {
            let node_self = node_self_clone.clone();
            let state = node_self.state.clone();
            let connected_peers = state.connected_peers.read()
                .expect("connected_peers poisoined (read)");
            connected_peers.len()
        };
        if connected_peers_len >= MAX_PEERS { return Ok(()); }
        let state = self.state.clone();
        let peers: Vec<Peer> = {
            let peers = state.known_peers.read()
                .expect("known_peers poisoined (read)");
            peers.iter().cloned().collect()
        };

        for peer in peers.iter() {
            let node_self = node_self_clone.clone();
            let state = node_self.state.clone();
            let connected_peers = state.connected_peers.read()
                .expect("connected_peers poisoined (read)");
            if connected_peers.contains_key(peer) {
                continue;
            }
            let self_clone = self.clone();
            let addr = peer.addr;
            let _ping_task = tokio::spawn(async move {
                let _ = hello(self_clone.clone(), addr).await;
            });
        }
        Ok(())
    }

    async fn wash(self) -> io::Result<()> {
        tokio::time::sleep(10*KEEP_ALIVE).await;
        {
            let nonces = self.nonces.write()
                .expect("nonces poisoined (write)");
            nonces.clone().retain(|_, ts| since(*ts) < crate::TS_VALID);
        }

        {
            let nonces = self.sent_nonces.write()
                .expect("sent_nonces poisoined (write)");
            nonces.clone().retain(|_, (_, ts)| since(*ts) < crate::TS_VALID);
        }

        {
            let nonces = self.recv_nonces.write()
                .expect("recv_nonces poisoined (write)");
            nonces.clone().retain(|_, (_, ts)| since(*ts) < crate::TS_VALID);
        }

        {
            let gossiped = self.gossiped.write()
                .expect("gossiped poisoined (write)");
            gossiped.clone().retain(|_, ts| since(*ts) < crate::TS_VALID);
        }
        Ok(())
    }

    async fn process(self, msg: Message, socket: &mut TcpStream) -> io::Result<()> {
        // self is needed when we expect a response
        match msg {
            Message::Ping => {
                println!("ping!");
                pong(self, socket).await
            },

            Message::Pong(peer) => {
                println!("pong!");
                let mut peers = self.state.connected_peers.write()
                    .expect("connected_peers poisoned (write)");
                peers.entry(*peer).and_modify(|ts| { *ts = now(); });
                Ok(())
            },

            Message::Hello(peer) => {
                self.clone().add_known_peer(*peer.clone()).await?;
                println!("hello! {:?}", peer.addr);
                let _ = challenge(self.clone(), socket, *peer).await;
                gossip_peer(self, *peer.clone()).await
            },

            Message::Challenge(nonce_peer_tuple) => {
                let nonce = nonce_peer_tuple.0;
                let peer = nonce_peer_tuple.1;
                self.clone().add_known_peer(peer).await?;
                if nonce == Nonce::default() { return Ok(()); }
                println!("challenge: {:?}", peer.addr);

                match self.clone().is_nonce_seen(nonce).await {
                    Ok(false) => self.add_sent_nonce(nonce, peer.addr, NonceType::Received).await,
                    _ => Ok(())
                }
            },

            Message::Accept(connect_message) => {
                println!("accept");
                let connect_message = *connect_message;
                let peer = connect_message.peer;
                let ts = connect_message.timestamp;
                let nonce = connect_message.nonce;

                match self.clone().is_nonce_seen(nonce).await {
                    Ok(false) => {
                        if verify_connect_msg(connect_message)
                            && since(ts) < TS_VALID 
                                && self.clone().match_nonce(peer, nonce, NonceType::Sent).await? {
                            self.clone().add_connected_peer(peer).await?;
                            connect(self, peer, socket, nonce).await
                        } else {
                            Ok(()) // corrupted msg ignored
                        }
                    },
                    _ => Ok(())
                }
            },

            Message::Connect(connect_message) => {
                println!("connect");
                let connect_message = *connect_message;
                let peer = connect_message.peer;
                let ts = connect_message.timestamp;
                let nonce = connect_message.nonce;

                match self.clone().is_nonce_seen(nonce).await {
                    Ok(false) => {
                        if verify_connect_msg(connect_message)
                            && since(ts) < TS_VALID 
                                && self.clone().match_nonce(peer, nonce, NonceType::Received).await? {
                            self.add_connected_peer(peer).await
                        } else {
                            Ok(()) // corrupted msg ignored
                        }
                    }
                    _ => Ok(())
                }
            },

            Message::Gossip(boxed_gossip) => {
                println!("gossip");
                let known_peers: Vec<Peer> = {
                    let known_peers = self.state.known_peers.read()
                        .expect("");
                    known_peers.iter()
                        .cloned()
                        .collect()
                };
                println!("{:?} {:?}", self.peer.addr, known_peers);
                match *boxed_gossip.clone() {
                    Gossip::PeerGossip(peer_gossip) => {
                        self.clone().add_gossip(peer_gossip.hash).await?;
                        let peer: Peer = Encode::deserialize(peer_gossip.encoded_peer)?;
                        if peer.addr != self.peer.addr {
                            self.clone().add_known_peer(peer).await?;
                        }
                    }
                };
                gossip(self, *boxed_gossip).await
            },
            Message::NewBlock(_block) => Ok(()),
            Message::GetBlock(_block_digest) => Ok(()),
            Message::GetBlocks(_range) => Ok(()),
        }
    }

    async fn add_connected_peer(self, new_peer: Peer) -> io::Result<()> {
        let state = self.state.clone();
        let mut peers = {
            state.connected_peers.write()
                .expect("connected_peers poisoined (write)")
        };
        peers.insert(new_peer, now());
        Ok(())
    }

    async fn add_known_peer(self, new_peer: Peer) -> io::Result<()> {
        let state = self.state.clone();
        let mut peers = {
            state.known_peers.write()
                .expect("known_peers poisoined (write)")
        };
        if !peers.contains(&new_peer) {
            println!("peer known {:?}", new_peer.addr);
            peers.insert(new_peer);
        }
        Ok(())
    }

    pub async fn seen_nonce(self, nonce: Nonce) -> io::Result<()> {
        let nonces = self.nonces.clone();
        let mut nonces = nonces.write()
            .expect("nonces poisoined (write)");
        nonces.insert(nonce, now());
        Ok(())
    }

    pub async fn is_nonce_seen(self, nonce: Nonce) -> io::Result<bool> {
        let nonces = self.nonces.read()
            .expect("nonces poisoined (read)");
        Ok(nonces.contains_key(&nonce))
    }

    pub async fn add_sent_nonce(self, nonce: Nonce, peer_addr: SocketAddr, nonce_type: NonceType) -> io::Result<()> {
        let sent_nonces = {
            match nonce_type {
                NonceType::Sent => self.sent_nonces.clone(),
                NonceType::Received => self.recv_nonces.clone()
            }
        };
        let mut sent_nonces = sent_nonces.write()
            .expect("sent_nonces poisoined (write)");
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
        #[allow(unused_assignments)]
        let mut expected_nonce = Nonce::default();
        {
            let nonces = nonces.read()
                .expect("sent_or_recv_nonces poisoined (read)");
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
                .expect("sent_or_recv_nonces poisoined (write)");
            nonces.remove(&peer.addr);
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn add_gossip(self, hash: Hash) -> io::Result<()> {
        let mut gossiped = self.gossiped.write()
            .expect("gossiped poisoined (write)");
        gossiped.insert(hash, now());
        Ok(())
    }

    pub async fn is_gossip_seen(self, hash: Hash) -> io::Result<bool> {
        let gossiped = self.gossiped.read()
            .expect("gossiped poisoined (read)");
        Ok(gossiped.contains_key(&hash))
    }
}


