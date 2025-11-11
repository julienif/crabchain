use std::net::SocketAddr;
use crate::crypto::{hash_sign_connect_msg, nonce};
use crate::{Encode, Nonce, NonceType, node::*};
use crate::utils::now;
use crate::block::{Block, BlockHeader};
use crate::blockchain::BlockRange;
use blake3::Hash;
use ed25519_dalek::{PUBLIC_KEY_LENGTH, Signature};
use serde::{Serialize, Deserialize};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;      
use tokio::time::timeout;

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub sender_id: [u8; PUBLIC_KEY_LENGTH],
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub hash: Hash
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ConnectMessage {
    pub peer: Peer,
    pub timestamp: u64,
    pub nonce: Nonce,
    pub hash: Hash,
    pub sig: Signature
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerGossip {
    pub sender_id: [u8; PUBLIC_KEY_LENGTH],
    pub encoded_peer: Vec<u8>,
    pub hash: Hash
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Gossip {
    PeerGossip(Box<PeerGossip>)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Ping,
    Pong,
    Hello(Box<Peer>),
    Challenge(Box<(Nonce, Peer)>),
    Accept(Box<ConnectMessage>),
    Connect(Box<ConnectMessage>),
    Gossip(Box<Gossip>),
    NewBlock(Box<Block>),
    GetBlock(Box<Hash>),
    GetBlocks(Box<BlockRange>),
}

impl Encode for Message {}

pub async fn send_message(addr: SocketAddr, msg: Message) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    let data = Encode::serialize(&msg)?;
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(data.as_bytes()).await?;

    Ok(stream)
}

pub async fn send_message_socket(socket: &mut TcpStream, msg: Message) -> io::Result<()> {
    let data = Encode::serialize(&msg)?;
    let len = (data.len() as u32).to_be_bytes();
    socket.write_all(&len).await?;
    socket.write_all(data.as_bytes()).await
}

pub async fn read_socket(socket: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];

    socket.readable().await?;
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);

    let mut data = vec![0u8; len as usize];
    socket.read_exact(&mut data).await?;

    Ok(data)
}

pub async fn expect_answer(node: Node, socket: &mut TcpStream) -> io::Result<()> {
    match timeout(crate::TIMEOUT, node.handle_connection(socket)).await {
        Ok(inner_result) => inner_result, // error propagated from handle_connection
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout expired")),
    }
}

pub async fn ping(node: Node, addr: SocketAddr) 
-> io::Result<()> {
    let mut socket = send_message(addr, Message::Ping).await?;
    let state = node.state.clone();
    if let Err(e) = expect_answer(node, &mut socket).await {
        if e.kind() != io::ErrorKind::TimedOut {
            return Err(e);
        }
        let mut peers = state.connected_peers.write()
            .expect("connected_peers poisoined (write)");
        peers.retain(|p, _| p.addr != addr);
    }
    Ok(())
}

pub async fn pong(socket: &mut TcpStream) 
-> io::Result<()> {
    //tokio::time::sleep(time::Duration::from_secs(6)).await; // use this to handle timeout w/o crash
    send_message_socket(socket, Message::Pong).await
}

pub async fn hello(node: Node, addr: SocketAddr)
-> io::Result<()> {
    let mut socket = send_message(addr, Message::Hello(Box::new(node.peer))).await?;
    if let Err(e) = expect_answer(node, &mut socket).await
        && e.kind() != io::ErrorKind::TimedOut {
            return Err(e);
    }
    Ok(())
}

pub async fn challenge(node: Node, socket: &mut TcpStream, new_peer: Peer)
-> io::Result<()> {
    let state = node.state.clone();
    let connected_peers_size = {
        let connected_peers = state.connected_peers.read()
            .expect("connected_peers poisoined (read)");
        connected_peers.len()
    };
    
    let is_known_peer = {
        let known_peers = state.known_peers.read()
            .expect("known_peers poisoined (read)");
        known_peers.contains(&new_peer)

    };
    if !is_known_peer {
        let mut known_peers = state.known_peers.write()
            .expect("known_peers poisoined (write)");
        known_peers.insert(new_peer);
    } else if connected_peers_size >= crate::MAX_PEERS {
        return Ok(());
    }
    
    if connected_peers_size < crate::MAX_PEERS {
        let nonce = nonce();
        let node_clone = node.clone();
        node_clone.add_sent_nonce(nonce, new_peer.addr, NonceType::Sent).await?;
        send_message_socket(socket, Message::Challenge(Box::new((nonce, node.peer)))).await
    } else if !is_known_peer {
        let nonce = Nonce::default();
        send_message_socket(socket, Message::Challenge(Box::new((nonce, node.peer)))).await
    } else {
        Ok(())
    }
}

pub async fn accept(node: Node, nonce: Nonce, addr: SocketAddr)
-> io::Result<()> {
    let mut sk = node.sk.clone();
    let msg = hash_sign_connect_msg(node.peer, nonce, &mut sk);
    // pin for dodging recursion that will never happen
    Box::pin(async move {
        let mut socket = send_message(addr, Message::Accept(Box::new(msg))).await?;
        if let Err(e) = expect_answer(node, &mut socket).await
            && e.kind() != io::ErrorKind::TimedOut {
                return Err(e);
        }
        Ok(())
    }).await
}

pub async fn connect(node: Node, new_peer: Peer, socket: &mut TcpStream, nonce: Nonce)
-> io::Result<()> {
    let state = node.state.clone();
    let connected_peers_size = {
        let connected_peers = state.connected_peers.read()
            .expect("connected_peers poisoined (read)");
        connected_peers.len()
    };

    if connected_peers_size < crate::MAX_PEERS {
        {
            let mut connected_peers = state.connected_peers.write()
                .expect("connected_peers poisoined (write)");
            connected_peers.insert(new_peer, now());
        }
        let mut sk = node.sk.clone();
        let msg = hash_sign_connect_msg(node.peer, nonce, &mut sk);
        return send_message_socket(socket, Message::Connect(Box::new(msg))).await;
    }
    
    Ok(())
}

pub async fn gossip(node: Node, gossip: Gossip) -> io::Result<()> {
    let hash = match gossip.clone() {
        Gossip::PeerGossip(peer_gossip) => peer_gossip.hash,
    };
    match node.clone().is_gossip_seen(hash).await {
        Ok(true) => { return Ok(()) },
        _ => {}
    };

    let state = node.state.clone();
    let peers: Vec<Peer> = {
        let peers = state.connected_peers.read()
            .expect("connected_peers poisoined (read)");
        peers.iter()
            .map(|(p, _)| p)
            .cloned()
            .collect()
    };
    for p in peers {
        let gossip = gossip.clone();
        let _gossip_task = tokio::spawn(async move {
            let _ = send_message(p.addr, Message::Gossip(Box::new(gossip))).await;
        });
    }
    Ok(())
}

pub async fn gossip_peer(node: Node, peer: Peer) -> io::Result<()> {
    let encoded_peer = Encode::serialize(&peer).expect("Invalid Peer").as_bytes().to_vec();
    let hashed_payload = blake3::hash(&encoded_peer);
    let peer_gossip = PeerGossip {
        sender_id: node.peer.id,
        encoded_peer: encoded_peer,
        hash: hashed_payload
    };
    gossip(node, Gossip::PeerGossip(Box::new(peer_gossip))).await?;
    Ok(())
}
