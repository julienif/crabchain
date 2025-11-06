use std::fmt::write;
use std::net::SocketAddr;
use std::time::Duration;
use crate::node::*;
use crate::block::{Block, BlockHeader};
use crate::blockchain::BlockRange;
use crate::Digest;
use ed25519_dalek::SigningKey;
use serde::{Serialize, Deserialize};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;      

#[cfg(debug_assertions)]
use serde_json;

#[cfg(not(debug_assertions))]
use bincode;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub sender_id: [u8; 32],
    pub payload: Vec<u8>,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Ping,
    Pong,
    Hello(Box<Node>),
    GossipTx(Box<Transaction>),
    GossipBlock(Box<BlockHeader>),
    NewBlock(Box<Block>),
    GetBlock(Box<Digest>),
    GetBlocks(Box<BlockRange>),
}

#[cfg(debug_assertions)]
pub async fn send_message(addr: SocketAddr, msg: Message) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    let data = serialize(msg)?;
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(data.as_bytes()).await?;

    Ok(stream)
}

#[cfg(debug_assertions)]
pub async fn send_message_socket(socket: &mut TcpStream, msg: Message) -> io::Result<()> {
    let data = serialize(msg)?;
    let len = (data.len() as u32).to_be_bytes();
    socket.write_all(&len).await?;
    socket.write_all(data.as_bytes()).await
}

#[cfg(not(debug_assertions))]
pub async fn send_message(addr: SocketAddr, msg: Message) -> io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let data = serialize(msg)?;
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&data).await?;

    Ok(stream)
}

#[cfg(not(debug_assertions))]
pub async fn send_message_socket(socket: &mut TcpStream, msg: Message) -> io::Result<()> {
    println!("sending message to: {socket:?}");
    let data = serialize(msg)?;
    let len = (data.len() as u32).to_be_bytes();
    socket.write_all(&len).await?;
    socket.write_all(&data).await
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

pub async fn expect_answer(socket: &mut TcpStream, state: SharedState, sk: SigningKey) -> io::Result<()> {
    match timeout(crate::TIMEOUT, handle_connection(socket, state, sk)).await {
        Ok(inner_result) => inner_result, // error propagated from handle_connection
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "Timeout expired")),
    }
}

pub async fn ping(addr: SocketAddr, state: SharedState, sk: SigningKey) -> io::Result<()> {
    let mut socket = send_message(addr, Message::Ping).await?;
    let state = state.clone();
    if let Err(e) = expect_answer(&mut socket, state.clone(), sk).await {
        if e.kind() != io::ErrorKind::TimedOut {
            return Err(e);
        }
        let mut peers = state.connected_peers.write()
            .map_err(|_| io::Error::other("Connected peers poisoined"))?;
        peers.retain(|p, _| p.addr != addr);
    }
    Ok(())
}

pub async fn pong(socket: &mut TcpStream) -> io::Result<()> {
    //tokio::time::sleep(Duration::from_secs(6)).await; // use this to handle timeout w/o crash
    send_message_socket(socket, Message::Pong).await
}

#[cfg(debug_assertions)]
pub fn serialize(msg: Message) -> io::Result<String> {
    {
        serde_json::to_string(&msg)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
    
#[cfg(not(debug_assertions))]
pub fn serialize(msg: Message) -> io::Result<Vec<u8>> {
    {
        bincode::serialize(&msg)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

pub fn deserialize(data: Vec<u8>) -> io::Result<Message> {
    #[cfg(debug_assertions)]
    {
        serde_json::from_slice(data.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
    
    #[cfg(not(debug_assertions))]
    {
        bincode::deserialize(data.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
