use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task;
use crabchain::node::Node;
use crabchain::message::{Message, send_message, ping};
use crabchain::crypto::signing_key;
use crabchain::node::*;

#[tokio::test]
async fn integration_ping_pong() -> std::io::Result<()> {
    let addr_a: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    let sk_a = signing_key();
    let pk_a = sk_a.verifying_key();

    let sk_b = signing_key();
    let pk_b = sk_b.verifying_key();

    let node_a = Node { id: pk_a, addr: addr_a };
    let node_b = Node { id: pk_b, addr: addr_b };

    let listener_b = TcpListener::bind(addr_b).await?;

    let handle_b = task::spawn(async move {
        if let Err(e) = node_b.join_network(listener_b, sk_b).await {
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let state = Arc::new(State::default());

    ping(addr_b, state, sk_a).await?;
    println!("Node A sent Ping to {}", addr_b);

    tokio::time::sleep(Duration::from_millis(2000)).await;

    drop(handle_b);

    Ok(())
}

