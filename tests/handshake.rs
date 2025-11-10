use std::net::SocketAddr;
use std::time::Duration;

use crabchain::{message::hello, node::*};
use tokio::net::TcpListener;
use tokio::io;

#[tokio::test]
async fn integration_handshake() -> io::Result<()> {
    let addr_a: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8002".parse().unwrap();

    let node_a = Node::new(addr_a);
    let node_b = Node::new(addr_b);

    let listener_b = TcpListener::bind(addr_b).await?;


    //let node_b_clone = node_b.clone();
    //let handle_b = tokio::spawn(async move {
    //    let node_b = node_b_clone.clone();
    //    if let Err(e) = node_b.join_network(listener_b).await {
    //    }
    //});

    //hello(node_a.clone(), node_b.peer).await?;
    let listener_a = TcpListener::bind(addr_a).await?;

    //let node_a_clone = node_a.clone();
    //let handle_a = tokio::spawn(async move {
    //    let node_a = node_a_clone.clone();
    //    if let Err(e) = node_a.join_network(listener_a).await {
    //    }
    //});
    let node_a_clone = node_a.clone();
    let _noda_a_task = tokio::spawn( async move {
        node_a_clone.clone().join_network(listener_a).await
    });

    let node_b_clone = node_b.clone();
    let _noda_b_task = tokio::spawn( async move {
        node_b_clone.join_network(listener_b).await
    });

    tokio::time::sleep(Duration::from_secs(10)).await;

    let connected_a = node_a.state.connected_peers.read().expect("cool"); 
    let len_a = connected_a.len();

    let connected_b = node_b.state.connected_peers.read().expect("cool"); 
    let len_b = connected_b.len();

    println!("verifiying connection");
    assert_eq!(len_a, 1);
    assert_eq!(len_a, len_b);

    Ok(())
}
