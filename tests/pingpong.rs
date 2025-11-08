use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task;
use crabchain::node::Node;
use crabchain::message::ping;

#[tokio::test]
async fn integration_ping_pong() -> std::io::Result<()> {
    let addr_a: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:8081".parse().unwrap();

    let node_a = Node::new(addr_a);
    let node_b = Node::new(addr_b);

    let listener_b = TcpListener::bind(addr_b).await?;

    let handle_b = task::spawn(async move {
        if let Err(e) = node_b.join_network(listener_b).await {
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    ping(node_a, addr_b).await?;
    println!("Node A sent Ping to {}", addr_b);

    tokio::time::sleep(Duration::from_millis(2000)).await;

    drop(handle_b);

    Ok(())
}

