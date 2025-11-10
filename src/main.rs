use tokio::net::TcpListener;
use std::net::SocketAddr;
use std::{env, io};
use crabchain::node::*;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: crabchain <address> (ip:port)");
        std::process::exit(1);
    }
    
    let address: SocketAddr = args[1].parse().unwrap();

    let node = Node::new(address);

    let listener = TcpListener::bind(address).await?;
    
    let _net_task = tokio::spawn( async move {
        node.join_network(listener).await
    }).await?;

    loop {}
    
    #[allow(unreachable_code, dead_code)]
    Ok(())
}
