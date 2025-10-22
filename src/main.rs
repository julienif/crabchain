use tokio::net::TcpListener;
use std::{env, io};
use crabchain::node::*;
use crabchain::crypto::signing_key;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: crabchain <address> (ip::port)");
        std::process::exit(1);
    }
    
    let addr = args[1].clone();
    let sk = signing_key();
    let pk = sk.verifying_key();

    let node = Node {
        id: pk,
        addr: addr.clone()
    };

    let listener = TcpListener::bind(addr).await?;
    
    node.join_network(listener).await?;

    Ok(())
}
