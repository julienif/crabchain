# 🦀 **crabchain**
## *Cool Asynchronous Rust BlockChain*  
Pedagogic project destined to learn and apply basic protocols involved in blockchain such as p2p network, handshake, gossip, peer rotation, etc...  
Still a *WIP*.

## 🚀 Install and Run ##
### From source ###
    git clone https://github.com/julienif/crabchain.git
    cargo run 127.0.0.1:8001 # this is the first node
    # From other terminals
    cargo run 127.0.0.1:<port> # peers

## ✨ Features
- Built with tokio for fully asynchronous networking.
- Nodes perform a secure handshake using:
    - a challenge with a **nonce** to prevent replay
    - a **signature** to prevent MitM
- Used Nonces are stored and cleared when the timestamp in the connection message becomes invalid.
- New nodes announces themselves to the bootstrap/origin node (specified in `res/peers.txt`) when joining the network.
- Nodes maintains connectivity through a simple ping-pong protocol.
- Peers are propagated across the network using a gossip mechanism.
- To avoid isolation of nodes, a rotation is implemetend allowing nodes to randomly change connections.

## 🔜 Further improvements ***TODO***
- Introduce a transaction logic, blocks and consensus protocol (PoW, PoS, etc...).

## ⚠️ Notes
This project is purely educational and does not intend to be used anywhere. It might contain several bugs and does not support NAT nor firewalls.  
You can only run this in a local environment :)
