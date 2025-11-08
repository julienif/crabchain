use blake3::Hash;
use serde::{Deserialize, Serialize};
use crate::message::Transaction;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BlockHeader {
    pub idx: u32,
    pub timestamp: u64,
    pub hash: Hash,
    pub previous_hash: Hash
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub txs: Vec<Transaction>,
}
