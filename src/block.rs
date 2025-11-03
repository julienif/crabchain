use serde::{Deserialize, Serialize};
use crate::Digest;
use crate::message::Transaction;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BlockHeader {
    pub idx: u32,
    pub timestamp: u64,
    pub hash: Digest,
    pub previous_hash: Digest
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Block {
    pub header: BlockHeader,
    pub txs: Vec<Transaction>,
}
