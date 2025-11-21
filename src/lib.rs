#![warn(
    warnings,
    unused_imports,
    unused_variables,
    dead_code,
    unreachable_code,
    trivial_casts,
    trivial_numeric_casts,
    missing_debug_implementations,
    missing_copy_implementations,
    unsafe_code
)]

use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio::io;

pub const TIMEOUT: Duration = Duration::from_secs(5);
pub const KEEP_ALIVE: Duration = Duration::from_secs(15);
pub const CHALLENGED: Duration = Duration::from_secs(5);
pub const HELLO: Duration = Duration::from_secs(10);
pub const TS_VALID: u64 = 10; // timestamp ok if 10 secs

pub const MAX_PEERS: usize = 2;

const MAX_TXS_PER_BLOCK: usize = 16;

#[derive(Debug, Clone, Copy)]
pub enum NonceType {
    Received,
    Sent
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Nonce(pub [u8; 2]); // 16 bits nonce

impl Default for Nonce {
    fn default() -> Self {
        Nonce([0u8, 2])
    }
}

pub trait Encode {
    fn serialize(&self) -> io::Result<String> where Self: Serialize {
        serde_json::to_string(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn deserialize(data: Vec<u8>) -> io::Result<Self> where for<'a> Self: Deserialize<'a> {
        serde_json::from_slice(data.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

pub mod node;
pub mod message;
pub mod block;
pub mod blockchain;
pub mod pool;
pub mod crypto;
pub mod utils;
