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

const DIGEST_LEN: usize = 32;
pub type Digest = [u8; DIGEST_LEN];

pub const TIMEOUT: Duration = Duration::from_secs(5);
pub const KEEP_ALIVE: Duration = Duration::from_secs(5);

const MAX_TXS_PER_BLOCK: usize = 16;

pub mod node;
pub mod message;
pub mod block;
pub mod blockchain;
pub mod pool;
pub mod crypto;
