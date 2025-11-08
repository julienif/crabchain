use std::time::{self, UNIX_EPOCH};

pub fn now() -> u64 {
    time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn since(timestamp: u64) -> u64 {
    now() - timestamp
}
