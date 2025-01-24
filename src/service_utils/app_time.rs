use std::ops::Add;
use std::time::{Duration, SystemTime};

pub fn now_millis() -> u128 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis()
}

pub fn now_plus_duration_millis(dur: Duration) -> u128 {
    let start = SystemTime::now().add(dur);
    let since_the_epoch = start
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis()
}