use std::sync;
use std::sync::atomic::Ordering;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use crate::service_utils::errors::{ServiceError};
use crate::service_utils::errors::ServiceError::{RaceConditionError, ThrottlingError};
use crate::runtime_core::types::{RuntimeTask};

pub trait TaskBuffer {
    fn claim_spot(&mut self) -> u64;
    fn put_task(&mut self, task: RuntimeTask, spot: u64);
    fn get_task(&mut self) -> Option<RuntimeTask>;
}

pub struct TaskBufferImpl<T> {
    read_index: u64,
    write_index: sync::atomic::AtomicU64,
    tasks: Vec<T>,
}

impl <T>TaskBufferImpl<T> {
    pub fn new() -> Self {
        Self {
            read_index: 0,
            write_index: sync::atomic::AtomicU64::new(0),
            tasks: Vec::new(),
        }
    }

    pub fn peek_size(&self) -> u64 {
        self.write_index.load(Ordering::Relaxed) - self.read_index
    }

    /// Atomically claims a spot in the buffer for writing.
    ///
    /// Uses a retry loop to handle contention properly instead of a check-then-act
    /// pattern that could race between load and compare_exchange.
    pub fn claim_spot(&self) -> Result<u64, ServiceError> {
        // Use a retry loop to handle concurrent claim attempts
        const MAX_RETRIES: u32 = 100;

        for _ in 0..MAX_RETRIES {
            let idx = self.write_index.load(Ordering::Acquire);

            // Check for index overflow (would wrap around causing corruption)
            let next_idx = match idx.checked_add(1) {
                Some(n) => n,
                None => {
                    return Err(ThrottlingError("Write index overflow".to_string()));
                }
            };

            // Check if buffer is full (write index caught up to read index after wrap)
            // This check uses the atomic read_index if it were atomic, but since read_index
            // is not atomic in this design, this check is inherently racy.
            // For now, we just prevent overflow.

            // Atomically try to claim this spot
            match self.write_index.compare_exchange_weak(
                idx,
                next_idx,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(idx),
                Err(_) => {
                    // Another thread claimed this spot, retry
                    std::hint::spin_loop();
                    continue;
                }
            }
        }

        // Too much contention
        Err(RaceConditionError)
    }

    pub fn put_task(&mut self, task: T, spot: u64) {
        let spot_usize = spot as usize;
        self.tasks.insert(spot_usize % self.tasks.len(), task);
    }

    pub fn get_task(&mut self) -> Option<&T> {
        let usize_idx = self.read_index as usize;
        let task = &self.tasks[usize_idx % self.tasks.len()];
        self.read_index += 1;
        Some(task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // =========================================================================
    // Regression test: TaskBuffer claim_spot returns unique indices
    // Fix: Replaced check-then-act pattern with CAS retry loop
    // =========================================================================
    #[test]
    fn test_claim_spot_sequential() {
        let buffer: TaskBufferImpl<i32> = TaskBufferImpl::new();

        let spot1 = buffer.claim_spot().expect("Should claim spot 0");
        let spot2 = buffer.claim_spot().expect("Should claim spot 1");
        let spot3 = buffer.claim_spot().expect("Should claim spot 2");

        assert_eq!(spot1, 0);
        assert_eq!(spot2, 1);
        assert_eq!(spot3, 2);
    }

    // =========================================================================
    // Regression test: Concurrent claim_spot operations return unique indices
    // Fix: CAS retry loop prevents race conditions
    // =========================================================================
    #[test]
    fn test_claim_spot_concurrent() {
        let buffer = Arc::new(TaskBufferImpl::<i32>::new());
        let num_threads = 8;
        let claims_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let buffer_clone = Arc::clone(&buffer);
                thread::spawn(move || {
                    let mut spots = Vec::with_capacity(claims_per_thread);
                    for _ in 0..claims_per_thread {
                        match buffer_clone.claim_spot() {
                            Ok(spot) => spots.push(spot),
                            Err(ServiceError::RaceConditionError) => {
                                // Retryable under high contention, but should be rare
                            }
                            Err(e) => panic!("Unexpected error: {:?}", e),
                        }
                    }
                    spots
                })
            })
            .collect();

        let mut all_spots: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All claimed spots should be unique
        let original_len = all_spots.len();
        all_spots.sort();
        all_spots.dedup();
        assert_eq!(
            original_len,
            all_spots.len(),
            "Duplicate spots were claimed! Race condition detected."
        );

        // All spots should be sequential starting from 0
        for (i, spot) in all_spots.iter().enumerate() {
            assert_eq!(
                *spot, i as u64,
                "Spots should be sequential without gaps"
            );
        }
    }

    // =========================================================================
    // Regression test: Overflow protection prevents u64 wrap-around
    // Fix: Added checked_add to prevent silent overflow
    // =========================================================================
    #[test]
    fn test_claim_spot_overflow_protection() {
        let buffer: TaskBufferImpl<i32> = TaskBufferImpl::new();

        // Set write_index near u64::MAX to test overflow protection
        buffer
            .write_index
            .store(u64::MAX, Ordering::Release);

        let result = buffer.claim_spot();
        assert!(result.is_err());
        match result {
            Err(ThrottlingError(msg)) => {
                assert!(msg.contains("overflow"), "Error should mention overflow");
            }
            _ => panic!("Expected ThrottlingError for overflow"),
        }
    }

    // =========================================================================
    // Regression test: peek_size returns correct buffer size
    // =========================================================================
    #[test]
    fn test_peek_size() {
        let buffer: TaskBufferImpl<i32> = TaskBufferImpl::new();

        assert_eq!(buffer.peek_size(), 0);

        buffer.claim_spot().unwrap();
        assert_eq!(buffer.peek_size(), 1);

        buffer.claim_spot().unwrap();
        buffer.claim_spot().unwrap();
        assert_eq!(buffer.peek_size(), 3);
    }
}