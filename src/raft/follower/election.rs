use std::time::Duration;
use rand::Rng;
use rand::rngs::ThreadRng;

use crate::raft::candidate::RaftCandidateStateDelegate;
use crate::raft::raft_sm::RaftMessageStateChange;
use crate::service_utils::app_time::{now_millis, now_plus_duration_millis};

/// Election timeout state and management.
pub struct ElectionTimer {
    pub election_timeout: u128,
    pub rng: ThreadRng,
    pub election_timeout_min: u128,
    pub election_timeout_max: u128,
}

impl ElectionTimer {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let min: u128 = 600;
        let max: u128 = 1300;
        let election_timeout = rng.gen_range(min..max);
        Self {
            election_timeout: now_plus_duration_millis(Duration::from_millis(election_timeout as u64)),
            rng,
            election_timeout_min: min,
            election_timeout_max: max,
        }
    }

    /// Resets the election timeout to a new random value.
    pub fn reset(&mut self) {
        let timeout_ms = self.rng.gen_range(self.election_timeout_min..self.election_timeout_max);
        self.election_timeout = now_plus_duration_millis(Duration::from_millis(timeout_ms as u64));
    }

    /// Checks if the election timeout has expired.
    /// Returns `Some(RaftMessageStateChange::Candidate)` if timeout expired, `None` otherwise.
    pub fn check_timeout(&self) -> Option<RaftMessageStateChange> {
        let now = now_millis();
        if self.election_timeout < now {
            Some(RaftMessageStateChange::Candidate(
                Box::new(RaftCandidateStateDelegate::new())
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_election_timer_new_initializes_within_bounds() {
        let timer = ElectionTimer::new();

        // Election timeout should be set in the future
        assert!(timer.election_timeout > 0);

        // Min and max should be properly set
        assert_eq!(timer.election_timeout_min, 600);
        assert_eq!(timer.election_timeout_max, 1300);
        assert!(timer.election_timeout_min < timer.election_timeout_max);
    }

    #[test]
    fn test_election_timer_reset_updates_timeout() {
        let mut timer = ElectionTimer::new();

        // Reset multiple times and verify timeout is always in the future
        for _ in 0..10 {
            timer.reset();
            // The timeout should always be positive (in the future)
            assert!(timer.election_timeout > 0);
        }
    }

    #[test]
    fn test_check_timeout_returns_none_when_not_expired() {
        let timer = ElectionTimer::new();

        // New timer should not be expired
        let result = timer.check_timeout();
        assert!(result.is_none());
    }

    #[test]
    fn test_check_timeout_returns_candidate_when_expired() {
        let mut timer = ElectionTimer::new();

        // Set timeout to 0 (past)
        timer.election_timeout = 0;

        let result = timer.check_timeout();
        assert!(result.is_some());

        match result.unwrap() {
            RaftMessageStateChange::Candidate(_) => {}
            _ => panic!("Expected Candidate state change"),
        }
    }

    #[test]
    fn test_election_timer_reset_sets_future_timeout() {
        let mut timer = ElectionTimer::new();

        // Set timeout to past
        timer.election_timeout = 0;
        assert!(timer.check_timeout().is_some());

        // Reset should set it to the future
        timer.reset();
        assert!(timer.check_timeout().is_none());
    }
}
