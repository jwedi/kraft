use crate::raft::raft_sm::RaftStateMachineExecutor;
use crate::raft::raft_sm::{LocalRaftMessage, SharedState, StateMachineExecutorImpl};
use crossbeam_queue::SegQueue;
use crossbeam_utils::Backoff;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct StateMachineWorker {
    work_queue: Arc<SegQueue<LocalRaftMessage>>,
    state_machine: Box<StateMachineExecutorImpl>,
}

impl StateMachineWorker {
    pub fn new(work_queue: Arc<SegQueue<LocalRaftMessage>>, shared_state: SharedState) -> Self {
        Self {
            work_queue,
            state_machine: Box::new(StateMachineExecutorImpl::new(shared_state)),
        }
    }

    pub fn run(&mut self) {
        log::info!("Running worker");
        let backoff = Backoff::new();
        loop {
            let mut work_done: u64 = 0;

            work_done += self.state_machine.time_step();

            // Drain the work queue
            while let Some(task) = self.work_queue.pop() {
                self.state_machine.accept(task);
                work_done += 1;
            }

            if work_done > 0 {
                backoff.reset();
            } else {
                if backoff.is_completed() {
                    thread::park_timeout(Duration::from_micros(500));
                    backoff.reset();
                } else {
                    backoff.snooze();
                }
            }
        }
    }
}
