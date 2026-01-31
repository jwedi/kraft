use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use crossbeam_queue::SegQueue;
use tokio::task::yield_now;
use tokio::time::Instant;
use crate::raft::raft_sm::{LocalRaftMessage, SharedState, StateMachineExecutorImpl};
use crate::raft::raft_sm::RaftStateMachineExecutor;

pub struct StateMachineWorker {
    work_queue: Arc<SegQueue<LocalRaftMessage>>,
    state_machine: Box<StateMachineExecutorImpl>,
}

impl StateMachineWorker {
    pub fn new(work_queue: Arc<SegQueue<LocalRaftMessage>>, shared_state: SharedState) -> Self {
        Self {
            work_queue,
            state_machine: Box::new(StateMachineExecutorImpl::new(shared_state))
        }
    }

    pub fn run(&mut self) {
        log::info!("Running worker");
        let desired_cadence_micros = 10;
        loop {
            let start_time = Instant::now();
            self.state_machine.time_step();
            let task = self.work_queue.pop();

            match task {
                Some(task) => {
                    self.state_machine.accept(task)
                }
                None => {
                    thread::yield_now()
                }
            }
            let elapsed_micros = start_time.elapsed().as_micros();
            if elapsed_micros < desired_cadence_micros {
                sleep(Duration::from_micros((desired_cadence_micros-elapsed_micros) as u64));
            } else {
                thread::yield_now()
            }
        }
    }
}