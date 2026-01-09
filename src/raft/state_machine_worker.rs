use std::sync::Arc;
use std::thread;
use crossbeam_queue::SegQueue;
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
        self.state_machine.initialize();
        loop {
            self.state_machine.time_step();
            let task = self.work_queue.pop();

            match task {
                Some(task) => {
                    self.state_machine.accept(task)
                }
                None => {
                    let sleep_duration = core::time::Duration::from_micros(50);
                    thread::sleep(sleep_duration)
                }
            }
        }
    }
}