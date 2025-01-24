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

    pub fn claim_spot(&self) -> Result<u64, ServiceError> {
        let idx = self.write_index.load(Ordering::Relaxed);
        if idx == self.read_index {
            Err(ThrottlingError("Write index same as write index".to_string()))
        } else {
            let res = self.write_index.compare_exchange(idx, idx+1, Ordering::Acquire, Ordering::Relaxed);
            match res {
                Ok(v) => {
                    Ok(idx)
                }
                Err(e) => {
                    Err(RaceConditionError)
                }
            }
        }
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