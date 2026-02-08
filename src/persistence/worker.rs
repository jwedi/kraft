use crossbeam_channel::{Receiver, RecvTimeoutError};
use crossbeam_queue::SegQueue;
use csv::{ReaderBuilder, Writer, WriterBuilder};
use log::info;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufWriter, Read, Write};
use std::sync::Arc;
use std::time::Duration;
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub enum PersistenceTaskType {
    AppendVote {
        id: u64,
        term: u64,
        candidate_id: u32,
        parent_span: Span,
    },
    AppendLog {
        id: u64,
        data: Arc<Vec<u8>>,
        parent_span: Span,
        request_id: u64,
    },
    TruncateLog {
        target_length: usize,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct VoteRow {
    pub term: u64,
    pub candidate_id: u32,
}

pub enum PersistenceResponseType {
    VotePersisted { id: u64, term: u64, candidate_id: u32 },
    LogPersisted { id: u64 },
}

pub struct PersistenceConfig {
    pub out_dir: String,
}

pub struct PersistenceWorker {
    work_queue: Receiver<PersistenceTaskType>,
    response_queue: Arc<SegQueue<PersistenceResponseType>>,
    persistence_config: PersistenceConfig,
    entry_end_offsets: Vec<u64>,
}

impl PersistenceWorker {
    pub fn new(
        work_queue: Receiver<PersistenceTaskType>,
        response_queue: Arc<SegQueue<PersistenceResponseType>>,
        persistence_config: PersistenceConfig,
        entry_end_offsets: Vec<u64>,
    ) -> Self {
        Self {
            work_queue,
            response_queue,
            persistence_config,
            entry_end_offsets,
        }
    }

    pub fn set_entry_end_offsets(&mut self, offsets: Vec<u64>) {
        self.entry_end_offsets = offsets;
    }

    pub fn run(&mut self) {
        info!("Running persistence worker");

        // Batch size for accumulating AppendLog tasks before flushing
        const BATCH_SIZE: usize = 32;

        let log_write_file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("{}log.capnp", self.persistence_config.out_dir))
            .unwrap();
        let mut log_write_buffer = BufWriter::new(log_write_file_handle);

        let vote_write_file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("{}votes.csv", self.persistence_config.out_dir))
            .unwrap();
        let mut vote_write_buffer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(vote_write_file_handle);

        // Track pending log responses for batched flushing
        let mut pending_log_responses: Vec<PersistenceResponseType> = Vec::with_capacity(BATCH_SIZE);

        loop {
            // Block until task arrives (or timeout for batch flush)
            match self.work_queue.recv_timeout(Duration::from_millis(10)) {
                Ok(task) => {
                    self.process_task(
                        task,
                        &mut log_write_buffer,
                        &mut vote_write_buffer,
                        &mut pending_log_responses,
                        BATCH_SIZE,
                    );

                    // After processing, drain any additional queued tasks (non-blocking) for batching
                    while let Ok(task) = self.work_queue.try_recv() {
                        self.process_task(
                            task,
                            &mut log_write_buffer,
                            &mut vote_write_buffer,
                            &mut pending_log_responses,
                            BATCH_SIZE,
                        );
                    }

                    // Flush after draining the queue
                    if !pending_log_responses.is_empty() {
                        let flushed = self
                            .flush_pending_logs(&mut log_write_buffer, &mut pending_log_responses)
                            .unwrap();
                        if flushed > 1 {
                            tracing::debug!("Batched flush of {} log entries", flushed);
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Timeout - flush any pending writes to ensure durability
                    self.flush_pending_logs(&mut log_write_buffer, &mut pending_log_responses)
                        .unwrap();
                }
                Err(RecvTimeoutError::Disconnected) => {
                    // Channel closed, flush remaining and exit
                    self.flush_pending_logs(&mut log_write_buffer, &mut pending_log_responses)
                        .unwrap();
                    info!("Persistence worker channel disconnected, exiting");
                    break;
                }
            }
        }
    }

    fn process_task(
        &mut self,
        task: PersistenceTaskType,
        log_write_buffer: &mut BufWriter<File>,
        vote_write_buffer: &mut Writer<File>,
        pending_log_responses: &mut Vec<PersistenceResponseType>,
        batch_size: usize,
    ) {
        match task {
            PersistenceTaskType::AppendVote {
                id,
                term,
                candidate_id,
                parent_span,
            } => {
                // Flush any pending log writes before handling vote
                self.flush_pending_logs(log_write_buffer, pending_log_responses)
                    .unwrap();

                let span = tracing::span!(
                    Level::INFO,
                    "persistence_append_vote",
                    id = id,
                    term = term,
                    candidate = candidate_id
                );
                span.set_parent(parent_span.context());
                let _enter = span.enter();
                log::info!("Saving vote message {}, term {}, candidate {}", id, term, candidate_id);
                self.persist_vote(term, candidate_id, vote_write_buffer).unwrap();
                self.response_queue
                    .push(PersistenceResponseType::VotePersisted { id, term, candidate_id })
            }
            PersistenceTaskType::AppendLog {
                id,
                data,
                parent_span,
                request_id,
            } => {
                log::debug!(
                    "Appending log entry with id {} and data length {}",
                    request_id,
                    data.len()
                );
                let span = tracing::span!(Level::INFO, "persistence_append_log", id = id, bytes = data.len());
                span.set_parent(parent_span.context());
                let _enter = span.enter();
                tracing::debug!(
                    "Appending log entry with id {} and data length {}",
                    request_id,
                    data.len()
                );

                let data_len = data.len() as u64;

                // Write to buffer without flushing
                self.write_log_entry(data, log_write_buffer).unwrap();

                // Track byte end offset for this entry
                let last_end = self.entry_end_offsets.last().copied().unwrap_or(0);
                self.entry_end_offsets.push(last_end + data_len);

                // Queue response for later (after flush)
                pending_log_responses.push(PersistenceResponseType::LogPersisted { id });

                // Flush if batch is full
                if pending_log_responses.len() >= batch_size {
                    let flushed = self
                        .flush_pending_logs(log_write_buffer, pending_log_responses)
                        .unwrap();
                    if flushed > 1 {
                        tracing::debug!("Batched flush of {} log entries", flushed);
                    }
                }
                tracing::debug!("Append log entry done");
            }
            PersistenceTaskType::TruncateLog { target_length } => {
                // Flush any pending log writes before truncation
                self.flush_pending_logs(log_write_buffer, pending_log_responses)
                    .unwrap();

                let truncation_position = if target_length == 0 {
                    0
                } else {
                    self.entry_end_offsets[target_length - 1]
                };

                log::info!(
                    "Truncating log file to {} entries ({} bytes)",
                    target_length,
                    truncation_position
                );

                // Truncate the file on disk
                log_write_buffer
                    .get_ref()
                    .set_len(truncation_position)
                    .expect("Failed to truncate log file");

                // Truncate the offset tracking
                self.entry_end_offsets.truncate(target_length);

                log::info!(
                    "Log file truncated successfully, entry_end_offsets len={}",
                    self.entry_end_offsets.len()
                );
            }
        }
    }

    pub fn persist_vote(
        &mut self,
        term: u64,
        candidate_id: u32,
        file_handle: &mut Writer<File>,
    ) -> Result<(), Box<dyn Error>> {
        file_handle.serialize(VoteRow { term, candidate_id })?;
        file_handle.flush()?;
        Ok(())
    }

    /// Writes a log entry to the buffer without flushing.
    /// Used for batching multiple writes before a single flush.
    fn write_log_entry(&self, buffer: Arc<Vec<u8>>, file_handle: &mut BufWriter<File>) -> io::Result<()> {
        file_handle.write_all(&buffer)?;
        Ok(())
    }

    /// Flushes pending log writes and sends all queued responses.
    /// Returns the number of responses sent.
    fn flush_pending_logs(
        &self,
        file_handle: &mut BufWriter<File>,
        pending_responses: &mut Vec<PersistenceResponseType>,
    ) -> io::Result<usize> {
        if pending_responses.is_empty() {
            return Ok(0);
        }

        file_handle.flush()?;

        let count = pending_responses.len();
        for resp in pending_responses.drain(..) {
            self.response_queue.push(resp);
        }

        Ok(count)
    }

    pub fn read_votes(&mut self) -> Result<Vec<VoteRow>, Box<dyn Error>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}votes.csv", self.persistence_config.out_dir))?;

        let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);
        let mut votes = vec![];
        for result in rdr.deserialize() {
            // Notice that we need to provide a type hint for automatic
            // deserialization.
            let record: VoteRow = result?;
            votes.push(record)
        }
        Ok(votes)
    }

    pub fn read_log(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}log.capnp", self.persistence_config.out_dir))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_worker(
        dir: &str,
        entry_end_offsets: Vec<u64>,
    ) -> (
        PersistenceWorker,
        crossbeam_channel::Sender<PersistenceTaskType>,
        Arc<SegQueue<PersistenceResponseType>>,
    ) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let response_queue = Arc::new(SegQueue::new());
        let worker = PersistenceWorker::new(
            rx,
            Arc::clone(&response_queue),
            PersistenceConfig {
                out_dir: dir.to_string(),
            },
            entry_end_offsets,
        );
        (worker, tx, response_queue)
    }

    #[test]
    fn test_append_log_tracks_offsets() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = format!("{}/", tmp_dir.path().to_str().unwrap());
        let (mut worker, tx, response_queue) = create_test_worker(&dir, vec![]);

        // Send some AppendLog tasks, then disconnect
        let data1 = Arc::new(vec![1u8; 100]);
        let data2 = Arc::new(vec![2u8; 200]);
        let data3 = Arc::new(vec![3u8; 50]);

        tx.send(PersistenceTaskType::AppendLog {
            id: 1,
            data: data1,
            parent_span: tracing::Span::none(),
            request_id: 1,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 2,
            data: data2,
            parent_span: tracing::Span::none(),
            request_id: 2,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 3,
            data: data3,
            parent_span: tracing::Span::none(),
            request_id: 3,
        })
        .unwrap();

        drop(tx); // Disconnect to let run() exit
        worker.run();

        // Verify offsets
        assert_eq!(worker.entry_end_offsets, vec![100, 300, 350]);

        // Verify responses were sent
        assert_eq!(response_queue.len(), 3);

        // Verify file size
        let file_size = fs::metadata(format!("{}log.capnp", dir)).unwrap().len();
        assert_eq!(file_size, 350);
    }

    #[test]
    fn test_truncate_log_shortens_file_and_offsets() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = format!("{}/", tmp_dir.path().to_str().unwrap());
        let (mut worker, tx, _response_queue) = create_test_worker(&dir, vec![]);

        // Append 3 entries
        let data1 = Arc::new(vec![1u8; 100]);
        let data2 = Arc::new(vec![2u8; 200]);
        let data3 = Arc::new(vec![3u8; 50]);

        tx.send(PersistenceTaskType::AppendLog {
            id: 1,
            data: data1,
            parent_span: tracing::Span::none(),
            request_id: 1,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 2,
            data: data2,
            parent_span: tracing::Span::none(),
            request_id: 2,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 3,
            data: data3,
            parent_span: tracing::Span::none(),
            request_id: 3,
        })
        .unwrap();

        // Truncate to 2 entries
        tx.send(PersistenceTaskType::TruncateLog { target_length: 2 }).unwrap();

        drop(tx);
        worker.run();

        // Verify offsets truncated
        assert_eq!(worker.entry_end_offsets, vec![100, 300]);

        // Verify file was truncated
        let file_size = fs::metadata(format!("{}log.capnp", dir)).unwrap().len();
        assert_eq!(file_size, 300);
    }

    #[test]
    fn test_truncate_log_to_zero() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = format!("{}/", tmp_dir.path().to_str().unwrap());
        let (mut worker, tx, _response_queue) = create_test_worker(&dir, vec![]);

        // Append 2 entries
        tx.send(PersistenceTaskType::AppendLog {
            id: 1,
            data: Arc::new(vec![1u8; 100]),
            parent_span: tracing::Span::none(),
            request_id: 1,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 2,
            data: Arc::new(vec![2u8; 200]),
            parent_span: tracing::Span::none(),
            request_id: 2,
        })
        .unwrap();

        // Truncate to 0
        tx.send(PersistenceTaskType::TruncateLog { target_length: 0 }).unwrap();

        drop(tx);
        worker.run();

        assert!(worker.entry_end_offsets.is_empty());

        let file_size = fs::metadata(format!("{}log.capnp", dir)).unwrap().len();
        assert_eq!(file_size, 0);
    }

    #[test]
    fn test_truncate_then_append() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = format!("{}/", tmp_dir.path().to_str().unwrap());
        let (mut worker, tx, _response_queue) = create_test_worker(&dir, vec![]);

        // Append 3 entries
        tx.send(PersistenceTaskType::AppendLog {
            id: 1,
            data: Arc::new(vec![1u8; 100]),
            parent_span: tracing::Span::none(),
            request_id: 1,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 2,
            data: Arc::new(vec![2u8; 200]),
            parent_span: tracing::Span::none(),
            request_id: 2,
        })
        .unwrap();
        tx.send(PersistenceTaskType::AppendLog {
            id: 3,
            data: Arc::new(vec![3u8; 50]),
            parent_span: tracing::Span::none(),
            request_id: 3,
        })
        .unwrap();

        // Truncate to 1 entry
        tx.send(PersistenceTaskType::TruncateLog { target_length: 1 }).unwrap();

        // Append a new entry after truncation
        tx.send(PersistenceTaskType::AppendLog {
            id: 4,
            data: Arc::new(vec![4u8; 75]),
            parent_span: tracing::Span::none(),
            request_id: 4,
        })
        .unwrap();

        drop(tx);
        worker.run();

        // After truncate to 1 (offset 100), then append 75 bytes
        assert_eq!(worker.entry_end_offsets, vec![100, 175]);

        let file_size = fs::metadata(format!("{}log.capnp", dir)).unwrap().len();
        assert_eq!(file_size, 175);
    }

    #[test]
    fn test_worker_with_initial_offsets() {
        let tmp_dir = TempDir::new().unwrap();
        let dir = format!("{}/", tmp_dir.path().to_str().unwrap());

        // Pre-create a log file with some data
        let log_path = format!("{}log.capnp", dir);
        fs::write(&log_path, vec![0u8; 500]).unwrap();

        let initial_offsets = vec![100, 300, 500];
        let (mut worker, tx, _response_queue) = create_test_worker(&dir, initial_offsets);

        // Truncate to 2 entries
        tx.send(PersistenceTaskType::TruncateLog { target_length: 2 }).unwrap();

        drop(tx);
        worker.run();

        assert_eq!(worker.entry_end_offsets, vec![100, 300]);

        let file_size = fs::metadata(&log_path).unwrap().len();
        assert_eq!(file_size, 300);
    }
}
