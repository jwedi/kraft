use std::error::Error;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufWriter, Bytes, Write};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Condvar};
use std::thread::sleep;
use std::time::Duration;
use crossbeam_queue::SegQueue;
use csv::{ReaderBuilder, Writer, WriterBuilder};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub enum PersistenceTaskType {
    WriteState{id: u64, span: Span},
    ReadState{id: u64, span: Span},
    AppendVote{id: u64, term: u64, candidate_id: u32, parent_span: Span},
    ReadVotes{id: u64, span: Span},
    AppendLog{id: u64, data: Vec<u8>, parent_span: Span}
    // Can either be RC / Arc and immutable, or copied, or immutable and the persistence worker serializes while waiting for IO sync.
    // Preparing next batch while waiting for IO sync seems reasonable.
    // Now sure how to avoid IO if that's the case.
    // If the input is immutable structs and the worker does the serialization then the persistence worker could reuse the same byte buffer.
    // The caller wouldn't know the size of each entry though which makes batching harder. If the caller sends the serialized data then it knows the size and can properly batch it. I think that has to be the way to go.
    // I can do some unsafe stuff, send an RC and then free the buffer when the persistence worker returns because i know that the persistence worker wouldn't be using the buffer anymore.
    // Alternative is that the caller attaches the whole memory arena and the persistence worker is the one that clears it. So it's one allocation per batch.
    // Writing data to the byte buffer still requires the writer to know the size of the entry before serializing it into the given buffer.
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct VoteRow {
    pub term: u64,
    pub candidate_id: u32,
}

pub enum PersistenceResponseType {
    VotePersisted{id: u64, term: u64, candidate_id: u32},
    LogPersisted{id: u64}
}

pub struct PersistenceConfig {
    pub out_dir: String
}

// Writes persistent state such as term votes to disk
// Writes append entries to disk
// Reads persistent state from disk
// Reads log from disk.
pub struct PersistenceWorker {
    work_queue: Arc<SegQueue<PersistenceTaskType>>,
    response_queue: Arc<SegQueue<PersistenceResponseType>>,
    condvar: Arc<Condvar>,
    persistence_config: PersistenceConfig
}

impl PersistenceWorker {

    pub fn new(
        work_queue: Arc<SegQueue<PersistenceTaskType>>,
        response_queue: Arc<SegQueue<PersistenceResponseType>>,
        condvar: Arc<Condvar>,
        persistence_config: PersistenceConfig
    ) -> Self {
        Self {
            work_queue,
            response_queue,
            condvar,
            persistence_config
        }
    }

    pub fn run(&mut self) {
        tracing::info!("Running persistence worker");
        loop {
            let queue_len = self.work_queue.len();
            if queue_len > 5 {
                tracing::info!("Long persistence queue len: {}", queue_len);
            }
            let task = self.work_queue.pop();
            match task {
                Some(task) => {
                    tracing::debug!("Received persistence task");
                    match task {
                        PersistenceTaskType::AppendVote{id, term, candidate_id, parent_span} => {
                            let span = tracing::span!(Level::INFO, "persistence_append_vote", id=id, term=term, candidate=candidate_id);
                            span.set_parent(parent_span.context());
                            let _enter = span.enter();
                            tracing::info!("Saving vote message {}, term {}, candidate {}", id, term, candidate_id);
                            self.persist_vote(term, candidate_id).unwrap();
                            self.response_queue.push(PersistenceResponseType::VotePersisted{id, term, candidate_id})
                        }
                        PersistenceTaskType::ReadVotes{id, span} => {
                            let _enter = span.enter();
                        }
                        PersistenceTaskType::ReadState{id, span} => {
                            let _enter = span.enter();
                        }
                        PersistenceTaskType::WriteState{id, span} => {
                            let _enter = span.enter();
                        }
                        PersistenceTaskType::AppendLog {id, data, parent_span} => {
                            let span = tracing::span!(Level::INFO, "persistence_append_log", id=id, bytes=data.len());
                            span.set_parent(parent_span.context());
                            let _enter = span.enter();
                            self.append_log(&data).unwrap();
                            self.response_queue.push(PersistenceResponseType::LogPersisted{id})
                        }
                    }
                }
                None => {
                    // Condvar wait or sleep
                    sleep(Duration::from_micros(1000))
                }
            }
        }
    }

    pub fn persist_vote(&mut self, term: u64, candidate_id: u32) -> Result<(), Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("{}votes.csv", self.persistence_config.out_dir))
            .unwrap();
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(file);
        wtr.serialize(VoteRow {
            term,
            candidate_id
        })?;
        wtr.flush()?;
        Ok(())
    }

    fn append_log(&mut self, buffer: &[u8]) -> io::Result<()> {
        let mut file_handle = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(format!("{}log.pb", self.persistence_config.out_dir))
            .unwrap();
        let mut file = BufWriter::new(file_handle);
        file.write_all(buffer)?;
        Ok(())
    }

    pub fn read_votes(&mut self) -> Result<Vec<VoteRow>, Box<dyn Error>> {
        let mut file = OpenOptions::new()
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
            log::info!("{:?}", record);
            votes.push(record)
        }
        Ok(votes)
    }
}