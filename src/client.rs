use std::ops::Sub;
use std::sync::{Arc, Mutex};
use ping::ping_pong_client::{PingPongClient};
use ping::{PingRequest};
use raftproto::raft_client::{RaftClient};
use raftproto::{AppendEntriesRequest, VoteRequest};
use futures::future;
use tonic::transport::Channel;
use std::time::{Duration, Instant};
use datastoreproto::datastore_client::{DatastoreClient};
use crate::datastoreproto::PutDataStoreRecordRequest;
use chrono::Utc;

pub mod ping {
    tonic::include_proto!("ping"); // The string specified here must match the proto package name
}

pub mod raftproto {
    tonic::include_proto!("raftproto"); // The string specified here must match the proto package name
}

pub mod datastoreproto {
    tonic::include_proto!("datastoreproto"); // The string specified here must match the proto package name
}

async fn do_ping() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PingPongClient::connect("http://[::1]:50052").await?;

    let request = tonic::Request::new(PingRequest {
    });

    let response = client.ping(request).await?;

    println!("RESPONSE={:?}", response.into_inner());

    Ok(())
}

async fn do_append_entries() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RaftClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(AppendEntriesRequest {
        request_id: 0,
        term: 1,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
    });
    let response = client.append_entries(request).await?;
    println!("RESPONSE={:?}", response.into_inner());

    Ok(())
}

async fn do_write_batch_async() -> Result<(), Box<dyn std::error::Error>> {
    // Define the number of requests to make
    let n_requests = 5000;
    // Create a shared channel
    let channel = Channel::from_static("http://[::1]:50051")
        .connect()
        .await?;
    let channel2 = Channel::from_static("http://[::1]:50052")
        .connect()
        .await?;
    let channel3 = Channel::from_static("http://[::1]:50053")
        .connect()
        .await?;
    let chans = vec![channel, channel2, channel3];
    let now = Utc::now();

    let times: Vec<i64> = vec![];
    let safe_times = Arc::new(Mutex::new(times));
    let tasks = (0..n_requests).map(|i| {
        let t = Arc::clone(&safe_times);
        let mut client = DatastoreClient::new(chans[i%chans.len()].clone());
        async move {
            let mut request = tonic::Request::new(PutDataStoreRecordRequest {
                payload: "some_value".to_string(),
            });
            request.set_timeout(Duration::from_secs(1));

            let start = Utc::now();
            match client.put_record(request).await {
                Ok(response) => {
                    let done_time = Utc::now();
                    let taken = done_time.sub(start);
                    let taken_millis = taken.num_milliseconds();
                    if taken_millis > 1000 {
                        //println!("Slow response {}, start: {}, end: {}", taken_millis, start.to_rfc3339(), done_time.to_rfc3339());
                    }
                    let mut v = t.lock().unwrap();
                    v.push(taken_millis);
                }
                Err(e) => {
                    eprintln!("Task {} ERROR={:?}", i, e);
                }
            }
        }
    });

    future::join_all(tasks).await;
    let mut t = safe_times.lock().unwrap();
    let sum: i64 = t.iter().sum();
    t.sort();
    let fastest = t.get(0).unwrap();
    let p50 = t.get((t.len() as f32 / 2f32) as usize).unwrap();
    let p75 = t.get((t.len() as f32 * 0.75f32) as usize).unwrap();
    let p95 = t.get((t.len() as f32 * 0.95f32) as usize).unwrap();
    let slowest = t.get(t.len()-1).unwrap();

    let avg = sum / t.len() as i64;

    let elapsed = Utc::now().sub(now);
    log::info!("Elapsed: {}, Avg latency: {}, fastest: {}, slowest: {}, p50: {}, p75: {}, p95: {}", elapsed, avg, fastest, slowest, p50, p75, p95);

    Ok(())
}



async fn do_append_entries_async() -> Result<(), Box<dyn std::error::Error>> {
    // Define the number of requests to make
    let n_requests = 100000;
    // Create a shared channel
    let channel = Channel::from_static("http://[::1]:50051")
        .connect()
        .await?;
    let now = Instant::now();

    // Use an `Arc` to share the client across tasks (or create new clients per request)
    //let client = Arc::new(RaftClient::connect("http://[::1]:50051").await?);

    // Create a vector of async tasks
    let tasks = (0..n_requests).map(|i| {

        let mut client = RaftClient::new(channel.clone());
        async move {
            let request = tonic::Request::new(AppendEntriesRequest {
                request_id: 0,
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
            });

            // Make the gRPC request
            match client.append_entries(request).await {
                Ok(response) => {
                    //println!("Task {} RESPONSE={:?}", i, response.into_inner());
                }
                Err(e) => {
                    eprintln!("Task {} ERROR={:?}", i, e);
                }
            }
        }
    });

    // Await all tasks in parallel
    future::join_all(tasks).await;
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder().filter_level(log::LevelFilter::Info).init();
    let mut iterations = 0;
    let mut last_time = Instant::now();
    loop {
        //do_append_entries_async().await?;
        do_write_batch_async().await?;
        iterations += 1;
        if iterations % 10 == 0 {
            log::info!("10 iterations completed after {}", last_time.elapsed().as_millis());
            last_time = Instant::now();
        }
    }

    Ok(())
}