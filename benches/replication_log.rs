// benches/replication_log.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::thread;
use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use im::Vector;
use kraft_lib::transport::capnp::{build_owned_log_entry, OwnedLogEntry};

const DATA_SIZE: usize = 30000;

/// Create a test log entry with the given index and payload size
fn create_log_entry(index: u64, payload_size: usize) -> OwnedLogEntry {
    build_owned_log_entry(|mut builder| {
        builder.set_index(index);
        builder.set_term(1);
        builder.set_prev_log_index(index.saturating_sub(1));
        builder.set_prev_log_term(1);
        builder.set_message_id(index);
        builder.set_timestamp(12345678);
        let mut commands = builder.init_commands(1);
        let mut cmd = commands.reborrow().get(0);
        cmd.set_id(&format!("key_{}", index));
        cmd.set_payload(&vec![(index % 255) as u8; payload_size]);
        cmd.set_node_id(1);
    })
}

fn solution_one(data: Vec<OwnedLogEntry>) {
    let replication_vector: Arc<ArcSwap<Vector<Arc<OwnedLogEntry>>>> =
        Arc::new(ArcSwap::from_pointee(Vector::<Arc<OwnedLogEntry>>::new()));

    // Create 10.000 x OwnedLogEntry
    // Append them in writer thread
    // Read them in reader threads

    let r_vec = Arc::clone(&replication_vector);
    let handle = thread::spawn(move || {
        let mut v = Vector::<Arc<OwnedLogEntry>>::new();
        for item in data {
            v.push_back(Arc::new(item));
            r_vec.store(Arc::new(v.clone()));
        }
    });

    let r_vec2 = Arc::clone(&replication_vector);
    let r_vec3 = Arc::clone(&replication_vector);
    let r_vec4 = Arc::clone(&replication_vector);
    // Create 3 separate threads that while current index is less than 10.000 keeps reading next index;
    let handle2 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = r_vec2.load().get(i) {
                black_box(data);
                i += 1;
            }
        }
    });
    let handle3 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = r_vec3.load().get(i) {
                black_box(data);
                i += 1;
            }
        }
    });
    let handle4 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = r_vec4.load().get(i) {
                black_box(data);
                i += 1;
            }
        }
    });

    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    handle4.join().unwrap();
}

fn solution_two(data: Vec<OwnedLogEntry>) {
    // Create 10.000 x OwnedLogEntry
    // Send them over channels in writer thread to each reader thread
    // Clones whole data and sends it over the channel

    let queue_1: Arc<SegQueue<OwnedLogEntry>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<OwnedLogEntry>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<OwnedLogEntry>> = Arc::new(SegQueue::new());
    let write_queue_3 = Arc::clone(&queue_3);

    let handle = thread::spawn(move || {
        for item in data {
            write_queue_1.push(item.clone());
            write_queue_2.push(item.clone());
            write_queue_3.push(item);
        }
    });

    let handle2 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_1.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle3 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_2.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle4 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_3.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    handle4.join().unwrap();
}

fn solution_three(data: Vec<OwnedLogEntry>) {
    // Create 10.000 x OwnedLogEntry
    // Send them over channels in writer thread to each reader thread
    // Use Arc to avoid cloning the data and clones just the Arc pointer

    let queue_1: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_3 = Arc::clone(&queue_3);

    let handle = thread::spawn(move || {
        for item in data {
            // Use Arc to share ownership of the data
            let arc_item = Arc::new(item);
            write_queue_1.push(Arc::clone(&arc_item));
            write_queue_2.push(Arc::clone(&arc_item));
            write_queue_3.push(arc_item);
        }
    });

    let handle2 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_1.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle3 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_2.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle4 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_3.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    handle4.join().unwrap();
}

fn solution_four(data: Vec<OwnedLogEntry>) {
    // Create 10.000 x OwnedLogEntry
    // Send them over channels in writer thread to each reader thread
    // Keep a local copy in the writer

    let queue_1: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_3 = Arc::clone(&queue_3);

    let handle = thread::spawn(move || {
        let mut local_vec: Vec<Arc<OwnedLogEntry>> = Vec::with_capacity(100000);
        for item in data {
            // Use Arc to share ownership of the data
            let arc_item = Arc::new(item);
            write_queue_1.push(Arc::clone(&arc_item));
            write_queue_2.push(Arc::clone(&arc_item));
            write_queue_3.push(Arc::clone(&arc_item));
            local_vec.push(arc_item);
        }
        black_box(local_vec);
    });

    let handle2 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_1.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle3 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_2.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle4 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_3.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    handle4.join().unwrap();
}

fn solution_five(data: Vec<OwnedLogEntry>) {
    let queue_1: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<OwnedLogEntry>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Vec<Arc<OwnedLogEntry>>>> = Arc::new(SegQueue::new());
    let write_clone_queue_1: Arc<SegQueue<Vec<Arc<OwnedLogEntry>>>> = Arc::new(SegQueue::new());

    let handle = thread::spawn(move || {
        let mut local_vec: Vec<Arc<OwnedLogEntry>> = Vec::with_capacity(100000);
        for item in data {
            let arc_item = Arc::new(item);
            write_queue_1.push(Arc::clone(&arc_item));
            write_queue_2.push(Arc::clone(&arc_item));
            local_vec.push(arc_item);
        }
        write_clone_queue_1.push(local_vec.clone());
        black_box(local_vec);
    });

    let handle2 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_1.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle3 = thread::spawn(move || {
        let mut i = 0;
        while i < DATA_SIZE {
            if let Some(data) = queue_2.pop() {
                black_box(data);
                i += 1;
            }
        }
    });

    let handle4 = thread::spawn(move || {
        if let Some(data) = queue_3.pop() {
            black_box(data);
        }
    });

    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    handle4.join().unwrap();
}

fn solution_six(data: Vec<OwnedLogEntry>) {
    let asd = Arc::new(data);

    let clone1 = Arc::clone(&asd);
    let clone2 = Arc::clone(&asd);
    let clone3 = Arc::clone(&asd);

    let handle = thread::spawn(move || {
        let v = clone1;
        for data in v.iter() {
            black_box(data);
        }
    });

    let handle2 = thread::spawn(move || {
        let v = clone2;
        for data in v.iter() {
            black_box(data);
        }
    });

    let handle3 = thread::spawn(move || {
        let v = clone3;
        for data in v.iter() {
            black_box(data);
        }
    });

    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
}

fn benchmark(c: &mut Criterion) {
    let mut log_entries: Vec<OwnedLogEntry> = vec![];
    for i in 0..DATA_SIZE + 2 {
        log_entries.push(create_log_entry(i as u64, 2000));
    }

    c.bench_function("solution_one", |b| b.iter(|| solution_one(log_entries.clone())));
    c.bench_function("solution_two", |b| b.iter(|| solution_two(log_entries.clone())));
    c.bench_function("solution_three", |b| b.iter(|| solution_three(log_entries.clone())));
    c.bench_function("solution_four", |b| b.iter(|| solution_four(log_entries.clone())));
    c.bench_function("solution_five", |b| b.iter(|| solution_five(log_entries.clone())));
    c.bench_function("solution_six", |b| b.iter(|| solution_six(log_entries.clone())));
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
