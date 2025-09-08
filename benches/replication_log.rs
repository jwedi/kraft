// benches/my_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::{Arc, mpsc};
use std::thread;
use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use im::Vector;
use kraft_lib::service_utils::storage_utils::SerializedData;

#[macro_use]
extern crate criterion;

const DATA_SIZE: usize = 30000;

fn solution_one(data: Vec<SerializedData>) {
    let replication_vector: Arc<ArcSwap<Vector<Arc<SerializedData>>>> = Arc::new(ArcSwap::from_pointee(Vector::<Arc<SerializedData>>::new()));


    // Create 10.000 x SerializedData
    // Append them in writer thread
    // Read them in reader threads

    let r_vec = Arc::clone(&replication_vector);
    let handle = thread::spawn(move || {
        let mut v = Vector::<Arc<SerializedData>>::new();
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

fn solution_two(data: Vec<SerializedData>) {
    // Create 10.000 x SerializedData
    // Send them over channels in writer thread to each reader thread
    // Clones whole data and sends it over the channel

    let queue_1: Arc<SegQueue<SerializedData>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<SerializedData>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<SerializedData>> = Arc::new(SegQueue::new());
    let write_queue_3 = Arc::clone(&queue_3);

    let handle = thread::spawn(move || {
        for item in data {
            write_queue_1.push(item.clone());
            write_queue_2.push(item.clone());
            write_queue_3.push(item.clone());
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

fn solution_three(data: Vec<SerializedData>) {
    // Create 10.000 x SerializedData
    // Send them over channels in writer thread to each reader thread
    // Use Arc to avoid cloning the data and clones just the Arc pointer

    let queue_1: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_3 = Arc::clone(&queue_3);

    let handle = thread::spawn(move || {
        for item in data {
            // Use Arc to share ownership of the data
            let arc_item = Arc::new(item);
            write_queue_1.push(Arc::clone(&arc_item));
            write_queue_2.push(Arc::clone(&arc_item));
            write_queue_3.push(Arc::clone(&arc_item));
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

fn solution_four(data: Vec<SerializedData>) {
    // Create 10.000 x SerializedData
    // Send them over channels in writer thread to each reader thread

    let queue_1: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_3 = Arc::clone(&queue_3);

    let handle = thread::spawn(move || {
        let mut local_vec: Vec<Arc<SerializedData>> = Vec::with_capacity(100000);
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

fn solution_five(data: Vec<SerializedData>) {

    let queue_1: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Vec<Arc<SerializedData>>>> = Arc::new(SegQueue::new());
    let write_clone_queue_1: Arc<SegQueue<Vec<Arc<SerializedData>>>> = Arc::new(SegQueue::new());

    let handle = thread::spawn(move || {
        let mut local_vec: Vec<Arc<SerializedData>> = Vec::with_capacity(100000);
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

fn solution_six(data: Vec<SerializedData>) {

    let queue_1: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_1 = Arc::clone(&queue_1);

    let queue_2: Arc<SegQueue<Arc<SerializedData>>> = Arc::new(SegQueue::new());
    let write_queue_2 = Arc::clone(&queue_2);

    let queue_3: Arc<SegQueue<Vec<Arc<SerializedData>>>> = Arc::new(SegQueue::new());
    let write_clone_queue_1: Arc<SegQueue<Vec<Arc<SerializedData>>>> = Arc::new(SegQueue::new());

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
        let mut i = 0;
        for data in v.iter() {
            black_box(data);
        }
    });

    let handle3 = thread::spawn(move || {
        let v = clone3;
        let mut i = 0;
        for data in v.iter() {
            black_box(data);
        }
    });


    handle.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
}

fn benchmark(c: &mut Criterion) {
    let mut serialized_data: Vec<SerializedData> = vec![];
    for i in 0..DATA_SIZE+2 {
        let d = SerializedData{
            data: vec![(i%255) as u8; 2000]
        };
        serialized_data.push(d);
    }

    c.bench_function("solution_one", |b| b.iter(|| solution_one(serialized_data.clone())));
    c.bench_function("solution_two", |b| b.iter(|| solution_two(serialized_data.clone())));
    c.bench_function("solution_three", |b| b.iter(|| solution_three(serialized_data.clone())));
    c.bench_function("solution_four", |b| b.iter(|| solution_four(serialized_data.clone())));
    c.bench_function("solution_five", |b| b.iter(|| solution_five(serialized_data.clone())));
    c.bench_function("solution_six", |b| b.iter(|| solution_six(serialized_data.clone())));
}

criterion_group!(benches, benchmark);
criterion_main!(benches);