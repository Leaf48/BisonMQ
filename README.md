# BisonMQ
A Lightweight and Simple Rust Library for Job Queue

# The gist
## installation
```
cargo add bisonmq
```

Push a job to the queue
```Rust
let job_queue: JobQueue = JobQueue::new("redis://127.0.0.1/", queue_key)?;
// return the length of the queue
let len: i64 = job_queue.push_job(&queue_value)?;
```

Pop a job from the queue
```Rust
let job_queue: JobQueue = JobQueue::new("redis://127.0.0.1/", queue_key)?;

// 0.0 means waiting forever
let result: (String, String) = job_queue.pop_job(0.0);
```
