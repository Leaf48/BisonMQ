use std::{thread, time::Duration};

use redis::{Client, Commands, RedisResult};

pub struct JobQueue {
    client: Client,
    queue_key: String,
}

impl JobQueue {
    /// Create a new JobQueue instance.
    ///
    /// # Arguments
    ///
    /// * `redis_url` - The URL to the Redis server (e.g., "redis://127.0.0.1/").
    /// * `queue_key` - The key(name) in Redis that all data will be stored.
    ///
    /// # Returns
    ///
    /// Returns a `JobQueue` instance on success, or Redis error on failure.
    pub fn new(redis_url: &str, queue_key: &str) -> RedisResult<Self> {
        let client = Client::open(redis_url)?;

        Ok(Self {
            client,
            queue_key: queue_key.to_string(),
        })
    }

    /// Push a job into the queue
    ///
    /// # Arguments
    ///
    /// * `queue_value` - The value to push into the queue (e.g. JSON String)
    ///
    /// # Returns
    ///
    /// Return the current length of queue
    pub fn push_job(&self, queue_value: &str) -> RedisResult<i64> {
        let mut conn = self.client.get_connection()?;
        let len: i64 = conn.lpush(&self.queue_key, queue_value)?;

        Ok(len)
    }

    /// Pop a first element in the queue
    ///
    /// # Arguments
    ///
    /// * `timeout` - The timeout value in seconds for the BLPOP command (e.g. 0.0 for no limit).
    ///
    /// # Returns
    ///
    /// Return a tuple `(queue_key, value)` on success, wrapped in `RedisResult`.
    pub fn pop_job(&self, timeout: f64) -> RedisResult<(String, String)> {
        let mut con = self.client.get_connection()?;

        let result: (String, String) = con.blpop(&self.queue_key, timeout)?;

        Ok(result)
    }

    /// Reset the entire queue by deleting the queue.
    ///
    /// # Returns
    ///
    /// Return Ok(()) on success, or Redis error on failure.
    pub fn del_queue(&self) -> RedisResult<()> {
        let mut con = self.client.get_connection()?;

        let _: () = con.del(&self.queue_key)?;

        Ok(())
    }

    /// Listener to run the provided callback each time a job is received.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The timeout value in seconds for the BLPOP command (e.g. 0.0 for no limit).
    /// * `callback` - The callback function that takes a tuple `(queue_key, value)`.
    ///
    /// # Returns
    ///
    /// Return a `JoinHandle<()>` for the listener thread.
    pub fn listen<F>(&self, timeout: f64, callback: F) -> thread::JoinHandle<()>
    where
        F: Fn((String, String)) + Send + 'static,
        Client: Clone,
    {
        let client = self.client.clone();
        let queue_key = self.queue_key.clone();

        thread::spawn(move || loop {
            // attempt to get a connection
            let mut con = match client.get_connection() {
                Ok(conn) => conn,
                Err(err) => {
                    eprintln!("[Listener] Connection error: {}", err);
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };

            // Execute BLPOP; the callback is called on receiving a new job.
            match con.blpop(&queue_key, timeout) {
                Ok(res) => callback(res),
                Err(err) => {
                    eprintln!("[Listener] BLPOP Error: {}", err);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::JobQueue;

    /// Test that a single job can be pushed and popped correctly.
    #[test]
    fn job_queue_test() {
        let queue_key = "test_queue_test";
        let queue_value = "test_queue_value";

        let job_queue = JobQueue::new("redis://127.0.0.1/", queue_key).unwrap();
        job_queue.del_queue().unwrap();

        let len = job_queue.push_job(&queue_value).unwrap();
        assert_eq!(len, 1);

        let result = job_queue.pop_job(0.0).unwrap();

        println!("{:?}", result);
        assert_eq!(result.1, queue_value)
    }

    /// Test that multiple pushing are handled in a separate thread.
    #[test]
    fn job_queue_multi_push_test() {
        let queue_key = "test_queue_multi_push";

        let job_queue = Arc::new(JobQueue::new("redis://127.0.0.1/", queue_key).unwrap());
        job_queue.del_queue().unwrap();

        let consumer_queue = Arc::clone(&job_queue);
        let consumer_handle = thread::spawn(move || {
            for i in 0..5 {
                match consumer_queue.pop_job(0.0) {
                    Ok(job) => {
                        println!("[Consumer] {}: Queue key - {}, value - {}", i, job.0, job.1)
                    }
                    Err(e) => eprintln!("{:?}", e),
                }
            }
        });

        for i in 0..5 {
            let queue_value = format!("job_{}", i);

            job_queue
                .push_job(&queue_value)
                .expect("[Producer] Failed to push a job");

            println!("[Producer] Pushed {}", queue_value);
            thread::sleep(Duration::from_secs(1));
        }

        consumer_handle.join().unwrap();
    }

    /// Test that the listener functionality.
    #[test]
    fn job_queue_listener_test() {
        let queue_key = "test_queue_listener";

        let job_queue = JobQueue::new("redis://127.0.0.1/", queue_key).unwrap();
        let listen_handler = job_queue.listen(0.0, move |(q, value)| {
            println!("[Listener] Queue: {}, Job: {}", q, value);
        });

        for i in 0..5 {
            let new_job = format!("job_number_{}", i);
            let num = job_queue.push_job(&new_job).unwrap();
            println!("[Producer] Pushed: {}. Current length: {}", new_job, num);
            thread::sleep(Duration::from_millis(500));
        }

        thread::sleep(Duration::from_secs(1));

        // listen_handler.join();
    }
}
