use redis::{Client, Commands, RedisResult};

pub struct JobQueue {
    client: Client,
    queue_key: String,
}

impl JobQueue {
    pub fn new(redis_url: &str, queue_key: &str) -> RedisResult<Self> {
        let client = Client::open(redis_url)?;

        Ok(Self {
            client,
            queue_key: queue_key.to_string(),
        })
    }

    pub fn push_job(&self, queue_value: &str) -> RedisResult<i64> {
        let mut conn = self.client.get_connection()?;
        let len: i64 = conn.lpush(&self.queue_key, queue_value)?;

        Ok(len)
    }

    pub fn pop_job(&self, timeout: f64) -> RedisResult<(String, String)> {
        let mut con = self.client.get_connection()?;

        let result: (String, String) = con.blpop(&self.queue_key, timeout)?;

        Ok(result)
    }

    pub fn del_queue(&self) -> RedisResult<()> {
        let mut con = self.client.get_connection()?;

        let _: () = con.del(&self.queue_key)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::JobQueue;

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
}
