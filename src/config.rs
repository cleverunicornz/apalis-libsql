//! Configuration for LibsqlStorage

use std::time::Duration;

use apalis_core::backend::queue::Queue;

/// Configuration for the LibsqlStorage backend
#[derive(Debug, Clone)]
pub struct Config {
    queue: Queue,
    buffer_size: usize,
    poll_interval: Duration,
    keep_alive: Duration,
    reenqueue_orphaned_after: Duration,
}

impl Config {
    /// Create a new config with the given queue name
    #[must_use]
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: Queue::from(queue.into()),
            buffer_size: 10,
            poll_interval: Duration::from_millis(100),
            keep_alive: Duration::from_secs(30),
            reenqueue_orphaned_after: Duration::from_secs(300),
        }
    }

    /// Get the queue
    #[must_use]
    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    /// Get the buffer size
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Set the buffer size
    ///
    /// # Panics
    ///
    /// Panics if `size` is 0.
    #[must_use]
    pub fn set_buffer_size(mut self, size: usize) -> Self {
        assert!(size > 0, "Buffer size must be greater than 0");
        self.buffer_size = size;
        self
    }

    /// Get the poll interval
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Set the poll interval
    #[must_use]
    pub fn set_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Get the keep alive interval
    #[must_use]
    pub fn keep_alive(&self) -> Duration {
        self.keep_alive
    }

    /// Set the keep alive interval
    #[must_use]
    pub fn set_keep_alive(mut self, interval: Duration) -> Self {
        self.keep_alive = interval;
        self
    }

    /// Get the duration after which orphaned tasks are re-enqueued
    #[must_use]
    pub fn reenqueue_orphaned_after(&self) -> Duration {
        self.reenqueue_orphaned_after
    }

    /// Set the duration after which orphaned tasks are re-enqueued
    #[must_use]
    pub fn set_reenqueue_orphaned_after(mut self, duration: Duration) -> Self {
        self.reenqueue_orphaned_after = duration;
        self
    }
}
