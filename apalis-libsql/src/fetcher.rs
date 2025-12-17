//! Fetcher implementation for polling tasks from libSQL database

use std::{
    collections::VecDeque,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use apalis_core::{task::Task, worker::context::WorkerContext};
use apalis_sql::{context::SqlContext, from_row::TaskRow};
use futures::{FutureExt, future::BoxFuture, stream::Stream};
use libsql::Database;
use pin_project::pin_project;
use ulid::Ulid;

use crate::{CompactType, LibsqlError, LibsqlTask, config::Config, row::LibsqlTaskRow};

/// SQL query to fetch the next batch of tasks (atomic lock via UPDATE ... RETURNING)
const FETCH_NEXT_SQL: &str = r#"
UPDATE Jobs
SET status = 'Queued', lock_by = ?1, lock_at = strftime('%s', 'now')
WHERE ROWID IN (
    SELECT ROWID FROM Jobs
    WHERE job_type = ?2
        AND ((status = 'Pending' AND lock_by IS NULL) 
             OR (status = 'Failed' AND attempts < max_attempts))
        AND (run_at IS NULL OR run_at <= strftime('%s', 'now'))
    ORDER BY priority DESC, run_at ASC, id ASC
    LIMIT ?3
)
RETURNING job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata
"#;

/// Fetch the next batch of tasks from the database
pub async fn fetch_next(
    db: &'static Database,
    config: &Config,
    worker: &WorkerContext,
) -> Result<Vec<Task<CompactType, SqlContext, Ulid>>, LibsqlError> {
    let conn = db.connect()?;
    let job_type = config.queue().to_string();
    let buffer_size = config.buffer_size() as i64;
    let worker_id = worker.name().to_string();

    let mut rows = conn
        .query(
            FETCH_NEXT_SQL,
            libsql::params![worker_id, job_type, buffer_size],
        )
        .await
        .map_err(LibsqlError::Database)?;

    let mut tasks = Vec::new();
    while let Some(row) = rows.next().await.map_err(LibsqlError::Database)? {
        let libsql_row = LibsqlTaskRow::from_row(&row)?;
        let task_row: TaskRow = libsql_row.try_into()?;
        let task = task_row
            .try_into_task_compact::<Ulid>()
            .map_err(|e| LibsqlError::Other(e.to_string()))?;
        tasks.push(task);
    }

    Ok(tasks)
}

/// State machine for the polling stream
enum StreamState {
    /// Ready to fetch
    Ready,
    /// Waiting for delay before next fetch
    Delay(Pin<Box<tokio::time::Sleep>>),
    /// Currently fetching from database
    Fetch(BoxFuture<'static, Result<Vec<LibsqlTask<CompactType>>, LibsqlError>>),
    /// Buffered tasks ready to yield
    Buffered(VecDeque<LibsqlTask<CompactType>>),
}

/// Polling-based fetcher for retrieving tasks from a libSQL backend
#[pin_project]
pub struct LibsqlPollFetcher<Decode> {
    db: &'static Database,
    config: Config,
    worker: WorkerContext,
    _marker: PhantomData<Decode>,
    state: StreamState,
}

impl<Decode> std::fmt::Debug for LibsqlPollFetcher<Decode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibsqlPollFetcher")
            .field("config", &self.config)
            .field("worker", &self.worker)
            .finish()
    }
}

impl<Decode> Clone for LibsqlPollFetcher<Decode> {
    fn clone(&self) -> Self {
        Self {
            db: self.db,
            config: self.config.clone(),
            worker: self.worker.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
        }
    }
}

impl<Decode> LibsqlPollFetcher<Decode> {
    /// Create a new LibsqlPollFetcher
    #[must_use]
    pub fn new(db: &'static Database, config: &Config, worker: &WorkerContext) -> Self {
        Self {
            db,
            config: config.clone(),
            worker: worker.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
        }
    }
}

impl<Decode: Send + 'static> Stream for LibsqlPollFetcher<Decode> {
    type Item = Result<Option<LibsqlTask<CompactType>>, LibsqlError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                StreamState::Ready => {
                    // Start a fetch operation
                    let db = this.db;
                    let config = this.config.clone();
                    let worker = this.worker.clone();
                    let fut = async move { fetch_next(db, &config, &worker).await };
                    this.state = StreamState::Fetch(fut.boxed());
                }

                StreamState::Delay(sleep) => match Pin::new(sleep).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.state = StreamState::Ready;
                    }
                },

                StreamState::Fetch(fut) => {
                    match fut.poll_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => match result {
                            Ok(tasks) => {
                                if tasks.is_empty() {
                                    // No tasks available, wait before polling again
                                    let delay = tokio::time::sleep(this.config.poll_interval());
                                    this.state = StreamState::Delay(Box::pin(delay));
                                } else {
                                    let buffer: VecDeque<_> = tasks.into_iter().collect();
                                    this.state = StreamState::Buffered(buffer);
                                }
                            }
                            Err(e) => {
                                // Log the error and transition to delay state for retry
                                // Stream continues running even after errors (they transition to Delay state)
                                log::error!("Error fetching tasks: {}", e);
                                let delay = tokio::time::sleep(this.config.poll_interval());
                                this.state = StreamState::Delay(Box::pin(delay));
                                return Poll::Ready(Some(Err(e)));
                            }
                        },
                    }
                }

                StreamState::Buffered(buffer) => {
                    if let Some(task) = buffer.pop_front() {
                        if buffer.is_empty() {
                            this.state = StreamState::Ready;
                        }
                        return Poll::Ready(Some(Ok(Some(task))));
                    } else {
                        // Buffer is empty, transition back to ready state
                        this.state = StreamState::Ready;
                    }
                }
            }
        }
    }
}

impl<Decode> LibsqlPollFetcher<Decode> {
    /// Take pending tasks from the fetcher's buffer
    pub fn take_pending(&mut self) -> VecDeque<LibsqlTask<CompactType>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}
