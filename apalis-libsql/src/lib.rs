#![doc = include_str!("../README.md")]

use std::{fmt, marker::PhantomData, pin::Pin};

use apalis_core::{
    backend::{codec::Codec, Backend, BackendExt},
    error::BoxDynError,
    layers::Stack,
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
pub use apalis_sql::context::SqlContext;
use futures::{stream::BoxStream, FutureExt, Stream, StreamExt};
use libsql::Database;
use pin_project::pin_project;
use ulid::Ulid;

pub mod ack;
/// Configuration for the libSQL storage backend
pub mod config;
/// Fetcher implementation for polling tasks
pub mod fetcher;
/// Row mapping from database rows to task structs
pub mod row;
/// Sink implementation for pushing tasks
pub mod sink;

pub use ack::{LibsqlAck, LockTaskLayer, LockTaskService};
pub use config::Config;
pub use fetcher::LibsqlPollFetcher;
pub use sink::LibsqlSink;

/// Type alias for a task stored in libsql backend
pub type LibsqlTask<Args> = Task<Args, SqlContext, Ulid>;

/// CompactType is the type used for compact serialization in libsql backend
pub type CompactType = Vec<u8>;

/// Error type for libSQL storage operations
#[derive(Debug, thiserror::Error)]
pub enum LibsqlError {
    /// Database error from libsql
    #[error("Database error: {0}")]
    Database(#[from] libsql::Error),
    /// Other errors
    #[error("Other error: {0}")]
    Other(String),
}

/// SQL query to register a worker
const REGISTER_WORKER_SQL: &str = r#"
INSERT OR REPLACE INTO Workers (id, worker_type, storage_name, layers, last_seen)
VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))
"#;

/// SQL query to update worker heartbeat
const KEEP_ALIVE_SQL: &str = r#"
UPDATE Workers SET last_seen = strftime('%s', 'now') WHERE id = ?1
"#;

/// SQL query to re-enqueue orphaned tasks
const REENQUEUE_ORPHANED_SQL: &str = r#"
UPDATE Jobs
SET status = 'Pending', lock_by = NULL, lock_at = NULL
WHERE status = 'Running' AND lock_by IN (
    SELECT id FROM Workers WHERE last_seen < strftime('%s', 'now') - ?1
) AND job_type = ?2
"#;

/// LibsqlStorage is a storage backend for apalis using libsql as the database.
#[pin_project]
pub struct LibsqlStorage<T, C> {
    db: &'static Database,
    config: Config,
    job_type: PhantomData<T>,
    codec: PhantomData<C>,
    #[pin]
    sink: LibsqlSink<T, C>,
}

impl<T, C> fmt::Debug for LibsqlStorage<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LibsqlStorage")
            .field("db", &"Database")
            .field("config", &self.config)
            .field("job_type", &std::any::type_name::<T>())
            .field("codec", &std::any::type_name::<C>())
            .finish()
    }
}

impl<T, C> Clone for LibsqlStorage<T, C> {
    fn clone(&self) -> Self {
        Self {
            db: self.db,
            config: self.config.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            sink: self.sink.clone(),
        }
    }
}

impl<T> LibsqlStorage<T, ()> {
    /// Create a new LibsqlStorage with default JSON codec
    #[must_use]
    pub fn new(
        db: &'static Database,
    ) -> LibsqlStorage<T, apalis_core::backend::codec::json::JsonCodec<CompactType>> {
        let config = Config::new(std::any::type_name::<T>());
        LibsqlStorage {
            db,
            config: config.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            sink: LibsqlSink::new(db, &config),
        }
    }

    /// Create a new LibsqlStorage with custom config
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_config(
        db: &'static Database,
        config: Config,
    ) -> LibsqlStorage<T, apalis_core::backend::codec::json::JsonCodec<CompactType>> {
        LibsqlStorage {
            db,
            config: config.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            sink: LibsqlSink::new(db, &config),
        }
    }
}

impl<T, C> LibsqlStorage<T, C> {
    /// Get the database reference
    #[must_use]
    pub fn db(&self) -> &'static Database {
        self.db
    }

    /// Get the config
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Setup the database schema by running migrations
    pub async fn setup(&self) -> Result<(), LibsqlError> {
        let conn = self.db.connect()?;

        // Read and execute the migration SQL
        let migration_sql = include_str!("../migrations/001_initial.sql");

        // Execute the migration as a batch
        conn.execute_batch(migration_sql)
            .await
            .map_err(LibsqlError::Database)?;

        Ok(())
    }

    /// Change the codec used for serialization/deserialization
    #[must_use]
    pub fn with_codec<D>(self) -> LibsqlStorage<T, D> {
        LibsqlStorage {
            db: self.db,
            config: self.config.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            sink: LibsqlSink::new(self.db, &self.config),
        }
    }
}

/// Register a worker in the database
async fn register_worker(
    db: &'static Database,
    worker_id: &str,
    worker_type: &str,
) -> Result<(), LibsqlError> {
    let conn = db.connect()?;
    conn.execute(REGISTER_WORKER_SQL, libsql::params![worker_id, worker_type])
        .await
        .map_err(LibsqlError::Database)?;
    Ok(())
}

/// Update worker heartbeat
async fn keep_alive(db: &'static Database, worker_id: &str) -> Result<(), LibsqlError> {
    let conn = db.connect()?;
    conn.execute(KEEP_ALIVE_SQL, libsql::params![worker_id])
        .await
        .map_err(LibsqlError::Database)?;
    Ok(())
}

/// Re-enqueue orphaned tasks from dead workers
pub async fn reenqueue_orphaned(
    db: &'static Database,
    config: &Config,
) -> Result<u64, LibsqlError> {
    let conn = db.connect()?;
    let dead_for = config.reenqueue_orphaned_after().as_secs() as i64;
    let queue = config.queue().to_string();

    let rows = conn
        .execute(REENQUEUE_ORPHANED_SQL, libsql::params![dead_for, queue])
        .await
        .map_err(LibsqlError::Database)?;

    if rows > 0 {
        tracing::info!("Re-enqueued {} orphaned tasks", rows);
    }

    Ok(rows)
}

/// Initial heartbeat: register worker and re-enqueue orphaned tasks
#[allow(clippy::needless_pass_by_value)]
async fn initial_heartbeat(
    db: &'static Database,
    config: Config,
    worker: WorkerContext,
) -> Result<(), LibsqlError> {
    let worker_id = worker.name().to_string();
    let worker_type = config.queue().to_string();

    // Re-enqueue orphaned tasks first
    reenqueue_orphaned(db, &config).await?;

    // Register worker
    register_worker(db, &worker_id, &worker_type).await?;

    Ok(())
}

/// Create a heartbeat stream that periodically updates worker status
#[allow(clippy::needless_pass_by_value)]
fn heartbeat_stream(
    db: &'static Database,
    config: Config,
    worker: WorkerContext,
) -> impl Stream<Item = Result<(), LibsqlError>> + Send + 'static {
    let worker_id = worker.name().to_string();
    let keep_alive_interval = config.keep_alive();

    futures::stream::unfold((), move |_| {
        let db = db;
        let worker_id = worker_id.clone();
        let interval = keep_alive_interval;
        let config = config.clone();

        async move {
            // Wait for the keep-alive interval
            tokio::time::sleep(interval).await;

            // Update heartbeat
            if let Err(e) = keep_alive(db, &worker_id).await {
                return Some((Err(e), ()));
            }

            // Re-enqueue orphaned tasks periodically
            if let Err(e) = reenqueue_orphaned(db, &config).await {
                return Some((Err(e), ()));
            }

            Some((Ok(()), ()))
        }
    })
}

impl<Args, Decode> Backend for LibsqlStorage<Args, Decode>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SqlContext;
    type Error = LibsqlError;
    type Stream = apalis_core::backend::TaskStream<LibsqlTask<Args>, LibsqlError>;
    type Beat = BoxStream<'static, Result<(), LibsqlError>>;
    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<LibsqlAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let db = self.db;
        let config = self.config.clone();
        let worker = worker.clone();

        // Start heartbeat stream
        heartbeat_stream(db, config, worker).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.db);
        let ack = AcknowledgeLayer::new(LibsqlAck::new(self.db));
        Stack::new(lock, ack)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let db = self.db;
        let config = self.config.clone();
        let worker = worker.clone();

        // Initial registration - create a stream that owns the data
        let register = futures::stream::once(
            initial_heartbeat(db, config.clone(), worker.clone()).map(|res| res.map(|_| None)),
        );

        // Polling stream - we need to use a concrete type for the fetcher
        // Since we're in the Backend impl, we can use the Decode type parameter
        let fetcher = LibsqlPollFetcher::<Decode>::new(db, &config, &worker);

        // Chain registration with polling, and decode tasks
        register
            .chain(fetcher)
            .map(move |result| match result {
                Ok(Some(task)) => {
                    let decoded = task
                        .try_map(|t| Decode::decode(&t))
                        .map_err(|e| LibsqlError::Other(e.to_string()))?;
                    Ok(Some(decoded))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode> BackendExt for LibsqlStorage<Args, Decode>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = apalis_core::backend::TaskStream<LibsqlTask<CompactType>, LibsqlError>;

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        let db = self.db;
        let config = self.config.clone();
        let worker = worker.clone();

        // Initial registration
        let register = futures::stream::once(
            initial_heartbeat(db, config.clone(), worker.clone()).map(|res| res.map(|_| None)),
        );

        // Polling stream (compact tasks) - use the Decode type parameter
        let fetcher = LibsqlPollFetcher::<Decode>::new(db, &config, &worker);

        register.chain(fetcher).boxed()
    }
}

impl<Args, Decode> LibsqlStorage<Args, Decode>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + 'static + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    /// Poll for tasks using the default polling strategy
    pub fn poll_default(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<LibsqlTask<CompactType>>, LibsqlError>> + Send + 'static
    {
        let db = self.db;
        let config = self.config.clone();
        let worker = worker.clone();

        // Initial registration
        let register = futures::stream::once(
            initial_heartbeat(db, config.clone(), worker.clone()).map(|res| res.map(|_| None)),
        );

        // Polling stream (compact tasks) - use () as the codec since we want compact tasks
        let fetcher = LibsqlPollFetcher::<()>::new(db, &config, &worker);

        register.chain(fetcher).boxed()
    }

    /// Acknowledge a task completion
    pub async fn ack<Res>(
        &mut self,
        task_id: &Ulid,
        result: Result<Res, BoxDynError>,
    ) -> Result<(), LibsqlError>
    where
        Res: serde::Serialize + Send,
    {
        use apalis_core::task::status::Status;

        let task_id_str = task_id.to_string();
        let response = serde_json::to_string(&result.as_ref().map_err(|e| e.to_string()))
            .map_err(|e| LibsqlError::Other(e.to_string()))?;

        // First, get the current task information to find the lock_by, attempts, and max_attempts
        let conn = self.db.connect()?;
        let mut rows = conn
            .query(
                "SELECT lock_by, attempts, max_attempts FROM Jobs WHERE id = ?1",
                libsql::params![task_id_str.clone()],
            )
            .await
            .map_err(LibsqlError::Database)?;

        let (lock_by, current_attempts, max_attempts) =
            match rows.next().await.map_err(LibsqlError::Database)? {
                Some(row) => {
                    let lock_by: Option<String> = row.get(0).map_err(LibsqlError::Database)?;
                    let attempts: i64 = row.get(1).map_err(LibsqlError::Database)?;
                    let max_attempts: i64 = row.get(2).map_err(LibsqlError::Database)?;
                    (lock_by, attempts as i32, max_attempts as i32)
                }
                None => return Err(LibsqlError::Other("Task not found".into())),
            };

        let status = match &result {
            Ok(_) => Status::Done,
            Err(_) => {
                // Implement proper retry logic based on attempt count
                // If we've reached max_attempts, mark as Killed, otherwise Failed for retry
                if current_attempts + 1 >= max_attempts {
                    Status::Killed
                } else {
                    Status::Failed
                }
            }
        };
        let status_str = status.to_string();

        let worker_id =
            lock_by.ok_or_else(|| LibsqlError::Other("Task is not locked by any worker".into()))?;
        let new_attempts = match &result {
            Ok(_) => current_attempts,      // Don't increment on success
            Err(_) => current_attempts + 1, // Only increment on failure
        };

        let rows_affected = conn
            .execute(
                "UPDATE Jobs SET status = ?1, attempts = ?2, last_error = ?3, done_at = strftime('%s', 'now') WHERE id = ?4 AND lock_by = ?5",
                libsql::params![status_str, new_attempts, response, task_id_str, worker_id],
            )
            .await
            .map_err(LibsqlError::Database)?;

        if rows_affected == 0 {
            return Err(LibsqlError::Other("Task not found or already acked".into()));
        }

        Ok(())
    }
}

/// Enable WAL mode for better concurrency
///
/// This should be called after setup() to enable Write-Ahead Logging which
/// improves performance for concurrent database access.
pub async fn enable_wal_mode(db: &'static Database) -> Result<(), LibsqlError> {
    let conn = db.connect()?;
    conn.query("PRAGMA journal_mode=WAL", libsql::params![])
        .await
        .map_err(LibsqlError::Database)?;
    Ok(())
}

/// Implementation of Sink for LibsqlStorage to push tasks
impl<Args, Codec> futures::Sink<LibsqlTask<CompactType>> for LibsqlStorage<Args, Codec>
where
    Args: Send + Sync + 'static,
{
    type Error = LibsqlError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().sink.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: LibsqlTask<CompactType>) -> Result<(), Self::Error> {
        self.project().sink.start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().sink.poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().sink.poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_basic_connectivity() -> Result<(), Box<dyn std::error::Error>> {
        // Setup file-based database for testing (in-memory might have issues)
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test.db");

        let db = libsql::Builder::new_local(db_path.to_str().unwrap())
            .build()
            .await?;
        let db_static: &'static Database = Box::leak(Box::new(db));

        // Create storage
        let storage = LibsqlStorage::<(), ()>::new(db_static);

        // Verify we can access the database
        let conn = db_static.connect()?;
        let mut rows = conn.query("SELECT 1", libsql::params![]).await?;
        let row = rows.next().await?.unwrap();
        let result: i32 = row.get(0)?;
        assert_eq!(result, 1);

        // Setup schema
        storage.setup().await?;

        // Enable WAL mode for better concurrency
        enable_wal_mode(db_static).await?;

        // Verify tables exist using the same connection
        let mut rows = conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='Jobs'",
                libsql::params![],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            let name: String = row.get(0)?;
            assert_eq!(name, "Jobs");
        } else {
            panic!("Jobs table should exist after setup");
        }

        // Clean up
        drop(conn);

        Ok(())
    }
}
