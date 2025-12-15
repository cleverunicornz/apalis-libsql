//! Sink implementation for pushing tasks to libSQL database

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    future::{BoxFuture, Shared},
    FutureExt, Sink,
};
use libsql::Database;
use ulid::Ulid;

use crate::{config::Config, CompactType, LibsqlError, LibsqlTask};

/// SQL query to insert a new task
const INSERT_SQL: &str = r#"
INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata)
VALUES (?1, ?2, ?3, 'Pending', 0, ?4, ?5, ?6, ?7)
"#;

type FlushFuture = BoxFuture<'static, Result<(), Arc<LibsqlError>>>;

/// Sink for pushing tasks to libSQL
///
/// This struct is Unpin because it doesn't contain any self-referential data
pub struct LibsqlSink<Args, Codec> {
    db: &'static Database,
    config: Config,
    buffer: Vec<LibsqlTask<CompactType>>,
    flush_future: Option<Shared<FlushFuture>>,
    _marker: std::marker::PhantomData<fn() -> (Args, Codec)>,
}

// Manual Debug implementation to avoid requiring Debug on Args and Codec
impl<Args, Codec> std::fmt::Debug for LibsqlSink<Args, Codec> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibsqlSink")
            .field("config", &self.config)
            .field("buffer_len", &self.buffer.len())
            .finish()
    }
}

// LibsqlSink is Unpin because:
// - &'static Database is Unpin
// - Config is Unpin (regular struct)
// - Vec is Unpin
// - Option<Shared<BoxFuture>> is Unpin
// - PhantomData with fn() -> X is Unpin
impl<Args, Codec> Unpin for LibsqlSink<Args, Codec> {}

impl<Args, Codec> Clone for LibsqlSink<Args, Codec> {
    /// Creates a new `LibsqlSink` with the same configuration but an empty buffer.
    ///
    /// The cloned sink will have:
    /// - The same database reference and configuration
    /// - An empty buffer (any buffered tasks in the original are not copied)
    /// - No active flush future
    fn clone(&self) -> Self {
        Self {
            db: self.db,
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Args, Codec> LibsqlSink<Args, Codec> {
    /// Create a new LibsqlSink
    #[must_use]
    pub fn new(db: &'static Database, config: &Config) -> Self {
        Self {
            db,
            config: config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Push a batch of tasks to the database
pub async fn push_tasks(
    db: &'static Database,
    cfg: &Config,
    buffer: Vec<LibsqlTask<CompactType>>,
) -> Result<(), Arc<LibsqlError>> {
    tracing::debug!("push_tasks called with {} tasks", buffer.len());
    let conn = db
        .connect()
        .map_err(|e| Arc::new(LibsqlError::Database(e)))?;

    // Use transaction for batch insert
    tracing::debug!("Starting transaction");
    conn.execute("BEGIN", libsql::params![])
        .await
        .map_err(|e| Arc::new(LibsqlError::Database(e)))?;

    for (i, task) in buffer.iter().enumerate() {
        tracing::debug!("Processing task {}", i);
        let id = task
            .parts
            .task_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| Ulid::new().to_string());
        let run_at = task.parts.run_at as i64;
        let max_attempts = task.parts.ctx.max_attempts();
        let priority = task.parts.ctx.priority();
        let args = task.args.clone();
        let job_type = cfg.queue().to_string();
        let meta =
            serde_json::to_string(&task.parts.ctx.meta()).unwrap_or_else(|_| "{}".to_string());

        tracing::debug!("Executing INSERT with id: {}, job_type: {}", id, job_type);
        match conn
            .execute(
                INSERT_SQL,
                libsql::params![args, id, job_type, max_attempts, run_at, priority, meta],
            )
            .await
        {
            Ok(rows_affected) => {
                tracing::debug!(
                    "INSERT executed successfully, rows affected: {}",
                    rows_affected
                );
                if rows_affected != 1 {
                    tracing::warn!("INSERT affected {} rows instead of 1", rows_affected);
                }
            }
            Err(e) => {
                tracing::error!("INSERT failed: {:?}", e);
                // Try to rollback
                if let Err(rollback_err) = conn.execute("ROLLBACK", libsql::params![]).await {
                    tracing::error!("Failed to rollback transaction: {:?}", rollback_err);
                }
                return Err(Arc::new(LibsqlError::Database(e)));
            }
        }
    }

    tracing::debug!("Committing transaction");
    conn.execute("COMMIT", libsql::params![])
        .await
        .map_err(|e| Arc::new(LibsqlError::Database(e)))?;

    tracing::debug!("push_tasks completed successfully");
    Ok(())
}

impl<Args, Codec> Sink<LibsqlTask<CompactType>> for LibsqlSink<Args, Codec>
where
    Args: Send + Sync + 'static,
{
    type Error = LibsqlError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: LibsqlTask<CompactType>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // If there's no existing future and buffer is empty, we're done
        if this.flush_future.is_none() && this.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.flush_future.is_none() && !this.buffer.is_empty() {
            let db = this.db;
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.buffer);
            let sink_fut = async move { push_tasks(db, &config, buffer).await };
            this.flush_future = Some((Box::pin(sink_fut) as FlushFuture).shared());
        }

        // Poll the existing future
        if let Some(mut fut) = this.flush_future.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => {
                    Poll::Ready(Err(Arc::try_unwrap(e)
                        .unwrap_or_else(|arc| LibsqlError::Other(arc.to_string()))))
                }
                Poll::Pending => {
                    this.flush_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
