//! Acknowledgment implementation for libSQL backend

use apalis_core::{
    error::{AbortError, BoxDynError},
    layers::{Layer, Service},
    task::{Parts, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use apalis_sql::context::SqlContext;
use futures::{FutureExt, future::BoxFuture};
use libsql::Database;
use serde::Serialize;
use ulid::Ulid;

use crate::{LibsqlError, LibsqlTask};

/// SQL query to acknowledge a task completion
const ACK_SQL: &str = r#"
UPDATE Jobs
SET status = ?4, attempts = ?2, last_error = ?3, done_at = strftime('%s', 'now')
WHERE id = ?1 AND lock_by = ?5
"#;

/// SQL query to lock a task before processing  
const LOCK_SQL: &str = r#"
UPDATE Jobs
SET status = 'Running', lock_by = ?2, lock_at = strftime('%s', 'now')
WHERE id = ?1 AND (lock_by IS NULL OR lock_by = ?2)
"#;

/// Acknowledgment handler for libSQL backend
#[derive(Clone, Debug)]
pub struct LibsqlAck {
    db: &'static Database,
}

impl LibsqlAck {
    /// Create a new LibsqlAck
    #[must_use]
    pub fn new(db: &'static Database) -> Self {
        Self { db }
    }
}

impl<Res: Serialize + 'static> Acknowledge<Res, SqlContext, Ulid> for LibsqlAck {
    type Error = LibsqlError;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<SqlContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id;
        let worker_id = parts.ctx.lock_by().clone();
        let db = self.db;

        // Serialize response for storage
        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()));

        let status = calculate_status(parts, res);
        parts.status.store(status.clone());
        let attempt_result = parts.attempt.current().try_into();
        let status_str = status.to_string();

        async move {
            let task_id = task_id
                .ok_or_else(|| LibsqlError::Other("Missing task_id for ack".into()))?
                .to_string();
            let worker_id = worker_id
                .ok_or_else(|| LibsqlError::Other("Missing worker_id (lock_by)".into()))?;
            let res_str = response.map_err(|e| LibsqlError::Other(e.to_string()))?;

            let attempt: i32 = attempt_result
                .map_err(|e| LibsqlError::Other(format!("Attempt count overflow: {}", e)))?;

            let conn = db.connect()?;
            let rows_affected = conn
                .execute(
                    ACK_SQL,
                    libsql::params![task_id, attempt, res_str, status_str, worker_id],
                )
                .await
                .map_err(LibsqlError::Database)?;

            if rows_affected == 0 {
                return Err(LibsqlError::Other("Task not found or already acked".into()));
            }

            Ok(())
        }
        .boxed()
    }
}

/// Calculate the status based on the result and attempt count
pub fn calculate_status<Res>(
    parts: &Parts<SqlContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match res {
        Ok(_) => Status::Done,
        Err(e) => {
            // Check if max attempts exceeded or explicitly aborted
            #[allow(clippy::if_same_then_else)]
            if parts.ctx.max_attempts() as usize <= parts.attempt.current() {
                Status::Killed
            // Check if explicitly aborted
            } else if e.downcast_ref::<AbortError>().is_some() {
                Status::Killed
            } else {
                Status::Failed
            }
        }
    }
}

/// Lock a task for processing
pub async fn lock_task(
    db: &'static Database,
    task_id: &Ulid,
    worker_id: &str,
) -> Result<(), LibsqlError> {
    let conn = db.connect()?;
    let task_id_str = task_id.to_string();

    let rows_affected = conn
        .execute(LOCK_SQL, libsql::params![task_id_str, worker_id])
        .await
        .map_err(LibsqlError::Database)?;

    if rows_affected == 0 {
        return Err(LibsqlError::Other(
            "Task not found or already locked".into(),
        ));
    }

    Ok(())
}

/// Layer for locking tasks before processing
#[derive(Clone, Debug)]
pub struct LockTaskLayer {
    db: &'static Database,
}

impl LockTaskLayer {
    /// Create a new LockTaskLayer
    #[must_use]
    pub fn new(db: &'static Database) -> Self {
        Self { db }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService { inner, db: self.db }
    }
}

/// Service that locks tasks before passing them to the inner service
#[derive(Clone, Debug)]
pub struct LockTaskService<S> {
    inner: S,
    db: &'static Database,
}

impl<S, Args> Service<LibsqlTask<Args>> for LockTaskService<S>
where
    S: Service<LibsqlTask<Args>> + Send + 'static + Clone,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: LibsqlTask<Args>) -> Self::Future {
        let db = self.db;
        let worker_id = req
            .parts
            .data
            .get::<WorkerContext>()
            .map(|w| w.name().to_owned())
            .ok_or_else(|| LibsqlError::Other("Missing WorkerContext for lock".into()));

        let worker_id = match worker_id {
            Ok(id) => id,
            Err(e) => return async move { Err(e.into()) }.boxed(),
        };

        let task_id = match &req.parts.task_id {
            Some(id) => *id.inner(),
            None => {
                return async { Err(LibsqlError::Other("Missing task_id for lock".into()).into()) }
                    .boxed();
            }
        };

        // Update context with lock_by
        req.parts.ctx = req.parts.ctx.with_lock_by(Some(worker_id.clone()));

        let mut inner = self.inner.clone();

        async move {
            lock_task(db, &task_id, &worker_id).await?;
            inner.call(req).await.map_err(Into::into)
        }
        .boxed()
    }
}
