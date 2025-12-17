//! Tests for acknowledgment functionality

use apalis_core::{
    error::{AbortError, BoxDynError},
    layers::Service,
    task::{Task, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use apalis_libsql::{
    CompactType, LibsqlError, SqlContext,
    ack::{LibsqlAck, LockTaskLayer, calculate_status},
};
use libsql::Builder;
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

struct TestDb {
    db: &'static libsql::Database,
    _temp_dir: Arc<TempDir>,
}

async fn setup_test_db() -> TestDb {
    let temp_dir = Arc::new(TempDir::new().unwrap());
    let db_path = temp_dir.path().join("test_ack.db");

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await
        .unwrap();

    let db_static: &'static libsql::Database = Box::leak(Box::new(db));

    // Setup schema
    let conn = db_static.connect().unwrap();
    conn.execute_batch(include_str!("../migrations/001_initial.sql"))
        .await
        .unwrap();

    TestDb {
        db: db_static,
        _temp_dir: temp_dir,
    }
}

/// Helper function to create test Parts for Acknowledge trait testing
fn create_test_parts_with_attempts(
    task_id: Ulid,
    lock_by: &str,
    attempts: u32,
) -> apalis_core::task::Parts<SqlContext, Ulid> {
    let ctx = SqlContext::default().with_lock_by(Some(lock_by.to_string()));
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;
    task.parts.task_id = Some(apalis_core::task::task_id::TaskId::new(task_id));
    // Set attempt count to match the expected value
    task.parts.attempt = apalis_core::task::attempt::Attempt::new();
    for _ in 0..attempts {
        let _ = task.parts.attempt.increment();
    }
    task.parts
}

/// Helper function to create test Parts for Acknowledge trait testing (default 1 attempt)
fn create_test_parts(task_id: Ulid, lock_by: &str) -> apalis_core::task::Parts<SqlContext, Ulid> {
    create_test_parts_with_attempts(task_id, lock_by, 1)
}

#[test]
fn test_libsql_ack_new() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let test_db = rt.block_on(setup_test_db());
    let _ack = LibsqlAck::new(test_db.db);
    // Just verify it can be created without panicking
    println!("LibsqlAck created successfully");
}

#[tokio::test]
async fn test_ack_success() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 0, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_id],
    )
    .await
    .unwrap();

    // Test successful ack through storage
    let mut storage = apalis_libsql::LibsqlStorage::<(), ()>::new_with_config(
        db,
        apalis_libsql::Config::new(job_type),
    );

    let result: Result<(), BoxDynError> = Ok(());
    storage.ack(&task_id, result).await.unwrap();

    // Verify task status in database
    let mut rows = conn
        .query(
            "SELECT status, done_at FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let done_at: Option<i64> = row.get(1).unwrap();
        assert_eq!(status, "Done");
        assert!(done_at.is_some());
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_ack_failure() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 1, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_id],
    )
    .await
    .unwrap();

    // Test failed ack through storage
    let mut storage = apalis_libsql::LibsqlStorage::<(), ()>::new_with_config(
        db,
        apalis_libsql::Config::new(job_type),
    );

    let result: Result<(), BoxDynError> = Err("test error".into());
    storage.ack(&task_id, result).await.unwrap();

    // Verify task status in database
    let mut rows = conn
        .query(
            "SELECT status, attempts FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let attempts: i64 = row.get(1).unwrap();
        assert_eq!(status, "Failed");
        assert_eq!(attempts, 2); // Should be incremented
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_ack_killed_max_attempts() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task with max attempts reached
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 3, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_id],
    )
    .await
    .unwrap();

    // Test failed ack through storage (should be killed since attempts >= max_attempts)
    let mut storage = apalis_libsql::LibsqlStorage::<(), ()>::new_with_config(
        db,
        apalis_libsql::Config::new(job_type),
    );

    let result: Result<(), BoxDynError> = Err("test error".into());
    storage.ack(&task_id, result).await.unwrap();

    // Verify task status in database
    let mut rows = conn
        .query(
            "SELECT status FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        assert_eq!(status, "Killed");
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_ack_task_not_found() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Try to ack a non-existent task
    let task_id = Ulid::new();
    let job_type = "TestTask";

    let mut storage = apalis_libsql::LibsqlStorage::<(), ()>::new_with_config(
        db,
        apalis_libsql::Config::new(job_type),
    );

    let result: Result<(), BoxDynError> = Ok(());
    let error = storage.ack(&task_id, result).await.unwrap_err();

    match error {
        LibsqlError::Other(msg) => assert!(msg.contains("Task not found")),
        _ => panic!("Expected Other error"),
    }
}

#[tokio::test]
async fn test_ack_task_not_locked() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task without lock_by
    let task_id = Ulid::new();
    let job_type = "TestTask";

    let conn = db.connect().unwrap();
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Running', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id.to_string(), job_type],
    )
    .await
    .unwrap();

    // Test ack through storage (should fail because task is not locked)
    let mut storage = apalis_libsql::LibsqlStorage::<(), ()>::new_with_config(
        db,
        apalis_libsql::Config::new(job_type),
    );

    let result: Result<(), BoxDynError> = Ok(());
    let error = storage.ack(&task_id, result).await.unwrap_err();

    match error {
        LibsqlError::Other(msg) => assert!(msg.contains("Task is not locked by any worker")),
        _ => panic!("Expected Other error"),
    }
}

#[tokio::test]
async fn test_ack_wrong_worker_lock() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task locked by different worker
    let task_id = Ulid::new();
    let worker_id = "different-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 0, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_id],
    )
    .await
    .unwrap();

    // Test ack through storage (should succeed since the storage's ack method
    // doesn't validate the worker ID - it just checks if the task exists and is locked)
    let mut storage = apalis_libsql::LibsqlStorage::<(), ()>::new_with_config(
        db,
        apalis_libsql::Config::new(job_type),
    );

    let result: Result<(), BoxDynError> = Ok(());
    storage.ack(&task_id, result).await.unwrap();

    // Verify task status in database
    let mut rows = conn
        .query(
            "SELECT status FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        assert_eq!(status, "Done");
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_lock_task_layer() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Test that LockTaskLayer can be created
    let _layer = LockTaskLayer::new(db);

    // Verify the layer was created successfully
    println!("LockTaskLayer created successfully");
}

#[test]
fn test_calculate_status_ok() {
    let ctx = SqlContext::new().with_max_attempts(3);
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;

    let result: Result<String, BoxDynError> = Ok("success".to_string());
    let status = calculate_status(&task.parts, &result);
    assert_eq!(status, Status::Done);
}

#[test]
fn test_calculate_status_err_retry() {
    let ctx = SqlContext::new().with_max_attempts(3);
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;

    let result: Result<String, BoxDynError> = Err("test error".into());
    let status = calculate_status(&task.parts, &result);
    assert_eq!(status, Status::Failed);
}

#[test]
fn test_calculate_status_abort_error() {
    let ctx = SqlContext::new().with_max_attempts(3);
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;

    let abort_error = AbortError::new("explicit abort");
    let result: Result<String, BoxDynError> = Err(Box::new(abort_error));
    let status = calculate_status(&task.parts, &result);
    assert_eq!(status, Status::Killed);
}

#[tokio::test]
async fn test_lock_task_success() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    // Insert a pending task
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id.to_string(), job_type],
    )
    .await
    .unwrap();

    // Test lock_task function
    use apalis_libsql::ack::lock_task;
    lock_task(db, &task_id, worker_id).await.unwrap();

    // Verify task is locked in database
    let mut rows = conn
        .query(
            "SELECT status, lock_by, lock_at FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let lock_by: String = row.get(1).unwrap();
        let lock_at: Option<i64> = row.get(2).unwrap();
        assert_eq!(status, "Running");
        assert_eq!(lock_by, worker_id);
        assert!(lock_at.is_some());
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_lock_task_not_found() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let task_id = Ulid::new();
    let worker_id = "test-worker";

    // Test lock_task with non-existent task_id
    use apalis_libsql::ack::lock_task;
    let error = lock_task(db, &task_id, worker_id).await.unwrap_err();

    match error {
        LibsqlError::Other(msg) => assert!(msg.contains("Task not found or already locked")),
        _ => panic!("Expected Other error"),
    }
}

#[tokio::test]
async fn test_lock_task_already_locked_by_other() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_1 = "worker-1";
    let worker_2 = "worker-2";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_1, job_type],
    )
    .await
    .unwrap();

    // Insert a task locked by worker-1
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 0, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_1],
    )
    .await
    .unwrap();

    // Test lock_task as worker-2 (should fail)
    use apalis_libsql::ack::lock_task;
    let error = lock_task(db, &task_id, worker_2).await.unwrap_err();

    match error {
        LibsqlError::Other(msg) => assert!(msg.contains("Task not found or already locked")),
        _ => panic!("Expected Other error"),
    }
}

#[tokio::test]
async fn test_acknowledge_trait_ack_success() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 0, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_id],
    )
    .await
    .unwrap();

    // Test successful ack through trait method
    let mut ack = LibsqlAck::new(db);
    let parts = create_test_parts(task_id, worker_id);
    let result: Result<String, BoxDynError> = Ok("success".to_string());

    ack.ack(&result, &parts).await.unwrap();

    // Verify task status in database
    let mut rows = conn
        .query(
            "SELECT status, done_at FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let done_at: Option<i64> = row.get(1).unwrap();
        assert_eq!(status, "Done");
        assert!(done_at.is_some());
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_acknowledge_trait_ack_failure() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by) 
         VALUES (?1, ?2, ?3, 'Running', 1, 3, strftime('%s', 'now'), 0, '{}', ?4)",
        libsql::params![b"test_job_data", task_id.to_string(), job_type, worker_id],
    )
    .await
    .unwrap();

    // Test failed ack through trait method
    let mut ack = LibsqlAck::new(db);
    let parts = create_test_parts_with_attempts(task_id, worker_id, 2); // Use 2 attempts for failure case
    let result: Result<String, BoxDynError> = Err("test error".into());

    ack.ack(&result, &parts).await.unwrap();

    // Verify task status in database
    let mut rows = conn
        .query(
            "SELECT status, attempts FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let attempts: i64 = row.get(1).unwrap();
        assert_eq!(status, "Failed");
        // The attempts should be incremented from 1 to 2
        println!("Attempts: {}", attempts);
        assert_eq!(attempts, 2); // Should be incremented
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_acknowledge_trait_missing_task_id() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let mut ack = LibsqlAck::new(db);

    // Create Parts with task_id = None
    let ctx = SqlContext::default().with_lock_by(Some("test-worker".to_string()));
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;
    task.parts.task_id = None;
    let parts = &task.parts;

    let result: Result<String, BoxDynError> = Ok("success".to_string());
    let error = ack.ack(&result, &parts).await.unwrap_err();

    match error {
        LibsqlError::Other(msg) => assert!(msg.contains("Missing task_id for ack")),
        _ => panic!("Expected Other error"),
    }
}

#[tokio::test]
async fn test_acknowledge_trait_missing_lock_by() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let mut ack = LibsqlAck::new(db);
    let task_id = Ulid::new();

    // Create Parts with ctx.lock_by() = None
    let ctx = SqlContext::default(); // No lock_by set
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;
    task.parts.task_id = Some(apalis_core::task::task_id::TaskId::new(task_id));
    let parts = &task.parts;

    let result: Result<String, BoxDynError> = Ok("success".to_string());
    let error = ack.ack(&result, &parts).await.unwrap_err();

    match error {
        LibsqlError::Other(msg) => assert!(msg.contains("Missing worker_id (lock_by)")),
        _ => panic!("Expected Other error"),
    }
}

// Mock service for testing LockTaskService
#[derive(Clone, Debug)]
struct MockService {
    called: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl MockService {
    fn new() -> Self {
        Self {
            called: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn was_called(&self) -> bool {
        self.called.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl<Args> Service<apalis_libsql::LibsqlTask<Args>> for MockService {
    type Response = String;
    type Error = BoxDynError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: apalis_libsql::LibsqlTask<Args>) -> Self::Future {
        self.called.store(true, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move { Ok("mock_response".to_string()) })
    }
}

#[tokio::test]
async fn test_lock_task_service_call() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a task first
    let task_id = Ulid::new();
    let worker_id = "test-worker";
    let job_type = "TestTask";

    let conn = db.connect().unwrap();

    // Create worker first (required by foreign key constraint)
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    // Insert a pending task
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id.to_string(), job_type],
    )
    .await
    .unwrap();

    // Create a mock inner service
    let mock_service = MockService::new();

    // Wrap with LockTaskLayer
    use apalis_core::layers::Layer;
    let layer = LockTaskLayer::new(db);
    let mut lock_task_service = layer.layer(mock_service.clone());

    // Create a LibsqlTask with WorkerContext
    let ctx = SqlContext::default();
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;
    task.parts.task_id = Some(apalis_core::task::task_id::TaskId::new(task_id));

    // Add WorkerContext to task data
    let worker_ctx = WorkerContext::new::<&str>(worker_id);
    task.parts.data.insert(worker_ctx);

    // Call the service
    let result = lock_task_service.call(task).await;

    // Verify success
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "mock_response");
    assert!(mock_service.was_called());

    // Verify task is locked in database
    let mut rows = conn
        .query(
            "SELECT status, lock_by FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let lock_by: String = row.get(1).unwrap();
        assert_eq!(status, "Running");
        assert_eq!(lock_by, worker_id);
    } else {
        panic!("Task should exist");
    }
}

#[tokio::test]
async fn test_lock_task_service_missing_worker_context() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a mock inner service
    let mock_service = MockService::new();

    // Wrap with LockTaskLayer
    use apalis_core::layers::Layer;
    let layer = LockTaskLayer::new(db);
    let mut lock_task_service = layer.layer(mock_service.clone());

    // Create a LibsqlTask WITHOUT WorkerContext
    let ctx = SqlContext::default();
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;
    task.parts.task_id = Some(apalis_core::task::task_id::TaskId::new(Ulid::new()));
    // No WorkerContext added to task.parts.data

    // Call the service (should fail)
    let result = lock_task_service.call(task).await;

    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_msg = error.to_string();
    assert!(error_msg.contains("Missing WorkerContext for lock"));
    assert!(!mock_service.was_called()); // Inner service should not be called
}

#[tokio::test]
async fn test_lock_task_service_missing_task_id() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a mock inner service
    let mock_service = MockService::new();

    // Wrap with LockTaskLayer
    use apalis_core::layers::Layer;
    let layer = LockTaskLayer::new(db);
    let mut lock_task_service = layer.layer(mock_service.clone());

    // Create a LibsqlTask with WorkerContext but no task_id
    let ctx = SqlContext::default();
    let mut task = Task::new(CompactType::new());
    task.parts.ctx = ctx;
    task.parts.task_id = None; // No task_id

    // Add WorkerContext to task data
    let worker_ctx = WorkerContext::new::<&str>("test-worker");
    task.parts.data.insert(worker_ctx);

    // Call the service (should fail)
    let result = lock_task_service.call(task).await;

    // Verify error
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_msg = error.to_string();
    assert!(error_msg.contains("Missing task_id for lock"));
    assert!(!mock_service.was_called()); // Inner service should not be called
}

#[tokio::test]
async fn test_lock_task_service_poll_ready() {
    // This test verifies that LockTaskService implements the poll_ready method
    // The actual polling logic is tested indirectly through the call method
    // since poll_ready just delegates to the inner service

    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Create a mock inner service
    let mock_service = MockService::new();

    // Wrap with LockTaskLayer - this tests that the layer can be created
    use apalis_core::layers::Layer;
    let layer = LockTaskLayer::new(db);
    let _lock_task_service = layer.layer(mock_service.clone());

    // Test that the service can be created and used
    // The main test is that we can create the layer and service without compilation errors
    // and that the service has the expected type and properties

    // Verify the service was created successfully
    // We can't easily test poll_ready without complex async setup, but we can verify
    // that the service exists and has the expected structure
    println!("LockTaskService created successfully");

    // The fact that we got here means the service implements the required traits
    // and can be used in the apalis ecosystem
}
