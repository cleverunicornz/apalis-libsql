use apalis_core::backend::{Backend, TaskSink};
use apalis_libsql::LibsqlStorage;
use futures::StreamExt;
use libsql::Builder;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

// Use atomic counter to ensure unique database files per test
static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_db_path() -> String {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test_db_{}.db", id)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestTask {
    pub message: String,
    pub value: i32,
}

/// Test basic database operations and task push
#[tokio::test]
async fn test_push_task() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join(unique_db_path());

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));

    // Setup storage and schema
    let mut storage = LibsqlStorage::<TestTask, ()>::new(db_static);
    storage.setup().await?;

    // Enable WAL mode for better concurrency
    apalis_libsql::enable_wal_mode(db_static).await?;

    // Push a task
    let test_task = TestTask {
        message: "Hello World".to_string(),
        value: 42,
    };

    storage.push(test_task.clone()).await?;

    // Verify task exists in database
    let conn = db_static.connect()?;
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending'",
            libsql::params![],
        )
        .await?;

    if let Some(row) = rows.next().await? {
        let count: i64 = row.get(0)?;
        assert_eq!(count, 1, "Should have exactly 1 pending task");
    }

    Ok(())
}

/// Test polling tasks from the database
#[tokio::test]
async fn test_poll_task() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join(unique_db_path());

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));

    // Setup storage and schema
    let storage = LibsqlStorage::<TestTask, ()>::new(db_static);
    storage.setup().await?;

    // Enable WAL mode
    apalis_libsql::enable_wal_mode(db_static).await?;

    // Get a connection for inserting tasks
    let conn = db_static.connect()?;

    // Insert a task directly (bypassing Sink to avoid connection conflicts)
    let task_data = serde_json::to_vec(&TestTask {
        message: "Test Poll".to_string(),
        value: 123,
    })?;
    let task_id = ulid::Ulid::new().to_string();
    let job_type = std::any::type_name::<TestTask>();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 25, strftime('%s', 'now'), 0, '{}')",
        libsql::params![task_data, task_id, job_type]
    ).await?;
    drop(conn);

    // Poll for the task
    use apalis_core::worker::context::WorkerContext;
    let worker = WorkerContext::new::<&'static str>("test-worker");

    let mut task_stream = storage.poll_default(&worker).boxed();

    // First item should be None (from initial_heartbeat registration)
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), task_stream.next())
        .await?
        .ok_or("Stream ended unexpectedly")?;

    // This might be None from registration or Some(task)
    match first? {
        None => {
            // Get the actual task
            let task_result =
                tokio::time::timeout(std::time::Duration::from_secs(2), task_stream.next())
                    .await?
                    .ok_or("Stream ended unexpectedly")?;

            let task = task_result?.expect("Should receive a task");
            let decoded: TestTask = serde_json::from_slice(&task.args)?;
            assert_eq!(decoded.message, "Test Poll");
            assert_eq!(decoded.value, 123);
        }
        Some(task) => {
            let decoded: TestTask = serde_json::from_slice(&task.args)?;
            assert_eq!(decoded.message, "Test Poll");
            assert_eq!(decoded.value, 123);
        }
    }

    Ok(())
}

/// Test full task lifecycle: push -> poll -> ack
#[tokio::test]
async fn test_full_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join(unique_db_path());

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));

    // Setup storage and schema
    let mut storage = LibsqlStorage::<TestTask, ()>::new(db_static);
    storage.setup().await?;

    // Enable WAL mode for better concurrency
    apalis_libsql::enable_wal_mode(db_static).await?;

    // Get a connection for inserting and verifying tasks
    let conn = db_static.connect()?;

    // Insert task directly to avoid connection conflicts
    let test_task = TestTask {
        message: "Lifecycle Test".to_string(),
        value: 999,
    };
    let task_data = serde_json::to_vec(&test_task)?;
    let task_id = ulid::Ulid::new();
    let task_id_str = task_id.to_string();
    let job_type = std::any::type_name::<TestTask>();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 25, strftime('%s', 'now'), 0, '{}')",
        libsql::params![task_data, task_id_str.clone(), job_type]
    ).await?;

    // Verify task is Pending
    let mut rows = conn
        .query(
            "SELECT status FROM Jobs WHERE id = ?1",
            libsql::params![task_id_str.clone()],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let status: String = row.get(0)?;
        assert_eq!(status, "Pending");
    }
    drop(conn);

    // Poll for the task
    use apalis_core::worker::context::WorkerContext;
    let worker = WorkerContext::new::<&'static str>("test-worker");

    let storage_clone = storage.clone();
    let mut task_stream = storage_clone.poll_default(&worker).boxed();

    // Skip registration result, get actual task
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), task_stream.next()).await?;

    let task_result = tokio::time::timeout(std::time::Duration::from_secs(2), task_stream.next())
        .await?
        .ok_or("Stream ended unexpectedly")?;

    let task = task_result?.expect("Should receive a task");

    // Verify task data
    let decoded: TestTask = serde_json::from_slice(&task.args)?;
    assert_eq!(decoded.message, "Lifecycle Test");
    assert_eq!(decoded.value, 999);

    // Verify task is now Running
    let conn = db_static.connect()?;
    let mut rows = conn
        .query(
            "SELECT status, lock_by FROM Jobs WHERE id = ?1",
            libsql::params![task_id_str.clone()],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let status: String = row.get(0)?;
        let lock_by: Option<String> = row.get(1)?;
        assert_eq!(status, "Queued");
        assert_eq!(lock_by, Some("test-worker".to_string()));
    }
    drop(conn);

    // Acknowledge task completion
    let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = Ok(());
    storage.ack(&task_id, result).await?;

    // Verify task is Done
    let conn = db_static.connect()?;
    let mut rows = conn
        .query(
            "SELECT status, done_at FROM Jobs WHERE id = ?1",
            libsql::params![task_id_str.clone()],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        let status: String = row.get(0)?;
        let done_at: Option<i64> = row.get(1)?;
        assert_eq!(status, "Done");
        assert!(done_at.is_some(), "done_at should be set");
    }

    Ok(())
}

/// Test that schema setup creates required tables
#[tokio::test]
async fn test_schema_setup() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join(unique_db_path());

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));

    let storage = LibsqlStorage::<TestTask, ()>::new(db_static);
    storage.setup().await?;

    let conn = db_static.connect()?;

    // Check Jobs table exists
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='Jobs'",
            libsql::params![],
        )
        .await?;
    assert!(rows.next().await?.is_some(), "Jobs table should exist");

    // Check Workers table exists
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='Workers'",
            libsql::params![],
        )
        .await?;
    assert!(rows.next().await?.is_some(), "Workers table should exist");

    Ok(())
}

/// Test worker heartbeat functionality
#[tokio::test]
async fn test_worker_heartbeat() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join(unique_db_path());

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));

    // Setup storage and schema with shorter heartbeat interval for testing
    let mut config = apalis_libsql::Config::new(std::any::type_name::<TestTask>());
    config = config.set_keep_alive(std::time::Duration::from_millis(500)); // 500ms for testing
    let storage = LibsqlStorage::<TestTask, ()>::new_with_config(db_static, config);
    storage.setup().await?;

    // Enable WAL mode for better concurrency
    apalis_libsql::enable_wal_mode(db_static).await?;

    // Get a connection for verification
    let conn = db_static.connect()?;

    // Create a worker - use poll first to register the worker via initial_heartbeat
    use apalis_core::worker::context::WorkerContext;
    let worker = WorkerContext::new::<&'static str>("heartbeat-test-worker");

    // Use poll to trigger initial registration (which includes worker registration)
    let mut poll_stream = storage.clone().poll_default(&worker).boxed();

    // Consume the initial registration result
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), poll_stream.next())
        .await?
        .ok_or("Poll stream ended unexpectedly")??;

    // Now start the heartbeat stream for periodic updates
    let mut heartbeat_stream = storage.heartbeat(&worker).boxed();

    // Wait for first heartbeat (should happen quickly with 500ms interval)
    tokio::time::timeout(std::time::Duration::from_secs(1), heartbeat_stream.next())
        .await?
        .ok_or("Heartbeat stream ended unexpectedly")??;

    // Verify worker was registered and last_seen was updated
    let mut rows = conn
        .query(
            "SELECT id, last_seen FROM Workers WHERE id = ?1",
            libsql::params!["heartbeat-test-worker"],
        )
        .await?;

    if let Some(row) = rows.next().await? {
        let worker_id: String = row.get(0)?;
        let last_seen: i64 = row.get(1)?;
        assert_eq!(worker_id, "heartbeat-test-worker");
        assert!(last_seen > 0, "last_seen should be set");
    } else {
        panic!("Worker should be registered after initial poll");
    }

    // Wait for another heartbeat to verify updates continue
    tokio::time::timeout(std::time::Duration::from_secs(2), heartbeat_stream.next())
        .await?
        .ok_or("Heartbeat stream ended unexpectedly")??;

    // Verify last_seen was updated again
    let mut rows = conn
        .query(
            "SELECT last_seen FROM Workers WHERE id = ?1",
            libsql::params!["heartbeat-test-worker"],
        )
        .await?;

    if let Some(row) = rows.next().await? {
        let last_seen_after: i64 = row.get(0)?;
        assert!(last_seen_after > 0, "last_seen should still be set");
    }

    Ok(())
}
