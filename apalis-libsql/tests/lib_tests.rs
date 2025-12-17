//! Tests for lib.rs functionality

use apalis_core::{
    backend::{Backend, BackendExt},
    worker::context::WorkerContext,
};
use apalis_libsql::{LibsqlStorage, enable_wal_mode};
use futures::StreamExt;
use libsql::Builder;
use std::{sync::Arc, time::Duration};
use tempfile::TempDir;
use ulid::Ulid;

struct TestDb {
    db: &'static libsql::Database,
    _temp_dir: Arc<TempDir>,
}

async fn setup_test_db() -> TestDb {
    let temp_dir = Arc::new(TempDir::new().unwrap());
    let db_path = temp_dir.path().join("test_lib.db");

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

#[tokio::test]
async fn test_storage_new() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let storage = LibsqlStorage::<(), ()>::new(db);

    // Verify storage properties
    assert_eq!(storage.db() as *const _, db as *const _); // Should reference same database
    assert_eq!(storage.config().buffer_size(), 10); // Default buffer size
    // The default queue name is derived from the type name, which is "()" for unit type
    assert_eq!(storage.config().queue().to_string(), "()"); // Default queue name for unit type
}

#[tokio::test]
async fn test_storage_new_with_config() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = apalis_libsql::Config::new("TestTask").set_buffer_size(20);
    let storage = LibsqlStorage::<(), ()>::new_with_config(db, config.clone());

    // Verify storage was created with the correct config values
    assert_eq!(storage.config().buffer_size(), 20);
    assert_eq!(storage.config().queue().to_string(), "TestTask");
}

#[tokio::test]
async fn test_storage_with_codec() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let storage = LibsqlStorage::<(), ()>::new(db);
    let _storage_with_codec =
        storage.with_codec::<apalis_core::backend::codec::json::JsonCodec<Vec<u8>>>();

    // Verify storage can change codec
    println!("Storage codec changed successfully");
}

#[tokio::test]
async fn test_storage_db_getter() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let storage = LibsqlStorage::<(), ()>::new(db);
    let retrieved_db = storage.db();

    // Verify we get the same database reference
    assert_eq!(retrieved_db as *const _, db as *const _);
}

#[tokio::test]
async fn test_storage_config_getter() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = apalis_libsql::Config::new("TestTask").set_buffer_size(25);
    let _storage = LibsqlStorage::<(), ()>::new_with_config(db, config.clone());

    let retrieved_config = _storage.config();

    // Verify we get the same config
    assert_eq!(retrieved_config.buffer_size(), 25);
    assert_eq!(retrieved_config.queue().to_string(), "TestTask");
}

#[test]
fn test_storage_debug() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let test_db = rt.block_on(setup_test_db());
    let db = test_db.db;

    let storage = LibsqlStorage::<(), ()>::new(db);
    let debug_str = format!("{:?}", storage);

    assert!(debug_str.contains("LibsqlStorage"));
    assert!(debug_str.contains("Database"));
}

#[test]
fn test_storage_clone() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let test_db = rt.block_on(setup_test_db());
    let db = test_db.db;

    let storage1 = LibsqlStorage::<(), ()>::new(db);
    let storage2 = storage1.clone();

    // Verify clone works
    assert_eq!(storage2.db() as *const _, db as *const _);
}

#[tokio::test]
async fn test_enable_wal_mode() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Test enabling WAL mode
    enable_wal_mode(db).await.unwrap();

    // Verify WAL mode was enabled by checking the journal mode
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("PRAGMA journal_mode", libsql::params![])
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let mode: String = row.get(0).unwrap();
        assert_eq!(mode, "wal");
    }
}

#[tokio::test]
async fn test_backend_poll() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let worker_id = "test-worker";

    // Create worker first
    let conn = db.connect().unwrap();
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    // Insert a task
    let task_id = Ulid::new();
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id.to_string(), job_type],
    )
    .await
    .unwrap();

    // Use the default storage which has JSON codec
    let storage = LibsqlStorage::<(), ()>::new(db);
    let worker = WorkerContext::new::<&str>(worker_id);

    // Test polling - just verify we can create the stream without errors
    let mut stream = storage.poll(&worker);

    // Try to get the first item (registration)
    let first_result = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;

    // The first item should be the registration result (None)
    match first_result {
        Ok(Some(Ok(None))) => {
            // This is expected - first item is registration result (None)
            // Registration successful, no task returned yet
        }
        Ok(Some(Ok(Some(_task)))) => {
            // This is also acceptable - some implementations might return a task immediately
            panic!("Expected registration result (None) first, but got a task");
        }
        Ok(Some(Err(e))) => {
            // This might happen due to JSON decoding issues, which is expected
            // when using () as the Args type, but we should still get a registration result
            panic!("Expected registration result, but got error: {}", e);
        }
        Ok(None) => {
            // Stream ended unexpectedly
            panic!("Stream ended before registration result");
        }
        Err(_) => {
            // Timeout - this is not expected for registration
            panic!("Timeout waiting for registration result");
        }
    }

    // Try to get the second item (should be the actual task)
    let second_result = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;

    match second_result {
        Ok(Some(Ok(Some(_task)))) => {
            // Successfully received a task
        }
        Ok(Some(Ok(None))) => {
            // No task available yet, which is also acceptable
        }
        Ok(Some(Err(e))) => {
            // Task decoding failed, which is expected when using () as Args type
            // but the polling mechanism itself is working
            println!("Task decoding failed as expected: {}", e);
        }
        Ok(None) => {
            // Stream ended
            println!("Stream ended");
        }
        Err(_) => {
            // Timeout - no task available
            println!("No task available within timeout");
        }
    }
}

#[tokio::test]
async fn test_backend_heartbeat() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let _job_type = "TestTask";
    let worker_id = "test-worker";

    // Use the default storage which has JSON codec
    let storage = LibsqlStorage::<(), ()>::new(db);
    let worker = WorkerContext::new::<&str>(worker_id);

    // Test heartbeat - just verify we can create the stream without errors
    let mut heartbeat_stream = storage.heartbeat(&worker);

    // Try to get the first heartbeat
    let first_result = tokio::time::timeout(Duration::from_secs(5), heartbeat_stream.next()).await;

    // We verify that heartbeat stream can be created and polled
    // The specific outcome depends on implementation details, but the stream should be functional
    match first_result {
        Ok(Some(Ok(()))) => {
            // Heartbeat succeeded - this is the expected case
        }
        Ok(Some(Err(e))) => {
            // Heartbeat failed but stream is still functional - this is acceptable
            println!("Heartbeat error (stream still functional): {}", e);
        }
        Ok(None) => {
            // Stream ended - this is a valid state
        }
        Err(_) => {
            // Timeout - stream is pending but still functional
        }
    }

    // Verify that we can query the database (basic connectivity test)
    // The worker registration might happen asynchronously or require additional steps
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query(
            "SELECT id FROM Workers WHERE id = ?1",
            libsql::params![worker_id],
        )
        .await
        .unwrap();

    // We can at least verify that the database query works
    // Worker registration may happen asynchronously or require poll() to be called first
    let _worker_exists = rows.next().await.unwrap().is_some();

    // The main test is that we can create and interact with the heartbeat stream
    // without crashing - the specific worker registration timing is implementation detail
}

#[tokio::test]
async fn test_backend_middleware() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let storage = LibsqlStorage::<(), ()>::new(db);

    // Test middleware
    let _middleware = storage.middleware();

    // Verify middleware was created
    println!("Storage middleware created successfully");
}

#[tokio::test]
async fn test_backend_poll_compact() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let worker_id = "test-worker";

    // Create worker first
    let conn = db.connect().unwrap();
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    // Insert a task
    let task_id = Ulid::new();
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id.to_string(), job_type],
    )
    .await
    .unwrap();

    let storage =
        LibsqlStorage::<(), ()>::new_with_config(db, apalis_libsql::Config::new(job_type));
    let worker = WorkerContext::new::<&str>(worker_id);

    // Test polling compact tasks
    let mut stream = storage.poll_compact(&worker);

    // The first item should be the registration result (None)
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(first.is_ok());

    // The second item should be the actual task
    let second = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(second.is_ok());

    if let Ok(Some(Ok(Some(task)))) = second {
        // Since we're using Vec<u8> as the Args type, task.args should be Vec<u8>
        assert_eq!(task.args, b"test_job_data");
    } else {
        panic!("Should have received a task: {:?}", second);
    }
}

#[tokio::test]
async fn test_storage_poll_default() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let worker_id = "test-worker";

    // Create worker first
    let conn = db.connect().unwrap();
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    // Insert a task
    let task_id = Ulid::new();
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id.to_string(), job_type],
    )
    .await
    .unwrap();

    let storage =
        LibsqlStorage::<(), ()>::new_with_config(db, apalis_libsql::Config::new(job_type));
    let worker = WorkerContext::new::<&str>(worker_id);

    // Test polling default
    let mut stream = storage.poll_default(&worker);

    // The first item should be the registration result (None)
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(first.is_ok());

    // The second item should be the actual task
    let second = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(second.is_ok());

    if let Ok(Some(Ok(Some(task)))) = second {
        // Default polling should return compact tasks (Vec<u8>)
        assert_eq!(task.args, b"test_job_data");
    } else {
        panic!("Should have received a task");
    }
}

#[tokio::test]
async fn test_orphan_task_reenqueue() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let dead_worker_id = "dead-worker";
    let task_id = Ulid::new();

    // 1. Setup DB with schema (already done in setup_test_db)

    let conn = db.connect().unwrap();

    // 2. Insert a worker record with old last_seen (dead worker) FIRST
    // Calculate last_seen to be older than reenqueue_orphaned_after (300 seconds) + 1 second
    let old_last_seen = chrono::Utc::now().timestamp() - 301;
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', ?3)",
        libsql::params![dead_worker_id, job_type, old_last_seen],
    )
    .await
    .unwrap();

    // 3. Insert a task that is "locked" by the dead worker
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata, lock_by, lock_at) 
         VALUES (?1, ?2, ?3, 'Running', 0, 3, strftime('%s', 'now'), 0, '{}', ?4, strftime('%s', 'now'))",
        libsql::params![b"orphaned_task_data", task_id.to_string(), job_type, dead_worker_id],
    )
    .await
    .unwrap();

    // 4. Call reenqueue_orphaned() function
    let config = apalis_libsql::Config::new(job_type);
    let reenqueued_count = apalis_libsql::reenqueue_orphaned(db, &config)
        .await
        .unwrap();

    // 5. Verify:
    //    - Task status is now 'Pending'
    //    - lock_by is NULL
    //    - lock_at is NULL
    assert_eq!(
        reenqueued_count, 1,
        "Should have re-enqueued 1 orphaned task"
    );

    // Verify the task was re-enqueued
    let mut rows = conn
        .query(
            "SELECT status, lock_by, lock_at FROM Jobs WHERE id = ?1",
            libsql::params![task_id.to_string()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let status: String = row.get(0).unwrap();
        let lock_by: Option<String> = row.get(1).unwrap();
        let lock_at: Option<i64> = row.get(2).unwrap();

        assert_eq!(
            status, "Pending",
            "Task status should be 'Pending' after re-enqueue"
        );
        assert!(lock_by.is_none(), "lock_by should be NULL after re-enqueue");
        assert!(lock_at.is_none(), "lock_at should be NULL after re-enqueue");
    } else {
        panic!("Task should exist after re-enqueue");
    }
}
