//! Tests for sink functionality

use apalis_core::task::Task;
use apalis_libsql::{
    CompactType, Config, SqlContext,
    sink::{LibsqlSink, push_tasks},
};
use futures::Sink;
use libsql::Builder;
use std::sync::Arc;
use tempfile::TempDir;

struct TestDb {
    db: &'static libsql::Database,
    _temp_dir: Arc<TempDir>,
}

async fn setup_test_db() -> TestDb {
    let temp_dir = Arc::new(TempDir::new().unwrap());
    let db_path = temp_dir.path().join("test_sink.db");

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
async fn test_sink_new() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let sink = LibsqlSink::<(), ()>::new(db, &config);

    // Verify sink can be created and debug output contains expected info
    let debug_str = format!("{:?}", sink);
    assert!(debug_str.contains("LibsqlSink"));
    assert!(debug_str.contains("TestTask"));
    assert!(debug_str.contains("buffer_len"));
}

#[tokio::test]
async fn test_push_tasks_empty() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");

    // Test pushing empty buffer
    let empty_buffer = Vec::new();
    push_tasks(db, &config, empty_buffer).await.unwrap();

    // Verify no tasks were inserted
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Jobs", libsql::params![])
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let count: i64 = row.get(0).unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn test_push_tasks_batch() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let config = Config::new(job_type);

    // Create some tasks
    let mut tasks = Vec::new();
    for i in 0..3 {
        let ctx = SqlContext::new().with_max_attempts(5);
        let mut task = Task::new(CompactType::from(vec![i as u8]));
        task.parts.ctx = ctx;
        // Note: We can't easily set task_id in this test setup, but the function should work
        tasks.push(task);
    }

    // Push tasks
    push_tasks(db, &config, tasks).await.unwrap();

    // Verify tasks were inserted
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ?1",
            libsql::params![job_type],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let count: i64 = row.get(0).unwrap();
        assert_eq!(count, 3);
    }
}

#[tokio::test]
async fn test_sink_poll_ready() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let mut sink = LibsqlSink::<(), ()>::new(db, &config);

    // Test poll_ready - since LibsqlSink always returns Poll::Ready(Ok(())), this should work
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let mut pinned_sink = Pin::new(&mut sink);
    let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

    let result = pinned_sink.as_mut().poll_ready(cx);
    assert!(matches!(result, Poll::Ready(Ok(()))));
}

#[tokio::test]
async fn test_sink_start_send() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let mut sink = LibsqlSink::<(), ()>::new(db, &config);

    // Create a task
    let ctx = SqlContext::new().with_max_attempts(5);
    let mut task = Task::new(CompactType::from(vec![1, 2, 3]));
    task.parts.ctx = ctx;
    // Note: We can't easily set task_id in this test setup

    // Test start_send
    use std::pin::Pin;
    use std::task::Context;

    let mut pinned_sink = Pin::new(&mut sink);
    let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

    // First ensure we're ready
    let _ready = pinned_sink.as_mut().poll_ready(cx);

    // Then send the task
    pinned_sink.as_mut().start_send(task).unwrap();

    // The task should be buffered, not yet written to DB
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Jobs", libsql::params![])
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let count: i64 = row.get(0).unwrap();
        assert_eq!(count, 0); // Should still be 0 since we haven't flushed
    }
}

#[tokio::test]
async fn test_sink_poll_flush_empty() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let mut sink = LibsqlSink::<(), ()>::new(db, &config);

    // Test flushing empty sink
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let mut pinned_sink = Pin::new(&mut sink);
    let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

    let result = pinned_sink.as_mut().poll_flush(cx);
    assert!(matches!(result, Poll::Ready(Ok(()))));
}

#[tokio::test]
async fn test_sink_poll_flush_with_tasks() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let config = Config::new(job_type);
    let mut sink = LibsqlSink::<(), ()>::new(db, &config);

    // Create and send some tasks
    for i in 0..2 {
        let ctx = SqlContext::new().with_max_attempts(5);
        let mut task = Task::new(CompactType::from(vec![i as u8]));
        task.parts.ctx = ctx;
        // Note: We can't easily set task_id in this test setup

        use std::pin::Pin;
        use std::task::Context;

        let mut pinned_sink = Pin::new(&mut sink);
        let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

        // Ensure we're ready and send the task
        let _ready = pinned_sink.as_mut().poll_ready(cx);
        pinned_sink.as_mut().start_send(task).unwrap();
    }

    // Test flushing with tasks
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let mut pinned_sink = Pin::new(&mut sink);
    let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

    let result = pinned_sink.as_mut().poll_flush(cx);
    assert!(matches!(result, Poll::Ready(Ok(()))));

    // Verify tasks were inserted
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ?1",
            libsql::params![job_type],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let count: i64 = row.get(0).unwrap();
        assert_eq!(count, 2);
    }
}

#[tokio::test]
async fn test_sink_poll_close() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let mut sink = LibsqlSink::<(), ()>::new(db, &config);

    // Test closing (should flush first)
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let mut pinned_sink = Pin::new(&mut sink);
    let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

    let result = pinned_sink.as_mut().poll_close(cx);
    assert!(matches!(result, Poll::Ready(Ok(()))));
}

#[tokio::test]
async fn test_sink_clone_does_not_copy_buffer() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let mut sink1 = LibsqlSink::<(), ()>::new(db, &config);

    // Add a task to the first sink
    let ctx = SqlContext::new().with_max_attempts(5);
    let mut task = Task::new(CompactType::from(vec![1, 2, 3]));
    task.parts.ctx = ctx;

    use std::pin::Pin;
    use std::task::Context;

    let mut pinned_sink = Pin::new(&mut sink1);
    let cx = &mut Context::from_waker(futures::task::noop_waker_ref());

    // Ensure we're ready and send the task
    let _ready = pinned_sink.as_mut().poll_ready(cx);
    pinned_sink.as_mut().start_send(task).unwrap();

    // Verify the original sink has a task in buffer by checking debug output
    // We need to drop the pinned sink first to avoid borrowing conflicts
    drop(pinned_sink);
    let debug_str1 = format!("{:?}", sink1);
    assert!(
        debug_str1.contains("buffer_len: 1"),
        "Original sink should have 1 task in buffer: {}",
        debug_str1
    );

    // Clone the sink while it has buffered tasks
    let sink2 = sink1.clone();

    // Verify the cloned sink has an empty buffer (cloning should not copy buffered tasks)
    let debug_str2 = format!("{:?}", sink2);
    assert!(
        debug_str2.contains("buffer_len: 0"),
        "Cloned sink should have empty buffer: {}",
        debug_str2
    );

    // Re-create the pinned sink to flush the original
    let mut pinned_sink = Pin::new(&mut sink1);
    let _result = pinned_sink.as_mut().poll_flush(cx);

    // Verify task was inserted from original sink
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM Jobs", libsql::params![])
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let count: i64 = row.get(0).unwrap();
        assert_eq!(count, 1);
    }
}

#[tokio::test]
async fn test_sink_debug() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let config = Config::new("TestTask");
    let sink = LibsqlSink::<(), ()>::new(db, &config);

    let debug_str = format!("{:?}", sink);
    assert!(debug_str.contains("LibsqlSink"));
    assert!(debug_str.contains("TestTask"));
}

#[tokio::test]
async fn test_push_tasks_rollback_on_error() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    let job_type = "TestTask";
    let config = Config::new(job_type);

    // First, insert a task with a specific ID to create a UNIQUE constraint violation later
    let conn = db.connect().unwrap();
    let duplicate_ulid = ulid::Ulid::new();
    let duplicate_id = duplicate_ulid.to_string();
    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"existing_task", duplicate_id.clone(), job_type],
    )
    .await
    .unwrap();

    // Verify initial task count
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ?1",
            libsql::params![job_type],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let initial_count: i64 = row.get(0).unwrap();
        assert_eq!(initial_count, 1);
    }

    // Create tasks where one will cause a UNIQUE constraint violation
    let mut tasks = Vec::new();

    // First task - this will succeed
    let ctx1 = SqlContext::new().with_max_attempts(5);
    let mut task1 = Task::new(CompactType::from(vec![1u8]));
    task1.parts.ctx = ctx1;
    tasks.push(task1);

    // Second task - set its ID to the duplicate to force a constraint violation
    let ctx2 = SqlContext::new().with_max_attempts(5);
    let mut task2 = Task::new(CompactType::from(vec![2u8]));
    task2.parts.ctx = ctx2;
    // Set the task_id to the duplicate ID to trigger UNIQUE constraint violation
    task2.parts.task_id = Some(apalis_core::task::task_id::TaskId::new(duplicate_ulid));
    tasks.push(task2);

    // Third task - this should not be inserted due to rollback
    let ctx3 = SqlContext::new().with_max_attempts(5);
    let mut task3 = Task::new(CompactType::from(vec![3u8]));
    task3.parts.ctx = ctx3;
    tasks.push(task3);

    // Attempt to push tasks - this should fail due to duplicate ID
    let result = push_tasks(db, &config, tasks).await;

    // The operation should fail
    assert!(
        result.is_err(),
        "Expected push_tasks to fail due to duplicate ID"
    );

    // Verify that NO tasks were inserted (rollback should have occurred)
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ?1",
            libsql::params![job_type],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let final_count: i64 = row.get(0).unwrap();
        assert_eq!(
            final_count, 1,
            "Expected only the original task to remain after rollback"
        );
    }

    // Verify that the job data is still the original task, not any of the failed ones
    let mut rows = conn
        .query(
            "SELECT job FROM Jobs WHERE job_type = ?1 AND id = ?2",
            libsql::params![job_type, duplicate_id.clone()],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let job_data: Vec<u8> = row.get(0).unwrap();
        assert_eq!(
            job_data, b"existing_task",
            "Original task data should be preserved"
        );
    }
}
