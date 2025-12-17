//! Tests for row mapping functionality

use apalis_libsql::row::LibsqlTaskRow;
use libsql::Builder;
use std::sync::Arc;
use tempfile::TempDir;

struct TestDb {
    db: &'static libsql::Database,
    _temp_dir: Arc<TempDir>,
}

async fn setup_test_db() -> TestDb {
    let temp_dir = Arc::new(TempDir::new().unwrap());
    let db_path = temp_dir.path().join("test_row.db");

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
async fn test_libsql_task_row_from_row() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Insert a test row
    let conn = db.connect().unwrap();
    let task_id = "test_task_id";
    let job_type = "TestTask";

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id, job_type],
    )
    .await
    .unwrap();

    // Fetch the row back
    let mut rows = conn
        .query(
            "SELECT job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata FROM Jobs WHERE id = ?1",
            libsql::params![task_id],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let libsql_row = LibsqlTaskRow::from_row(&row).unwrap();

        assert_eq!(libsql_row.job, b"test_job_data");
        assert_eq!(libsql_row.id, Some(task_id.to_string()));
        assert_eq!(libsql_row.job_type, Some(job_type.to_string()));
        assert_eq!(libsql_row.status, Some("Pending".to_string()));
        assert_eq!(libsql_row.attempts, Some(0));
        assert_eq!(libsql_row.max_attempts, Some(3));
        assert!(libsql_row.run_at.is_some());
        assert_eq!(libsql_row.priority, Some(0));
    } else {
        panic!("Should have found a row");
    }
}

#[tokio::test]
async fn test_libsql_task_row_with_optional_fields() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Insert a test row with optional fields
    let conn = db.connect().unwrap();
    let task_id = "test_task_id_with_options";
    let job_type = "TestTask";
    let worker_id = "test_worker";

    // Create worker first
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Running', 1, 3, strftime('%s', 'now'), '\"Previous error\"', strftime('%s', 'now'), ?4, strftime('%s', 'now'), 1, '{\"key\": \"value\"}')",
        libsql::params![b"test_job_data", task_id, job_type, worker_id],
    )
    .await
    .unwrap();

    // Fetch the row back
    let mut rows = conn
        .query(
            "SELECT job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata FROM Jobs WHERE id = ?1",
            libsql::params![task_id],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let libsql_row = LibsqlTaskRow::from_row(&row).unwrap();

        assert_eq!(libsql_row.job, b"test_job_data");
        assert_eq!(libsql_row.id, Some(task_id.to_string()));
        assert_eq!(libsql_row.job_type, Some(job_type.to_string()));
        assert_eq!(libsql_row.status, Some("Running".to_string()));
        assert_eq!(libsql_row.attempts, Some(1));
        assert_eq!(libsql_row.max_attempts, Some(3));
        assert!(libsql_row.run_at.is_some());
        assert_eq!(
            libsql_row.last_error,
            Some("\"Previous error\"".to_string())
        );
        assert!(libsql_row.lock_at.is_some());
        assert_eq!(libsql_row.lock_by, Some(worker_id.to_string()));
        assert!(libsql_row.done_at.is_some());
        assert_eq!(libsql_row.priority, Some(1));
        assert_eq!(
            libsql_row.metadata,
            Some("{\"key\": \"value\"}".to_string())
        );
    } else {
        panic!("Should have found a row");
    }
}

#[tokio::test]
async fn test_try_into_task_row() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Insert a complete row
    let conn = db.connect().unwrap();
    let task_id = "test_task_id";
    let job_type = "TestTask";

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Pending', 0, 3, strftime('%s', 'now'), 0, '{}')",
        libsql::params![b"test_job_data", task_id, job_type],
    )
    .await
    .unwrap();

    // Fetch the row back
    let mut rows = conn
        .query(
            "SELECT job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata FROM Jobs WHERE id = ?1",
            libsql::params![task_id],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let libsql_row = LibsqlTaskRow::from_row(&row).unwrap();
        let task_row: apalis_sql::from_row::TaskRow = libsql_row.try_into().unwrap();

        assert_eq!(task_row.id, task_id);
        assert_eq!(task_row.job_type, job_type);
        assert_eq!(task_row.status, "Pending");
        assert_eq!(task_row.attempts, 0);
        assert_eq!(task_row.max_attempts, Some(3));
    } else {
        panic!("Should have found a row");
    }
}

#[tokio::test]
async fn test_try_into_task_row_with_optional_fields() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Insert a complete row with optional fields
    let conn = db.connect().unwrap();
    let task_id = "test_task_id_with_options";
    let job_type = "TestTask";
    let worker_id = "test_worker";

    // Create worker first
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Running', 1, 3, strftime('%s', 'now'), '\"Previous error\"', strftime('%s', 'now'), ?4, strftime('%s', 'now'), 1, '{\"key\": \"value\"}')",
        libsql::params![b"test_job_data", task_id, job_type, worker_id],
    )
    .await
    .unwrap();

    // Fetch the row back
    let mut rows = conn
        .query(
            "SELECT job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata FROM Jobs WHERE id = ?1",
            libsql::params![task_id],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let libsql_row = LibsqlTaskRow::from_row(&row).unwrap();
        let task_row: apalis_sql::from_row::TaskRow = libsql_row.try_into().unwrap();

        assert_eq!(task_row.id, task_id);
        assert_eq!(task_row.job_type, job_type);
        assert_eq!(task_row.status, "Running");
        assert_eq!(task_row.attempts, 1);
        assert_eq!(task_row.max_attempts, Some(3));
        assert_eq!(
            task_row.last_result,
            Some(serde_json::json!("Previous error"))
        ); // Valid JSON should parse correctly
        assert!(task_row.lock_at.is_some());
        assert_eq!(task_row.lock_by, Some(worker_id.to_string()));
        assert!(task_row.done_at.is_some());
        assert_eq!(task_row.priority, Some(1));
        assert_eq!(task_row.metadata, Some(serde_json::json!({"key": "value"})));
    // Valid JSON should parse correctly
    } else {
        panic!("Should have found a row");
    }
}

#[tokio::test]
async fn test_try_into_task_row_invalid_json() {
    let test_db = setup_test_db().await;
    let db = test_db.db;

    // Insert a row with invalid JSON
    let conn = db.connect().unwrap();
    let task_id = "test_task_id_invalid_json";
    let job_type = "TestTask";
    let worker_id = "test_worker";

    // Create worker first
    conn.execute(
        "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, 'LibsqlStorage', '', strftime('%s', 'now'))",
        libsql::params![worker_id, job_type],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata) 
         VALUES (?1, ?2, ?3, 'Failed', 1, 3, strftime('%s', 'now'), '{invalid json', strftime('%s', 'now'), ?4, strftime('%s', 'now'), 1, '{invalid json')",
        libsql::params![b"test_job_data", task_id, job_type, worker_id],
    )
    .await
    .unwrap();

    // Fetch the row back
    let mut rows = conn
        .query(
            "SELECT job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata FROM Jobs WHERE id = ?1",
            libsql::params![task_id],
        )
        .await
        .unwrap();

    if let Some(row) = rows.next().await.unwrap() {
        let libsql_row = LibsqlTaskRow::from_row(&row).unwrap();
        let task_row: apalis_sql::from_row::TaskRow = libsql_row.try_into().unwrap();

        assert_eq!(task_row.id, task_id);
        assert_eq!(task_row.job_type, job_type);
        assert_eq!(task_row.status, "Failed");
        assert_eq!(task_row.attempts, 1);
        assert_eq!(task_row.max_attempts, Some(3));
        assert_eq!(task_row.last_result, None); // Invalid JSON should be None
        assert!(task_row.lock_at.is_some());
        assert_eq!(task_row.lock_by, Some(worker_id.to_string()));
        assert!(task_row.done_at.is_some());
        assert_eq!(task_row.priority, Some(1));
        assert_eq!(task_row.metadata, None); // Invalid JSON should be None
    } else {
        panic!("Should have found a row");
    }
}
