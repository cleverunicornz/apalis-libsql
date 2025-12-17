//! Row mapping for libSQL database rows to TaskRow

use apalis_sql::from_row::TaskRow;
use chrono::{TimeZone, Utc};
use libsql::Row;

use crate::LibsqlError;

/// A task row from the libSQL database
#[derive(Debug)]
pub struct LibsqlTaskRow {
    pub job: Vec<u8>,
    pub id: Option<String>,
    pub job_type: Option<String>,
    pub status: Option<String>,
    pub attempts: Option<i64>,
    pub max_attempts: Option<i64>,
    pub run_at: Option<i64>,
    pub last_error: Option<String>,
    pub lock_at: Option<i64>,
    pub lock_by: Option<String>,
    pub done_at: Option<i64>,
    pub priority: Option<i64>,
    pub metadata: Option<String>,
}

impl LibsqlTaskRow {
    /// Create a LibsqlTaskRow from a libsql::Row
    ///
    /// Expects columns in the order returned by the fetch_next query:
    /// job, id, job_type, status, attempts, max_attempts, run_at, last_error, lock_at, lock_by, done_at, priority, metadata
    pub fn from_row(row: &Row) -> Result<Self, LibsqlError> {
        Ok(Self {
            job: row.get::<Vec<u8>>(0).map_err(LibsqlError::Database)?,
            id: row
                .get::<Option<String>>(1)
                .map_err(LibsqlError::Database)?,
            job_type: row
                .get::<Option<String>>(2)
                .map_err(LibsqlError::Database)?,
            status: row
                .get::<Option<String>>(3)
                .map_err(LibsqlError::Database)?,
            attempts: row.get::<Option<i64>>(4).map_err(LibsqlError::Database)?,
            max_attempts: row.get::<Option<i64>>(5).map_err(LibsqlError::Database)?,
            run_at: row.get::<Option<i64>>(6).map_err(LibsqlError::Database)?,
            last_error: row
                .get::<Option<String>>(7)
                .map_err(LibsqlError::Database)?,
            lock_at: row.get::<Option<i64>>(8).map_err(LibsqlError::Database)?,
            lock_by: row
                .get::<Option<String>>(9)
                .map_err(LibsqlError::Database)?,
            done_at: row.get::<Option<i64>>(10).map_err(LibsqlError::Database)?,
            priority: row.get::<Option<i64>>(11).map_err(LibsqlError::Database)?,
            metadata: row
                .get::<Option<String>>(12)
                .map_err(LibsqlError::Database)?,
        })
    }
}

impl TryFrom<LibsqlTaskRow> for TaskRow {
    type Error = LibsqlError;

    fn try_from(row: LibsqlTaskRow) -> Result<Self, Self::Error> {
        Ok(TaskRow {
            job: row.job,
            id: row
                .id
                .ok_or_else(|| LibsqlError::Other("Missing id".into()))?,
            job_type: row
                .job_type
                .ok_or_else(|| LibsqlError::Other("Missing job_type".into()))?,
            status: row
                .status
                .ok_or_else(|| LibsqlError::Other("Missing status".into()))?,
            attempts: row
                .attempts
                .ok_or_else(|| LibsqlError::Other("Missing attempts".into()))?
                .try_into()
                .map_err(|e| LibsqlError::Other(format!("Attempts overflow: {}", e)))?,
            max_attempts: row
                .max_attempts
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| LibsqlError::Other(format!("Max attempts overflow: {}", e)))?,
            run_at: row.run_at.and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
            last_result: row
                .last_error
                .as_ref()
                .and_then(|res| match serde_json::from_str(res) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        log::warn!("Failed to parse last_error JSON: {}", e);
                        None
                    }
                }),
            lock_at: row.lock_at.and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
            lock_by: row.lock_by,
            done_at: row.done_at.and_then(|ts| Utc.timestamp_opt(ts, 0).single()),
            priority: row
                .priority
                .map(|v| v.try_into())
                .transpose()
                .map_err(|e| LibsqlError::Other(format!("Priority overflow: {}", e)))?,
            metadata: row
                .metadata
                .as_ref()
                .and_then(|m| match serde_json::from_str(m) {
                    Ok(meta) => Some(meta),
                    Err(e) => {
                        log::warn!("Failed to parse metadata JSON: {}", e);
                        None
                    }
                }),
        })
    }
}
