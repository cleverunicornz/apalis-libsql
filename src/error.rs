use thiserror::Error;

#[derive(Error, Debug)]
pub enum LibsqlError {
    #[error("Database error: {0}")]
    Database(#[from] libsql::Error),
    
    #[error("Row not found")]
    RowNotFound,
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    #[error("Decode error: {0}")]
    Decode(#[from] serde_json::Error),
    
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    
    #[error("Task not found")]
    TaskNotFound,
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Other error: {0}")]
    Other(String),
}