# Apalis libSQL Storage Backend

A storage backend for Apalis that uses Turso's libSQL instead of sqlx. This enables Apalis to use Turso's embedded replica model with cloud sync.

## Features

- Task storage and retrieval using libSQL
- Support for both local and cloud sync databases
- Compatible with Apalis task processing framework
- Worker heartbeat and task acknowledgment
- Atomic task locking for concurrent processing

## Usage

```rust
use apalis_libsql::{LibsqlStorage, LibsqlError};
use libsql::Builder;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyTask {
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), LibsqlError> {
    // Create local database
    let db = Builder::new_local("tasks.db").build().await?;
    
    // Convert to static reference for use in storage backend
    // Note: Box::leak is used here to create a 'static reference.
    // This creates a permanent memory leak - the memory will never be freed.
    // This is intentional - the database connection needs to live for the 
    // entire duration of the application, and this pattern is common 
    // when working with async storage backends that require 'static data.
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));
    
    // Create storage backend
    let storage: LibsqlStorage<MyTask, _> = LibsqlStorage::new(db_static);
    
    // Setup database schema
    storage.setup().await?;
    
    Ok(())
}
```

## License

MIT