# apalis-libsql

[![Crates.io](https://img.shields.io/crates/v/apalis-libsql.svg)](https://crates.io/crates/apalis-libsql)
[![Documentation](https://docs.rs/apalis-libsql/badge.svg)](https://docs.rs/apalis-libsql)
[![License](https://img.shields.io/crates/l/apalis-libsql.svg)](https://github.com/cleverunicornz/apalis-libsql#license)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org)

A storage backend for [Apalis](https://github.com/geofmureithi/apalis) that uses [Turso's libSQL](https://libsql.org/) instead of sqlx. This enables Apalis to use Turso's embedded replica model with cloud sync.

## Features

- **Task storage and retrieval** using libSQL
- **Turso Cloud support** - sync with cloud databases
- **Embedded replica model** - work offline with automatic sync
- **Compatible with Apalis** task processing framework
- **Worker heartbeat** and task acknowledgment
- **Atomic task locking** for concurrent processing
- **High performance** with connection pooling
- **Type safe** with runtime SQL validation

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
apalis-libsql = "0.1.0"
```

### Feature Flags

- `tokio-comp` (default): Enable Tokio runtime compatibility
- `async-std-comp`: Enable async-std runtime compatibility

```toml
# Use with Tokio (default)
apalis-libsql = "0.1.0"

# Use with async-std
apalis-libsql = { version = "0.1.0", default-features = false, features = ["async-std-comp"] }
```

## Usage

### Basic Example

```rust
use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyTask {
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    
    // Push tasks using TaskSink trait
    use apalis_core::backend::TaskSink;
    let mut storage = storage;
    storage.push(MyTask {
        message: "Hello, World!".to_string(),
    }).await?;
    
    println!("Task pushed successfully!");
    Ok(())
}
```

### Turso Cloud Example

```rust,no_run
use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Turso Cloud with embedded replica
    // Replace with your actual Turso database URL and auth token
    let db = Builder::new_remote(
        "libsql://your-db.turso.io".to_string(), 
        "your-auth-token".to_string()
    )
        .build()
        .await?;
    
    let db_static: &'static libsql::Database = Box::leak(Box::new(db));
    let storage = LibsqlStorage::<Email, ()>::new(db_static);
    storage.setup().await?;
    
    // Push tasks - they will sync with the cloud
    use apalis_core::backend::TaskSink;
    let mut storage = storage;
    storage.push(Email {
        to: "user@example.com".to_string(),
        subject: "Hello from Turso!".to_string(),
    }).await?;
    
    Ok(())
}
```

## Examples

See the [examples](examples/) directory for more comprehensive examples:

- [`basic.rs`](examples/basic.rs) - Simple local database usage
- [`turso.rs`](examples/turso.rs) - Turso Cloud embedded replica usage

Run examples with:

```bash
cargo run --example basic
cargo run --example turso
```

## Architecture

This crate provides a storage backend for Apalis that:

1. **Implements Apalis traits** - `Backend`, `BackendExt`, `TaskSink`, `Acknowledge`
2. **Uses libSQL** - Native libSQL driver instead of sqlx
3. **Supports Turso Cloud** - Embedded replica with automatic sync
4. **Provides atomic operations** - Transaction-based task management
5. **Handles concurrency** - Row-level locking for worker coordination

## Performance

- **Connection pooling** - Efficient database connection management
- **Batch operations** - Optimized for bulk task insertion
- **Indexed queries** - Fast task polling and retrieval
- **Memory efficient** - Streaming task processing

## Testing

The crate includes comprehensive tests with 91%+ coverage:

```bash
cargo test --all-features
```

## License

Licensed under the MIT License. See LICENSE for details.