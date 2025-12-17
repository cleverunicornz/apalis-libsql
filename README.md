# apalis-libsql

[![Crates.io](https://img.shields.io/crates/v/apalis-libsql.svg)](https://crates.io/crates/apalis-libsql)
[![Documentation](https://docs.rs/apalis-libsql/badge.svg)](https://docs.rs/apalis-libsql)
[![License](https://img.shields.io/crates/l/apalis-libsql.svg)](https://github.com/cleverunicornz/apalis-libsql#license)

Native libSQL storage backend for [Apalis](https://github.com/geofmureithi/apalis) background job processing.

**Problem**: Apalis uses sqlx for SQLite, but that doesn't work with Turso's embedded replica model or edge deployment.

**Solution**: This crate provides a native libSQL driver that enables:
- Local-first development with SQLite
- Embedded replicas that sync with Turso Cloud
- Edge deployment (Cloudflare Workers, Fly.io, etc.)
- Automatic cloud sync without managing connections

## Performance

Real benchmarks on standard hardware:

```text
Raw write IOPS:     ~117K/sec  (single INSERTs)
Batched writes:     ~578K/sec  (transaction batching)
Read IOPS:          ~272K/sec  (primary key lookups)
Transaction TPS:    ~78K/sec   (BEGIN/UPDATE x2/COMMIT)
```

Run benchmarks: `cargo test --test perf_test --release -- --nocapture`

## Architecture

```text
+-----------------+     +------------------+     +-----------------+
|   Your App      |---->|  Local Replica   |<----|  Turso Cloud    |
|                 |     |  (SQLite file)   |     |  (Distributed)  |
+-----------------+     +------------------+     +-----------------+
                               |                         |
                               v                         v
                        +------------------+     +------------------+
                        |  Apalis Workers  |     |  Other Replicas  |
                        |  Process Tasks   |     |  Edge Locations  |
                        +------------------+     +------------------+
```

**How it works**: Your app writes to a local SQLite file. libSQL automatically syncs changes with Turso Cloud, which distributes to other replicas. Works offline, syncs when connected.

## When to Use This vs apalis-sqlite

**Use apalis-sqlite**:
- Standard SQLite deployment
- Compile-time SQL query validation
- Single-machine applications
- Traditional server environments

**Use apalis-libsql**:
- Turso Cloud integration needed
- Embedded replica model (local-first)
- Edge deployment (Cloudflare Workers, Fly.io)
- Multi-region applications
- Offline-first requirements

## Quick Start

### Local Development

```rust
use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Serialize, Deserialize};
use apalis_core::backend::TaskSink;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Local SQLite database
    let db = Builder::new_local("tasks.db").build().await?;
    let db: &'static _ = Box::leak(Box::new(db));
    
    let storage = LibsqlStorage::<Email, ()>::new(db);
    storage.setup().await?;
    
    // Push tasks
    let mut storage = storage;
    storage.push(Email {
        to: "user@example.com".to_string(),
        subject: "Hello!".to_string(),
    }).await?;
    
    Ok(())
}
```

### Turso Cloud (Embedded Replica)

```rust,no_run
use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Serialize, Deserialize};
use apalis_core::backend::TaskSink;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Embedded replica with cloud sync
    let db = Builder::new_remote(
        "libsql://your-db.turso.io".to_string(),
        "your-auth-token".to_string()
    ).build().await?;
    
    let db: &'static _ = Box::leak(Box::new(db));
    let storage = LibsqlStorage::<Email, ()>::new(db);
    storage.setup().await?;
    
    // Tasks automatically sync with Turso Cloud
    let mut storage = storage;
    storage.push(Email {
        to: "user@example.com".to_string(),
        subject: "Hello from Turso!".to_string(),
    }).await?;
    
    Ok(())
}
```

### Task Processing

```rust,no_run
use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Serialize, Deserialize};
use apalis_core::backend::{TaskSink, Backend};
use apalis_core::worker::context::WorkerContext;
use futures::StreamExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Builder::new_local("tasks.db").build().await?;
    let db: &'static _ = Box::leak(Box::new(db));
    
    let storage = LibsqlStorage::<Email, ()>::new(db);
    storage.setup().await?;
    
    // Push some tasks
    let mut storage = storage;
    storage.push(Email {
        to: "user@example.com".to_string(),
        subject: "Hello!".to_string(),
    }).await?;
    
    // Create worker context for polling
    let worker = WorkerContext::new::<&str>("email-worker");
    
    // Poll for tasks and process them
    let mut stream = storage.poll(&worker);
    while let Some(task_result) = stream.next().await {
        match task_result {
            Ok(Some(task)) => {
                println!("Processing email to: {}", task.args.to);
                // Process the task here
                // In production, you'd use proper worker infrastructure
            }
            Ok(None) => {
                // No tasks available, continue polling
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            Err(e) => {
                eprintln!("Error polling task: {}", e);
            }
        }
    }
    
    Ok(())
}
```

## Configuration

```rust
use std::time::Duration;
use apalis_libsql::{LibsqlStorage, Config};
use libsql::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Builder::new_local("tasks.db").build().await?;
    let db: &'static _ = Box::leak(Box::new(db));
    
    let config = Config::new("my_queue")
        .set_buffer_size(50)              // Tasks per poll (default: 10)
        .set_poll_interval(Duration::from_millis(50))  // Poll frequency (default: 100ms)
        .set_keep_alive(Duration::from_secs(60))       // Worker heartbeat (default: 30s)
        .set_reenqueue_orphaned_after(Duration::from_secs(600)); // Retry dead tasks after 10min
    
    let storage = LibsqlStorage::<(), ()>::new_with_config(db, config);
    
    Ok(())
}
```

**Key settings**:
- `buffer_size`: Tasks fetched per poll (affects memory usage)
- `poll_interval`: How often to check for new tasks (affects latency)
- `keep_alive`: Worker heartbeat interval (affects failure detection)
- `reenqueue_orphaned_after`: When to retry tasks from crashed workers

## Turso Setup

1. **Create database**:
```bash
# Install Turso CLI
curl -sSfL https://get.tur.so/install.sh | bash

# Login
turso auth login

# Create database
turso db create my-tasks-db

# Get database URL
turso db show my-tasks-db --url
```

2. **Get auth token**:
```bash
turso db tokens create my-tasks-db
```

3. **Use in your app**:
```rust,no_run
use apalis_libsql::LibsqlStorage;
use libsql::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Builder::new_remote(
        "libsql://my-tasks-db-your-org.turso.io".to_string(),
        "your-auth-token-here".to_string()
    ).build().await?;
    
    let db: &'static _ = Box::leak(Box::new(db));
    let storage = LibsqlStorage::<(), ()>::new(db);
    
    Ok(())
}
```

## Edge Deployment

Works on Cloudflare Workers, Fly.io, and other edge platforms:

```rust,no_run
// This is a conceptual example - actual Cloudflare Workers integration
// would require the worker crate and proper WASM setup
use apalis_libsql::LibsqlStorage;
use libsql::Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In a real Cloudflare Worker, you'd get these from environment secrets
    let db = Builder::new_remote(
        "libsql://your-db.turso.io".to_string(),
        "your-auth-token".to_string()
    ).build().await?;
    
    let db: &'static _ = Box::leak(Box::new(db));
    let storage = LibsqlStorage::<(), ()>::new(db);
    
    // Process tasks at the edge
    Ok(())
}
```

## Database Schema

The storage creates these tables:

```sql
-- Workers table (worker registration and heartbeats)
CREATE TABLE Workers (
    id TEXT PRIMARY KEY,
    worker_type TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    layers TEXT,
    last_seen INTEGER NOT NULL
);

-- Jobs table (task storage)
CREATE TABLE Jobs (
    job BLOB NOT NULL,              -- serialized task data
    id TEXT PRIMARY KEY,            -- task ID (ULID)
    job_type TEXT NOT NULL,         -- queue name
    status TEXT NOT NULL,           -- Pending, Running, Done, Failed
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    run_at INTEGER NOT NULL,        -- scheduled execution time
    last_error TEXT,                -- error message on failure
    lock_at INTEGER,                -- when task was locked
    lock_by TEXT,                   -- worker that locked the task
    done_at INTEGER,                -- completion time
    priority INTEGER NOT NULL,
    metadata TEXT                   -- additional JSON metadata
);
```

## Installation

```toml
[dependencies]
apalis-libsql = "0.1.0"
```

Feature flags:
- `tokio-comp` (default): Tokio runtime support
- `async-std-comp`: async-std runtime support

## Testing

```bash
# Run all tests
cargo test --all-features

# Run performance benchmarks
cargo test --test perf_test --release -- --nocapture

# Test with Turso (requires env vars)
TURSO_AUTH_TOKEN=xxx TURSO_DATABASE_URL=xxx cargo test --test turso_cloud
```

## License

MIT