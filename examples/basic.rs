//! Basic example of using apalis-libsql
//!
//! Run with: cargo run --example basic

use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create local database
    let db = Builder::new_local("tasks.db").build().await?;
    let db: &'static _ = Box::leak(Box::new(db));

    // Create storage
    let storage = LibsqlStorage::<Email, ()>::new(db);
    storage.setup().await?;

    // Push a task
    use apalis_core::backend::TaskSink;
    let mut storage = storage;
    storage
        .push(Email {
            to: "user@example.com".to_string(),
            subject: "Hello!".to_string(),
        })
        .await?;

    println!("Task pushed successfully!");
    Ok(())
}
