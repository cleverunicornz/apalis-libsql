//! Turso Cloud embedded replica example
//!
//! This example demonstrates using apalis-libsql with Turso Cloud's embedded replica feature.
//! The database will sync with the cloud and work offline.
//!
//! Run with: cargo run --example turso
//!
//! Required environment variables:
//! - TURSO_AUTH_TOKEN: Your Turso database auth token
//! - TURSO_DATABASE_URL: Your Turso database URL (e.g., libsql://your-db.turso.io)

use apalis_libsql::LibsqlStorage;
use libsql::Builder;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Notification {
    user_id: String,
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get Turso credentials from environment
    let auth_token =
        env::var("TURSO_AUTH_TOKEN").expect("TURSO_AUTH_TOKEN environment variable must be set");
    let database_url = env::var("TURSO_DATABASE_URL")
        .expect("TURSO_DATABASE_URL environment variable must be set");

    println!("Connecting to Turso Cloud...");

    // Create embedded replica that syncs with Turso Cloud
    let db = Builder::new_remote(database_url, auth_token)
        .build()
        .await?;

    let db: &'static _ = Box::leak(Box::new(db));

    // Create storage
    let storage = LibsqlStorage::<Notification, ()>::new(db);
    storage.setup().await?;

    println!("Database setup complete!");

    // Push some tasks
    use apalis_core::backend::TaskSink;
    let mut storage = storage;

    storage
        .push(Notification {
            user_id: "user123".to_string(),
            message: "Welcome to our service!".to_string(),
        })
        .await?;

    storage
        .push(Notification {
            user_id: "user456".to_string(),
            message: "Your order has been processed.".to_string(),
        })
        .await?;

    println!("Tasks pushed to Turso Cloud!");
    println!("The embedded replica will sync with the cloud automatically.");

    Ok(())
}
