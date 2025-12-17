//! Database Performance Benchmarks
//!
//! Run: cargo test --test perf_test --release -- --nocapture --test-threads=1

use libsql::Builder;
use std::time::Instant;
use tempfile::TempDir;

const ITERATIONS: u64 = 10_000;

// Helper function to create a test database with proper SQLite settings
async fn setup_benchmark_db() -> Result<(libsql::Connection, TempDir), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("benchmark.db");

    let db = Builder::new_local(db_path.to_str().unwrap())
        .build()
        .await?;
    let conn = db.connect()?;

    // Configure SQLite for optimal performance
    conn.query("PRAGMA journal_mode = WAL", libsql::params![])
        .await?;
    conn.query("PRAGMA synchronous = NORMAL", libsql::params![])
        .await?;
    conn.query("PRAGMA cache_size = 100000", libsql::params![])
        .await?;
    conn.query("PRAGMA temp_store = MEMORY", libsql::params![])
        .await?;
    conn.query("PRAGMA wal_autocheckpoint = 1000", libsql::params![])
        .await?;

    // Create test tables
    conn.execute(
        "CREATE TABLE IF NOT EXISTS benchmark_data (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )",
        libsql::params![],
    )
    .await?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL,
            name TEXT NOT NULL
        )",
        libsql::params![],
    )
    .await?;

    Ok((conn, temp_dir))
}

// Helper to populate test data
async fn populate_data(
    conn: &libsql::Connection,
    count: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx = conn.transaction().await?;

    for i in 0..count {
        tx.execute(
            "INSERT INTO benchmark_data (id, data, created_at) VALUES (?1, ?2, ?3)",
            libsql::params![i as i64, format!("data_{}", i), i as i64],
        )
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

// Helper to populate accounts for transaction testing
async fn populate_accounts(
    conn: &libsql::Connection,
    count: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx = conn.transaction().await?;

    for i in 0..count {
        tx.execute(
            "INSERT INTO accounts (id, balance, name) VALUES (?1, ?2, ?3)",
            libsql::params![i as i64, 1000, format!("account_{}", i)],
        )
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_raw_write_iops() -> Result<(), Box<dyn std::error::Error>> {
    let (conn, _temp_dir) = setup_benchmark_db().await?;

    println!("\n=== Raw Write IOPS ===");
    println!("Testing single INSERT performance (worst case)");

    let start = Instant::now();

    // Single inserts without batching
    for i in 0..ITERATIONS {
        conn.execute(
            "INSERT INTO benchmark_data (id, data, created_at) VALUES (?1, ?2, ?3)",
            libsql::params![i as i64, format!("data_{}", i), i as i64],
        )
        .await?;
    }

    let elapsed = start.elapsed();
    let iops = ITERATIONS as f64 / elapsed.as_secs_f64();

    println!("  Iterations: {}", ITERATIONS);
    println!("  Time: {:.1}s", elapsed.as_secs_f64());
    println!("  IOPS: {:.0} writes/sec", iops);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_batched_write_throughput() -> Result<(), Box<dyn std::error::Error>> {
    let (conn, _temp_dir) = setup_benchmark_db().await?;

    println!("\n=== Batched Write Throughput ===");
    println!("Testing batched INSERT performance (best case)");

    let start = Instant::now();

    // All inserts in a single transaction
    let tx = conn.transaction().await?;
    for i in 0..ITERATIONS {
        tx.execute(
            "INSERT INTO benchmark_data (id, data, created_at) VALUES (?1, ?2, ?3)",
            libsql::params![i as i64, format!("data_{}", i), i as i64],
        )
        .await?;
    }
    tx.commit().await?;

    let elapsed = start.elapsed();
    let throughput = ITERATIONS as f64 / elapsed.as_secs_f64();

    println!("  Iterations: {}", ITERATIONS);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.0} writes/sec", throughput);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_read_iops() -> Result<(), Box<dyn std::error::Error>> {
    let (conn, _temp_dir) = setup_benchmark_db().await?;

    // Pre-populate data
    populate_data(&conn, ITERATIONS).await?;

    println!("\n=== Read IOPS ===");
    println!("Testing single SELECT performance");

    let start = Instant::now();
    let mut reads = 0u64;

    // Single selects by primary key
    for i in 0..ITERATIONS {
        let mut rows = conn
            .query(
                "SELECT data FROM benchmark_data WHERE id = ?1",
                libsql::params![i as i64],
            )
            .await?;

        if rows.next().await?.is_some() {
            reads += 1;
        }
    }

    let elapsed = start.elapsed();
    let iops = reads as f64 / elapsed.as_secs_f64();

    println!("  Iterations: {}", reads);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
    println!("  IOPS: {:.0} reads/sec", iops);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_concurrent_reads() -> Result<(), Box<dyn std::error::Error>> {
    let (conn, _temp_dir) = setup_benchmark_db().await?;

    // Pre-populate data
    populate_data(&conn, ITERATIONS).await?;

    println!("\n=== Concurrent Reads (4 threads) ===");
    println!("Testing concurrent SELECT performance");

    let start = Instant::now();
    let mut handles = vec![];

    // Spawn 4 concurrent readers
    for thread_id in 0..4 {
        let conn_clone = conn.clone();
        let handle = tokio::spawn(async move {
            let mut reads = 0u64;
            let start_id = thread_id * (ITERATIONS / 4);
            let end_id = start_id + (ITERATIONS / 4);

            for i in start_id..end_id {
                let mut rows = conn_clone
                    .query(
                        "SELECT data FROM benchmark_data WHERE id = ?1",
                        libsql::params![i as i64],
                    )
                    .await
                    .unwrap();

                if rows.next().await.unwrap().is_some() {
                    reads += 1;
                }
            }
            reads
        });
        handles.push(handle);
    }

    // Wait for all readers to complete
    let mut total_reads = 0u64;
    for handle in handles {
        total_reads += handle.await?;
    }

    let elapsed = start.elapsed();
    let iops = total_reads as f64 / elapsed.as_secs_f64();

    println!("  Total reads: {}", total_reads);
    println!("  Time: {:.3}s", elapsed.as_secs_f64());
    println!("  IOPS: {:.0} reads/sec", iops);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_transaction_tps() -> Result<(), Box<dyn std::error::Error>> {
    let (conn, _temp_dir) = setup_benchmark_db().await?;

    // Pre-populate accounts
    populate_accounts(&conn, 1000).await?;

    println!("\n=== Transaction TPS ===");
    println!("Testing transaction performance (BEGIN, UPDATE x2, COMMIT)");

    const TX_COUNT: u64 = 1_000;
    let start = Instant::now();

    for _i in 0..TX_COUNT {
        let tx = conn.transaction().await?;

        // Debit from account 1
        tx.execute(
            "UPDATE accounts SET balance = balance - 10 WHERE id = ?1",
            libsql::params![0],
        )
        .await?;

        // Credit to account 2
        tx.execute(
            "UPDATE accounts SET balance = balance + 10 WHERE id = ?1",
            libsql::params![1],
        )
        .await?;

        tx.commit().await?;
    }

    let elapsed = start.elapsed();
    let tps = TX_COUNT as f64 / elapsed.as_secs_f64();

    println!("  Transactions: {}", TX_COUNT);
    println!("  Time: {:.1}s", elapsed.as_secs_f64());
    println!("  TPS: {:.0} tx/sec", tps);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_mixed_workload() -> Result<(), Box<dyn std::error::Error>> {
    let (conn, _temp_dir) = setup_benchmark_db().await?;

    // Pre-populate data
    populate_data(&conn, ITERATIONS).await?;

    println!("\n=== Mixed Workload (80/20) ===");
    println!("Testing realistic read/write mix");

    let start = Instant::now();
    let mut operations = 0u64;

    // 80% reads, 20% writes
    for i in 0..ITERATIONS {
        if i % 5 == 0 {
            // 20% writes
            conn.execute(
                "INSERT INTO benchmark_data (id, data, created_at) VALUES (?1, ?2, ?3)",
                libsql::params![(ITERATIONS + i) as i64, format!("new_data_{}", i), i as i64],
            )
            .await?;
        } else {
            // 80% reads
            let mut rows = conn
                .query(
                    "SELECT data FROM benchmark_data WHERE id = ?1",
                    libsql::params![(i % ITERATIONS) as i64],
                )
                .await?;

            if rows.next().await?.is_some() {
                operations += 1;
            }
        }
        operations += 1;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = operations as f64 / elapsed.as_secs_f64();

    println!("  Operations: {}", operations);
    println!("  Time: {:.1}s", elapsed.as_secs_f64());
    println!("  OPS: {:.0} ops/sec", ops_per_sec);

    Ok(())
}

// Summary benchmark that runs all tests and provides a formatted output
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_summary() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n============================================");
    println!("       libSQL Database Performance");
    println!("============================================");

    // Note: Individual benchmarks are separate tests and should be run individually
    // This summary just provides the header format expected in the contract
    println!("Run individual benchmarks with:");
    println!(
        "cargo test --test perf_test --release -- --nocapture --test-threads=1 bench_raw_write_iops"
    );
    println!(
        "cargo test --test perf_test --release -- --nocapture --test-threads=1 bench_batched_write_throughput"
    );
    println!(
        "cargo test --test perf_test --release -- --nocapture --test-threads=1 bench_read_iops"
    );
    println!(
        "cargo test --test perf_test --release -- --nocapture --test-threads=1 bench_concurrent_reads"
    );
    println!(
        "cargo test --test perf_test --release -- --nocapture --test-threads=1 bench_transaction_tps"
    );
    println!(
        "cargo test --test perf_test --release -- --nocapture --test-threads=1 bench_mixed_workload"
    );

    println!("\n============================================");

    Ok(())
}
