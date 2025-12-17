# apalis-libsql

A storage backend for [Apalis](https://github.com/geofmureithi/apalis) using [libSQL](https://github.com/tursodatabase/libsql) / [Turso](https://turso.tech).

## Why?

Apalis provides `apalis-sqlite` using sqlx, but Turso's embedded replica model requires the native libSQL client. This crate enables:

- **Local-first**: Instant writes to local SQLite
- **Cloud sync**: Automatic replication to Turso Cloud
- **Edge deployment**: Works on Cloudflare Workers, Fly.io, etc.

## Features

- `Backend` + `BackendExt` traits for task polling
- `Sink` trait for pushing tasks
- `Acknowledge` trait for task completion
- Worker heartbeat and orphan task recovery
- 91%+ test coverage

## Usage

```rust
use apalis_libsql::LibsqlStorage;
use libsql::Builder;

// Local database
let db = Builder::new_local("tasks.db").build().await?;
let db: &'static _ = Box::leak(Box::new(db));

let storage = LibsqlStorage::<MyTask, _>::new(db);
storage.setup().await?;
```

## Status

MVP complete. Ready for integration testing with Turso Cloud.

## License

MIT
