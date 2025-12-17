-- Workers table
CREATE TABLE IF NOT EXISTS Workers (
    id TEXT PRIMARY KEY NOT NULL,
    worker_type TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    layers TEXT,
    last_seen INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE INDEX IF NOT EXISTS WTIdx ON Workers(worker_type);
CREATE INDEX IF NOT EXISTS LSIdx ON Workers(last_seen);

-- Jobs table
CREATE TABLE IF NOT EXISTS Jobs (
    job BLOB NOT NULL,              -- serialized task data (blob)
    id TEXT PRIMARY KEY NOT NULL,   -- task ID (ULID)
    job_type TEXT NOT NULL,         -- queue name
    status TEXT NOT NULL DEFAULT 'Pending',  -- Pending, Queued, Running, Done, Failed
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25,
    run_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    last_error TEXT,                -- serialized error result
    lock_at INTEGER,
    lock_by TEXT,
    done_at INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,  -- task priority
    metadata TEXT,                  -- additional metadata (JSON)
    FOREIGN KEY(lock_by) REFERENCES Workers(id)
);

CREATE INDEX IF NOT EXISTS SIdx ON Jobs(status);
CREATE INDEX IF NOT EXISTS LIdx ON Jobs(lock_by);
CREATE INDEX IF NOT EXISTS JTIdx ON Jobs(job_type);