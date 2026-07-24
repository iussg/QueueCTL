"""
storage/queries.py — ALL raw SQL for QueueCTL lives here.

No other module may contain SQL strings.  Every query is a named
module-level constant so callers reference SQL by a descriptive name,
never by an inline string.  This makes schema changes a single-file
concern and keeps SQL out of business logic.
"""

# ---------------------------------------------------------------------------
# DDL — Schema creation
# All statements use IF NOT EXISTS so they are safe to run on every startup.
# ---------------------------------------------------------------------------

CREATE_JOBS_TABLE: str = """
    CREATE TABLE IF NOT EXISTS jobs (
        id          TEXT    PRIMARY KEY,
        command     TEXT    NOT NULL,
        state       TEXT    NOT NULL
                            CHECK(state IN ('pending','processing','completed','failed','dead')),
        attempts    INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        next_run_at TEXT,               -- ISO-8601 UTC; NULL or <= now means eligible to claim
        worker_id   TEXT,               -- PID/name of the worker that currently owns this job
        exit_code   INTEGER,            -- most recent subprocess exit code
        stdout      TEXT,               -- most recent stdout, truncated to 5 000 chars
        stderr      TEXT,               -- most recent stderr, truncated to 5 000 chars
        created_at  TEXT    NOT NULL,
        picked_at   TEXT,               -- when a worker atomically claimed the job
        started_at  TEXT,               -- when subprocess execution actually began
        finished_at TEXT,               -- when execution ended (success, failure, or timeout)
        updated_at  TEXT    NOT NULL
    )
"""

CREATE_CONFIG_TABLE: str = """
    CREATE TABLE IF NOT EXISTS config (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
"""

# Composite index on the two columns used by the atomic claim query.
# This avoids a full table scan on every worker poll iteration.
CREATE_JOBS_INDEX: str = """
    CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run
        ON jobs (state, next_run_at)
"""

# ---------------------------------------------------------------------------
# Config seeding — INSERT OR IGNORE so re-runs never overwrite user values.
# ---------------------------------------------------------------------------

SEED_DEFAULT_CONFIG: list[str] = [
    "INSERT OR IGNORE INTO config (key, value) VALUES ('max_retries',     '3')",
    "INSERT OR IGNORE INTO config (key, value) VALUES ('backoff_base',    '2')",
    "INSERT OR IGNORE INTO config (key, value) VALUES ('timeout_seconds', '300')",
    "INSERT OR IGNORE INTO config (key, value) VALUES ('poll_interval_ms','500')",
]

# ---------------------------------------------------------------------------
# DML — Job lifecycle queries
# ---------------------------------------------------------------------------

# params: (id, command, max_retries, created_at, updated_at)
INSERT_JOB: str = """
    INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
    VALUES (?, ?, 'pending', 0, ?, ?, ?)
"""

# params: (job_id,)
SELECT_JOB_BY_ID: str = "SELECT * FROM jobs WHERE id = ?"

# params: ()
SELECT_ALL_JOBS: str = "SELECT * FROM jobs ORDER BY created_at ASC"

# params: (state,)
SELECT_JOBS_BY_STATE: str = """
    SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC
"""

# params: ()
SELECT_JOB_COUNTS: str = """
    SELECT state, COUNT(*) AS count FROM jobs GROUP BY state
"""

# The atomic claim query — core concurrency guarantee.
# SQLite serializes writes, so two workers issuing this UPDATE concurrently
# will NOT both match the same row.  The second one's subquery simply won't
# find that row anymore once the first commit lands.
# params: (worker_id, picked_at, updated_at, now_for_comparison)
CLAIM_JOB: str = """
    UPDATE jobs
    SET
        state      = 'processing',
        worker_id  = ?,
        picked_at  = ?,
        updated_at = ?
    WHERE id = (
        SELECT id FROM jobs
        WHERE state = 'pending'
          AND (next_run_at IS NULL OR next_run_at <= ?)
        ORDER BY created_at ASC
        LIMIT 1
    )
    RETURNING *
"""

# params: (exit_code, stdout, stderr, finished_at, updated_at, job_id)
UPDATE_JOB_COMPLETE: str = """
    UPDATE jobs
    SET
        state       = 'completed',
        exit_code   = ?,
        stdout      = ?,
        stderr      = ?,
        finished_at = ?,
        updated_at  = ?
    WHERE id = ?
"""

# params: (exit_code, stdout, stderr, finished_at, updated_at, job_id)
# RETURNING * so callers can inspect the updated attempts count immediately.
UPDATE_JOB_FAILED: str = """
    UPDATE jobs
    SET
        state       = 'failed',
        attempts    = attempts + 1,
        exit_code   = ?,
        stdout      = ?,
        stderr      = ?,
        finished_at = ?,
        updated_at  = ?
    WHERE id = ?
    RETURNING *
"""

# ---------------------------------------------------------------------------
# DML — Phase 3 (retry + DLQ + config)
# ---------------------------------------------------------------------------

# params: (updated_at, job_id)
UPDATE_JOB_DEAD: str = """
    UPDATE jobs
    SET state = 'dead', updated_at = ?
    WHERE id = ?
"""

# params: (next_run_at, updated_at, job_id)
UPDATE_JOB_RETRY: str = """
    UPDATE jobs
    SET
        state       = 'pending',
        next_run_at = ?,
        updated_at  = ?
    WHERE id = ?
"""

# params: (key,)
SELECT_CONFIG_VALUE: str = "SELECT value FROM config WHERE key = ?"

# params: (key, value)
UPSERT_CONFIG_VALUE: str = """
    INSERT INTO config (key, value) VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
"""

# params: ()
SELECT_ALL_CONFIG: str = "SELECT key, value FROM config ORDER BY key ASC"

# ---------------------------------------------------------------------------
# DML — Phase 4 (worker / crash recovery)
# ---------------------------------------------------------------------------

# Identifies jobs orphaned by a crashed worker (startup recovery scan).
# params: ()
SELECT_PROCESSING_JOBS: str = "SELECT * FROM jobs WHERE state = 'processing'"

# Resets orphaned processing jobs to pending so they can be reclaimed.
# params: (updated_at,)
RESET_ORPHANED_JOBS: str = """
    UPDATE jobs
    SET
        state     = 'pending',
        worker_id = NULL,
        updated_at = ?
    WHERE state = 'processing'
"""

# Records the exact moment subprocess execution begins (distinct from picked_at).
# params: (started_at, updated_at, job_id)
UPDATE_JOB_STARTED: str = """
    UPDATE jobs
    SET started_at = ?, updated_at = ?
    WHERE id = ?
"""
