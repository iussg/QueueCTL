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
# DML — populated in later phases; declared here to keep the module complete.
# ---------------------------------------------------------------------------

# Phase 2: job_service.py will use:
#   INSERT_JOB, SELECT_JOB_BY_ID, CLAIM_JOB, UPDATE_JOB_COMPLETE,
#   UPDATE_JOB_FAILED, UPDATE_JOB_DEAD, SELECT_JOBS_BY_STATE,
#   SELECT_ALL_JOBS, SELECT_PROCESSING_JOBS (crash recovery)

# Phase 3: config.py will use:
#   SELECT_CONFIG_VALUE, UPSERT_CONFIG_VALUE, SELECT_ALL_CONFIG

# Phase 4: worker.py will use:
#   RESET_ORPHANED_JOBS (crash recovery on startup)
