# QueueCTL

A durable, CLI-driven background job queue backed by **SQLite** and **Python multiprocessing**.

QueueCTL is a minimal, correct implementation of what Sidekiq, Celery, or AWS SQS + Lambda do in production: jobs are enqueued, claimed atomically by worker processes, executed as shell commands, retried with exponential backoff on failure, and permanently-failed jobs are quarantined in a Dead Letter Queue rather than silently lost.

---

## Table of Contents

1. [Features](#features)
2. [Architecture](#architecture)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [CLI Reference](#cli-reference)
   - [enqueue](#enqueue)
   - [status](#status)
   - [list](#list)
   - [worker start](#worker-start)
   - [dlq](#dlq)
   - [config](#config)
6. [Configuration Reference](#configuration-reference)
7. [Running the Tests](#running-the-tests)
8. [Design Decisions](#design-decisions)
9. [Future Work](#future-work)

---

## Features

| Feature | Detail |
|---------|--------|
| **Atomic claim** | `UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *` — SQLite serialises writes so two workers can never claim the same job |
| **Exponential backoff** | `delay = backoff_base ^ attempts`, capped at 300 s — configurable |
| **Dead Letter Queue** | Jobs that exhaust retries move to `dead` state; operator can revive with `dlq retry` |
| **Crash recovery** | On startup, workers reset orphaned `processing` jobs to `pending`; jobs owned by live sibling workers are skipped — no false resets in multi-worker deployments |
| **Graceful shutdown** | `SIGINT` / `Ctrl-C` finishes the current job before exiting — no mid-execution kills |
| **Job timeout** | Subprocess killed after `timeout_seconds`; counts as a failure attempt |
| **Configurable** | `max_retries`, `backoff_base`, `timeout_seconds`, `poll_interval_ms` stored in the DB |
| **168 tests** | Unit + multiprocessing stress tests — zero double-claims under 2× worker contention |

---

## Architecture

```
Operators
    │
    ▼  queuectl <command>
┌──────────────────────────┐
│         cli.py           │  thin adapter: parse → call service → format output
└──────────────────────────┘
    │
    ▼
┌──────────────────────────┐     ┌──────────────┐
│          core/           │     │  config.py   │
│  job.py  job_service.py  │     │  (DB config  │
│  retry.py  worker.py     │◄────│   wrapper)   │
│  exceptions.py           │     └──────────────┘
└──────────────────────────┘
    │
    ▼
┌──────────────────────────┐
│        storage/          │
│  db.py     queries.py    │
└──────────────────────────┘
    │
    ▼
┌──────────────────────────┐
│      queuectl.db         │  SQLite, WAL mode, busy_timeout = 5 s
│  ┌───────┐  ┌────────┐   │
│  │ jobs  │  │ config │   │
│  └───────┘  └────────┘   │
└──────────────────────────┘
```

**Job state machine:**

```
enqueue()     claim_job()          mark_complete()
  [*] ──► pending ──────────► processing ──────────────► completed ──► [*]
                                    │
                      mark_failed() │
                                    ▼
                                 failed
                               /         \
              attempts < max   │         │  attempts >= max
                               ▼         ▼
               schedule_retry()         move_to_dlq()
              (next_run_at =             state = dead ──► [*]
               now + base^attempts)          │
                    │                        │ dlq_retry() (operator)
                    └─────── pending ◄───────┘
```

For full Mermaid diagrams (sequence diagrams, module dependency graph, file map):
→ [`ARCHITECTURE.md`](ARCHITECTURE.md)

---

## Installation

**Requirements:** Python 3.10+, no external dependencies beyond `click`.

```bash
# 1. Clone
git clone https://github.com/iussg/QueueCTL.git
cd QueueCTL/queuectl

# 2. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install
pip install -e .
```

Verify:

```bash
queuectl --help
```

**Database location** — defaults to `~/.queuectl/queuectl.db`.
Override with an environment variable:

```bash
# Linux / macOS
export QUEUECTL_DB_PATH=/path/to/my/queue.db

# Windows PowerShell
$env:QUEUECTL_DB_PATH = "C:\path\to\my\queue.db"
```

---

## Quick Start

**1. Enqueue a job:**

```bash
queuectl enqueue '{"id": "job-001", "command": "echo hello world", "max_retries": 3}'
# Enqueued  id='job-001'  command='echo hello world'  max_retries=3  state=pending
```

**2. Start a worker:**

```bash
queuectl worker start
# Starting 1 worker (pid=12345)...  Press Ctrl+C to stop.
# [INFO] ... Worker worker-12345 starting ...
# [INFO] ... Worker worker-12345 claimed job job-001 (attempt 1/3)
# [INFO] ... Job succeeded | worker=worker-12345 job_id=job-001
```

**3. Check the result:**

```bash
queuectl status
#   Pending        : 0
#   Processing     : 0
#   Completed      : 1
#   Failed         : 0
#   Dead (DLQ)     : 0

queuectl list --state completed
# id=job-001  state=completed  exit_code=0  attempts=1  ...
```

---

## CLI Reference

### enqueue

Add a new job to the queue.

```bash
queuectl enqueue '<JSON>'
```

**JSON fields:**

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `id` | ✅ | — | Unique job identifier (string) |
| `command` | ✅ | — | Shell command to execute |
| `max_retries` | ❌ | `3` | Maximum failure attempts before DLQ |

```bash
# Minimal
queuectl enqueue '{"id": "job-1", "command": "python process.py"}'

# With custom retry limit
queuectl enqueue '{"id": "job-2", "command": "curl https://api.example.com/sync", "max_retries": 5}'

# Duplicate ID → rejected with a clear error (never overwrites existing jobs)
queuectl enqueue '{"id": "job-1", "command": "echo duplicate"}'
# Error: Job with id 'job-1' already exists.
```

---

### status

Show a live count of jobs in each state.

```bash
queuectl status
```

```
  Pending        : 4
  Processing     : 2
  Completed      : 17
  Failed         : 1
  Dead (DLQ)     : 0
```

---

### list

List jobs, optionally filtered by state.

```bash
queuectl list [--state STATE]
```

**States:** `pending`, `processing`, `completed`, `failed`, `dead`

```bash
queuectl list                      # all jobs
queuectl list --state pending      # only pending
queuectl list --state failed       # investigate failures
```

`stdout` / `stderr` are truncated to 200 characters in list output.

---

### worker start

Start one or more background worker processes.

```bash
queuectl worker start [--count N]
```

```bash
# Single worker — runs in the foreground
queuectl worker start

# Four parallel workers
queuectl worker start --count 4
# Starting 4 workers...  Press Ctrl+C to stop all.
#   Started worker-12345-0  (pid=12346)
#   Started worker-12345-1  (pid=12347)
#   ...
```

**How workers coordinate:**
Each worker independently polls using the atomic claim query.
SQLite's write serialisation ensures two workers can never claim the same job —
no application-level lock is needed.

**Graceful shutdown:**
Press `Ctrl+C`. The signal handler sets a flag; each worker finishes its
current job and exits cleanly. The DB is always left in a consistent state.

**Crash recovery:**
On startup, each worker automatically resets any `processing` jobs left by
a previously crashed process back to `pending`. No job is ever permanently lost.

---

### dlq

Manage the Dead Letter Queue.

```bash
# List all dead jobs
queuectl dlq list

# Revive a dead job (resets attempts=0, state=pending — a full fresh cycle)
queuectl dlq retry <JOB_ID>
```

```bash
queuectl dlq list
# id=job-99  state=dead  exit_code=1  attempts=3/3  command=curl https://broken-api...

# After investigating and fixing the root cause:
queuectl dlq retry job-99
# Job 'job-99' re-queued  state=pending  attempts=0
```

---

### config

View and update runtime configuration. Values are stored in the DB and take
effect on the next worker restart.

```bash
queuectl config get <KEY>
queuectl config set <KEY> <VALUE>
```

**Supported keys:**

| Key | Default | Constraint | Effect |
|-----|---------|------------|--------|
| `max-retries` | `3` | integer ≥ 0 | Max failures before DLQ |
| `backoff-base` | `2` | number ≥ 1.0 | Exponential base: `delay = base^attempts` |
| `timeout-seconds` | `300` | integer ≥ 1 | Subprocess kill timeout |
| `poll-interval-ms` | `500` | integer ≥ 100 | Worker idle sleep between polls |

```bash
queuectl config set max-retries 5
queuectl config set backoff-base 1.5
queuectl config set poll-interval-ms 100
queuectl config set timeout-seconds 60

queuectl config get max-retries
#   max-retries = 5
```

---

## Configuration Reference

```
Default DB path:  ~/.queuectl/queuectl.db
Override:         QUEUECTL_DB_PATH environment variable

Config table defaults (seeded at first run, overridable via `config set`):
  max_retries      = 3
  backoff_base     = 2
  timeout_seconds  = 300
  poll_interval_ms = 500

Backoff formula:  delay = min(backoff_base ^ attempts, 300 s)

  attempt 1  →   2 s
  attempt 2  →   4 s
  attempt 3  →   8 s
  attempt 8  → 256 s
  attempt 9  → 300 s  (capped)
```

---

## Running the Tests

```bash
# Full suite
python -m pytest -v
```

```
============================= 168 passed in 6.74s ==============================
```

**Per-phase breakdown:**

| File | Tests | What it proves |
|------|-------|----------------|
| `test_schema.py` | 21 | Schema integrity, WAL mode, idempotent init |
| `test_job_service.py` | 58 | Service layer, atomic claim (single-process), FIFO ordering |
| `test_retry.py` | 53 | Backoff formula, DLQ movement, config CRUD |
| `test_worker.py` | 31 | Subprocess execution, crash recovery, timeout path |
| `test_concurrency.py` | 5 | **Multiprocessing stress** — no double-claims under real contention |

**Run individual suites:**

```bash
python -m pytest tests/test_schema.py -v
python -m pytest tests/test_job_service.py -v
python -m pytest tests/test_retry.py -v
python -m pytest tests/test_worker.py -v
python -m pytest tests/test_concurrency.py -v
```

**What the concurrency tests prove:**

`test_concurrency.py` spawns real OS processes sharing one SQLite file.
Three contention ratios are verified:

| Scenario | Workers | Jobs | Contention |
|----------|---------|------|------------|
| Equal | 10 | 10 | 1 worker per job |
| High pressure | 20 | 10 | 2 workers racing per job |
| Sequential | 3 | 15 | each worker claims ~5 jobs |

**Result in every run: zero double-claims.** This is the empirical proof that
SQLite's `UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *` with WAL mode
and `busy_timeout = 5000 ms` provides the concurrency guarantee without any
application-level locking.

---

## Design Decisions

### Why SQLite?

SQLite's write serialisation is the only synchronisation primitive needed.
A single `UPDATE…RETURNING` is atomic at the DB-engine level, so no
application-level mutex or Redis `SETNX` is required. WAL mode allows
concurrent readers (`status`, `list`) without blocking writers (workers).
The entire queue is a single portable file — zero infrastructure to manage.

### Atomic claim query

```sql
UPDATE jobs
SET    state     = 'processing',
       worker_id = ?,
       picked_at = ?,
       updated_at = ?
WHERE  id = (
    SELECT id FROM jobs
    WHERE  state = 'pending'
    AND    (next_run_at IS NULL OR next_run_at <= ?)
    ORDER  BY created_at ASC   -- FIFO
    LIMIT  1
)
RETURNING *;
```

There is no two-step read-then-update; the subquery and update execute as one
atomic operation that SQLite serialises. If two workers submit it simultaneously,
one wins and the other returns 0 rows. No application-level lock is needed.

### `picked_at` ≠ `started_at`

`picked_at` — set by the claim query (when the DB row was locked).
`started_at` — set just before `subprocess.run` (when the process actually launches).

The gap between them captures scheduling overhead, which is useful for debugging
slow worker startup or DB contention under load.

### All SQL in `storage/queries.py`

Every SQL string lives in one module. No raw SQL is scattered across service or
worker files. This makes the query surface reviewable, optimisable, and testable
in isolation without touching business logic.

### Config in the DB, not a file

Operators change config with `queuectl config set` — the same tool used for
everything else. Config changes are durable (survive restarts), consistent with
the rest of the data, and cannot drift out of sync with the application.

### Dependency direction (never violated)

```
cli.py  →  core/*  →  storage/*
config.py           →  storage/*
```

`core/` never imports from `cli.py`. `storage/` never imports from `core/`.
Every module in `core/` and `storage/` is fully testable without a click
invocation — this is what makes the 168 unit tests fast and deterministic.

---

## Future Work

The following are explicitly out of scope for this version but are the natural
next steps:

- **Distributed / multi-node** — SQLite cannot be shared across hosts. A real
  broker (Redis, RabbitMQ, SQS) would replace the `storage/` layer; `core/`
  and `cli.py` would need minimal changes because the dependency direction is clean.
- **Web dashboard** — `status` and `list` queries already have all the data;
  a Flask/FastAPI layer on top is additive.
- **Job priority** — add a `priority INTEGER` column, change `ORDER BY created_at`
  to `ORDER BY priority DESC, created_at ASC` in the claim query. One line change.
- **Per-attempt log history** — a `job_execution_logs` table (one row per attempt,
  indexed on `job_id`) would preserve output across retries without overwriting.
- **Scheduled / cron jobs** — `next_run_at` is already implemented; a `cron_expression`
  column and a scheduler process is the natural extension.

---

## Repository Layout

```
queuectl/
├── cli.py                  Entrypoint — all click commands
├── config.py               Config table CRUD and CLI↔DB key normalisation
├── seed_jobs.py            Demo seeder — enqueues sample jobs via the service layer
│                           (bypasses shell quoting; useful for quick manual testing)
├── simulate_crash.py       Injects an orphaned 'processing' job to demonstrate
│                           crash recovery without needing to kill a real worker
├── ARCHITECTURE.md         Full architecture diagrams and design log
├── core/
│   ├── exceptions.py       Domain exceptions (never expose raw sqlite3 errors)
│   ├── job.py              Job dataclass + state machine constants
│   ├── job_service.py      enqueue, claim_job (atomic), mark_complete, mark_failed
│   ├── retry.py            calculate_backoff, schedule_retry, move_to_dlq, dlq_retry
│   └── worker.py           Subprocess execution loop, sibling-aware crash recovery,
│                           SIGINT handler
├── storage/
│   ├── db.py               Connection factory (WAL, row_factory, busy_timeout=5000)
│   └── queries.py          All SQL — single source of truth for every query
└── tests/
    ├── conftest.py          tmp_db and tmp_db_path fixtures
    ├── test_schema.py       Phase 1 — 21 tests
    ├── test_job_service.py  Phase 2 — 58 tests
    ├── test_retry.py        Phase 3 — 53 tests
    ├── test_worker.py       Phase 4 — 31 tests
    └── test_concurrency.py  Phase 5 —  5 multiprocessing stress tests
```

---

*Built as a Backend Internship Assignment demonstrating durable job queuing,
atomic concurrency without application locks, and production-grade Python
engineering practices.*
