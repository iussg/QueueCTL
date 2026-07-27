# QueueCTL — Engineering Design Document

## 1. System Overview

**What you're building:** a durable, CLI-driven background job queue — a minimal, single-node version of what Sidekiq, Celery, or AWS SQS + Lambda do in production. Jobs are enqueued, claimed by worker processes, executed as shell commands, retried on failure with exponential backoff, and permanently-failed jobs are quarantined in a Dead Letter Queue instead of being lost or retried forever.

**Why this class of system exists:** any system that does slow or unreliable work (sending email, calling an external API, processing a file) needs to decouple "accept the request" from "do the work," survive crashes without losing work, and stop retrying things that will never succeed. This is the same problem Sidekiq/Celery/SQS solve — you're building a small, correct version of it.

**Core engineering problem being tested:** not "can you call subprocess.run()" — it's "can you make concurrent workers safely share one source of truth without double-processing or losing jobs." That's the whole assignment in one sentence.

---

## 2. Requirements

**Core (graded, must work):**
- Enqueue jobs with a spec (id, command, state, attempts, max_retries, timestamps)
- Run N worker processes in parallel, each claiming and executing jobs
- Exponential backoff retry: `delay = base ^ attempts`
- Move to DLQ after `max_retries` exhausted
- Persist across restarts (SQLite)
- Full CLI surface: enqueue, worker start/stop, status, list, dlq list/retry, config set

**Supporting:**
- Graceful shutdown (finish current job, don't kill mid-execution)
- Config management for retry count / backoff base
- Clean error messages, not stack traces, for user-facing CLI failures

**Explicitly out of scope for this assignment** (mention in README as "future work," don't build): distributed multi-node workers, a web dashboard, job priority queues, authentication. Building these would burn days you don't have and isn't what's being graded — the rubric weights functionality (40%) and robustness (20%), not feature breadth.

---

## 3. Architecture

```
┌─────────────┐
│   CLI Layer │  (argparse/click — parses commands, calls into core, formats output)
└──────┬──────┘
       │
┌──────▼──────────────┐
│   Job Service Layer  │  (enqueue, claim_job, mark_complete, mark_failed, move_to_dlq)
└──────┬───────────────┘
       │
┌──────▼──────────────┐
│  Persistence Layer    │  (SQLite connection, schema, atomic claim queries)
└──────┬───────────────┘
       │
┌──────▼──────────────┐
│   queuectl.db (SQLite)│
└───────────────────────┘

┌─────────────┐        ┌─────────────┐        ┌─────────────┐
│  Worker #1  │        │  Worker #2  │        │  Worker #3  │   (separate OS processes)
└──────┬──────┘        └──────┬──────┘        └──────┬──────┘
       │  each polls Job Service Layer independently  │
       └────────────────────┴────────────────────────┘
                             │
                    all share the same SQLite file
```

**Why processes, not threads, for workers:** Python's GIL makes threads a poor fit for anything that isn't I/O-bound waiting; `subprocess.run` for arbitrary shell commands is exactly the case where separate OS processes give you real isolation — one worker crashing on a bad command can't take down the others or corrupt shared in-memory state. Use `multiprocessing`.

**Job lifecycle (state machine):**

```
pending ──(worker claims)──► processing ──(exit code 0)──► completed
                                  │
                                  └──(exit code ≠ 0 / not found)──► failed
                                                                       │
                                                    attempts < max_retries?
                                                    ├─ yes ──► pending (after backoff delay)
                                                    └─ no  ──► dead (DLQ)
```

Invalid transitions to guard against: `completed → anything`, `dead → processing` (must go through explicit `dlq retry`, which resets to `pending` with `attempts=0`, per the finalized DLQ retry decision in Section 11).

**Worker lifecycle:** `starting → polling → claiming → executing → polling (loop) → shutting_down (on SIGINT/SIGTERM, finish current job first) → stopped`.

---

## 4. Folder Structure

```
queuectl/
├── cli.py              # entrypoint; argparse/click command definitions only — no business logic
├── core/
│   ├── job.py           # Job dataclass/model + state transition validation
│   ├── job_service.py    # enqueue, claim_job (the atomic claim query), mark_complete/failed
│   ├── retry.py          # backoff calculation, DLQ movement logic
│   └── worker.py         # worker process loop: poll → claim → execute → update
├── storage/
│   ├── db.py             # connection management, schema creation/migration
│   └── queries.py        # all raw SQL isolated here — nowhere else touches SQL directly
├── config.py             # load/save config (retry count, backoff base) — stored in DB, not a file
├── tests/
│   ├── test_job_service.py
│   ├── test_concurrency.py   # the stress test — multiple workers, no double-claim
│   └── test_retry.py
├── README.md
├── design.md              # optional architecture doc (this document, trimmed)
└── requirements.txt
```

**Dependency direction:** `cli.py` depends on `core/`, `core/` depends on `storage/`, nothing depends back upward. This means you can unit-test `core/` and `storage/` without touching the CLI at all — and it's the answer to "why is it structured this way" in the interview.

---

## 5. Database Schema

```sql
CREATE TABLE jobs (
    id          TEXT PRIMARY KEY,
    command     TEXT NOT NULL,
    state       TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
    attempts    INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    next_run_at TEXT,              -- ISO timestamp; NULL or <= now means eligible to claim
    worker_id   TEXT,              -- which worker currently owns/owned this job (NULL if never claimed)
    exit_code   INTEGER,           -- most recent execution's exit code
    stdout      TEXT,              -- most recent execution's stdout, truncated to ~5000 chars
    stderr      TEXT,              -- most recent execution's stderr, truncated to ~5000 chars
    created_at  TEXT NOT NULL,
    picked_at   TEXT,              -- when a worker claimed this job (most recent attempt)
    started_at  TEXT,              -- when subprocess execution actually began
    finished_at TEXT,              -- when subprocess execution ended (success, failure, or timeout)
    updated_at  TEXT NOT NULL
);

CREATE INDEX idx_jobs_state_next_run ON jobs(state, next_run_at);  -- speeds up the claim query

CREATE TABLE config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```
**Typical configuration entries stored in the `config` table:**

| Key | Example Value | Purpose |
|------|--------------|---------|
| `max_retries` | `3` | Default maximum retry attempts for newly enqueued jobs |
| `backoff_base` | `2` | Base used for exponential backoff (`base^attempts`) |
| `timeout_seconds` | `300` | Maximum execution time before a job is treated as timed out |
| `poll_interval_ms` | `500` | Delay between worker polling iterations when no eligible jobs exist |

**Why `next_run_at` matters:** this is how you implement backoff delay without a separate scheduler — a worker only claims a `pending` job where `next_run_at <= now`. On failure, you don't immediately re-set it to `pending`; you set `next_run_at = now + delay` and state stays `pending` (or a transient `waiting` state if you want to distinguish "ready now" from "ready later" — either is defensible, just document your choice).

**Why `worker_id`:** lets `status` show which worker owns which job, and makes the claim query's atomicity visible/debuggable.

**Why `exit_code`/`stdout`/`stderr` on the job itself:** not required by the assignment, but means you can answer "why did Job17 fail?" with the actual stderr instead of "I don't know" — cheap to add, high value in the live interview. Truncate before writing (~5000 chars) so a runaway command can't bloat the database. This is the simple version; if you finish early (Day 4+), the optional upgrade is a separate `job_execution_logs` table (`id, job_id, attempt, stdout, stderr, exit_code, started_at, finished_at, duration`, indexed on `job_id`) so every retry attempt's output is preserved instead of only the latest one overwriting the last. Don't build this on Day 1 — it's a clean, isolated addition later if time allows.

**Why `picked_at`/`started_at`/`finished_at` instead of just `updated_at`:** lets you compute `duration = finished_at - started_at` per execution, which is useful for debugging slow jobs and is a natural thing to surface in `status`. In practice `picked_at` and `started_at` will often be near-identical since claim and execution happen back-to-back in the same worker loop iteration — that's fine, keep both anyway since they're conceptually distinct events.

**Enable WAL mode** (`PRAGMA journal_mode=WAL`) — this is a one-line change that meaningfully improves concurrent read/write behavior in SQLite and is worth mentioning in the interview as a deliberate choice, not a default you didn't think about.

---

## 6. Concurrency Strategy (highest-weight section — this is what gets you disqualified or not)

**The claim query must be a single atomic statement**, not a SELECT followed by an UPDATE:

```sql
UPDATE jobs
SET state = 'processing', worker_id = ?, updated_at = ?
WHERE id = (
    SELECT id FROM jobs
    WHERE state = 'pending' AND (next_run_at IS NULL OR next_run_at <= ?)
    ORDER BY created_at
    LIMIT 1
)
RETURNING *;
```

This works because SQLite serializes writes — two workers issuing this UPDATE concurrently will not both match and claim the same row; the second one's subquery will simply not find that row anymore once the first commits. This is the difference between "I added a lock" (which people often get subtly wrong) and "I made the claim itself atomic" (which is provably correct). **Say this exact reasoning in the interview.**

**Do NOT** use an application-level `threading.Lock` or `multiprocessing.Lock` for this — it doesn't work across separate OS processes reliably the way you'd want, and it's the wrong abstraction layer. The database is your source of truth and your lock, simultaneously.

**Deadlock/contention handling:** set `PRAGMA busy_timeout` (e.g. 5000ms) so that if two workers do hit a write conflict, SQLite retries the lock acquisition instead of immediately throwing `database is locked`.

**Stress test you must actually run, not just claim you did:** enqueue 20+ jobs, start 5 workers, log every claim with worker_id and job_id, and grep the log afterward to prove no job_id appears claimed by two workers. This is your evidence for the "robustness" 20% of the grade.

---

## 7. Retry & Persistence

- **Backoff:** `delay_seconds = backoff_base ** attempts`, capped at a sane max (e.g. 300s) so a misconfigured base doesn't produce day-long delays.
- **DLQ movement:** on failure, if `attempts >= max_retries`, set `state = 'dead'` instead of rescheduling. `dlq retry <id>` resets `state = 'pending'`, `attempts = 0`, `next_run_at = NULL`.
- **Crash recovery:** During `queuectl worker start`, **before the worker polling loop begins**, perform a one-time recovery scan of the database. Any job left in the `processing` state is assumed to have been orphaned by an unexpected worker crash or system shutdown and is reset to `pending`, with the recovery event logged. This recovery process runs **exactly once during startup**, not during every polling iteration. A crashed worker does not necessarily mean the job itself failed, so returning it to `pending` allows it to be safely reclaimed and executed again instead of remaining permanently stuck.
- **Job ordering (FIFO):** claim query orders by `created_at ASC` — oldest pending job first. Chosen over random (unpredictable, hard to test) or newest-first (starves old jobs indefinitely under load). Fair, predictable, and trivial to explain in the interview.
- **Command timeout:** pass `timeout=X` (configurable, default e.g. 300s) to `subprocess.run`. On `subprocess.TimeoutExpired`, kill the process, record `stderr = "timed out after Xs"`, and treat it exactly like any other failed exit — it flows into the existing retry → DLQ path with zero special-case logic. This is one of the assignment's named bonus features and costs about 20-30 minutes to implement, so build it rather than deferring it.
- **Graceful shutdown:** on SIGINT/SIGTERM, set a flag checked between claim-loop iterations; finish the currently-executing job, then exit — don't kill the subprocess mid-run.

---

## 8. CLI Design

```
queuectl enqueue '{"id":"job1","command":"sleep 2"}'
queuectl worker start --count 3
queuectl worker stop
queuectl status
queuectl list --state pending
queuectl dlq list
queuectl dlq retry job1
queuectl config set max-retries 3
queuectl config set backoff-base 2
```

Validate input at the CLI boundary (malformed JSON, missing required fields) and fail with a clear message — don't let a bad `enqueue` call crash with a raw traceback. This maps directly to the "invalid commands fail gracefully" test scenario they listed.

### `queuectl worker stop` — full architecture

- Workers run as separate `multiprocessing` processes; the main CLI process keeps a record of each worker's PID (e.g. in a small runtime state file or table) when `worker start` spawns them.
- `queuectl worker stop` sends SIGTERM (falling back to SIGINT-style handling if needed) to every tracked worker PID — it does not kill them outright.
- Workers do **not** terminate immediately on receiving the signal. Each worker checks a shutdown flag between claim-loop iterations:
  - If a worker is idle (no job currently claimed), it exits immediately on the next check.
  - If a worker is mid-job, it finishes executing the current job, updates that job's final state in SQLite, and only then exits.
- `queuectl worker stop` blocks and waits until every tracked worker process has actually exited (polling PID liveness or joining the processes) before returning control to the user — so the command's completion is a guarantee, not an assumption.
- **Why this design:** the alternative — killing workers outright — risks leaving a job stuck in `processing` with partially-applied side effects and no clean exit code recorded, which directly undermines the "no job left half-executed" requirement and the crash-recovery story you're already relying on elsewhere. Treating `stop` as cooperative rather than forceful means a normal shutdown never depends on crash recovery to clean up after it — crash recovery stays reserved for actual crashes.

### `queuectl status` — what it displays

Example output:

```
Workers Active : 3
Workers Idle   : 1

Pending        : 5
Processing     : 2
Completed      : 18
Failed         : 1
Dead           : 0
```

Optionally also surface: database path, queue uptime, and average execution duration (`finished_at - started_at` averaged over completed jobs) if you have time to add it.

`status` is intentionally **read-only** — it only queries the `jobs` and `config` tables and never writes to them. This matters for the interview: a status/monitoring command mutating state it's supposed to be reporting on would be a design smell, and being able to say "status never writes" is a clean, correct answer if asked.

---

## 9. Testing Strategy

| Test type | What it proves |
|---|---|
| Unit — job_service | enqueue/claim/complete/fail logic correct in isolation |
| Unit — retry | backoff calculation and DLQ threshold correct |
| Concurrency stress test | no double-claim under real parallel load (the one that matters most) |
| Persistence test | kill process mid-run, restart, jobs still there in correct state |
| CLI smoke test | each command runs and produces expected output/exit code |

You don't need a huge suite — testing is only 10% of the grade — but the concurrency test specifically should exist and be runnable, because it's your proof against the #1 disqualifier.

---

## Logging Strategy

Use Python's built-in `logging` module rather than raw `print` statements — this costs nothing extra to set up and pays off heavily in debugging and in the live demo.

**Log levels:**
- `INFO` — normal lifecycle events (worker started, job claimed, job completed, retry scheduled)
- `WARNING` — recoverable issues (timeout occurred)
- `ERROR` — job permanently failed / moved to DLQ, crash recovery had to run

**Every log entry includes:** timestamp, worker ID, job ID (when applicable), and a clear event description — not just a bare message.

**Events to log:**
- Worker started
- Worker stopped
- Job claimed
- Job completed
- Job failed
- Retry scheduled
- Timeout occurred
- Job moved to DLQ
- Crash recovery executed

Example line: `[INFO] 2026-07-23T10:15:03Z Worker-2 Job17 attempt=2 claimed`

**Why this matters:** structured logging is what makes your Section 6 concurrency stress test verifiable — you prove no double-claim happened by grepping exactly this log, not by assuming it. It also turns your live demo into "watch the workers coordinate in real time" instead of a silent CLI, which is a stronger interview impression for very little added complexity.

---

## 10. Edge Cases Checklist

- ✔ Duplicate job ID on enqueue → rejected with a clear CLI error.
      Reason: Job IDs represent immutable units of work. Rejecting duplicates preserves data integrity, prevents accidental overwrites, and avoids modifying jobs that may already be pending, processing, or completed.
- ✔ Worker crashes mid-job → job stuck in `processing` → recovered on next startup
- ✔ SQLite locked / busy → handled via `busy_timeout`, not a crash
- ✔ Empty queue → workers poll idly without busy-looping the CPU. Default polling interval: 500ms — if no eligible pending job is found, a worker sleeps 500ms before polling again. This is intentionally conservative: responsiveness stays excellent for a CLI tool while idle CPU usage drops dramatically compared to a tight poll loop. Can later become a configurable value via the `config` table if desired.
- ✔ Invalid/nonexistent command → exit code captured, triggers retry like any other failure
- ✔ SIGINT during active job execution → finishes job, then exits
- ✔ Multiple `worker start` calls → don't spawn duplicate worker pools untracked

---

## 11. Documentation Plan

**README.md** (per their required structure): setup, usage examples with real command output, architecture overview, assumptions & trade-offs, testing instructions.

**README "Assumptions & Trade-offs" section — keep this short, one line each:**
1. Duplicate job IDs are rejected — preserves data integrity, prevents accidental overwrites.
2. Manual DLQ retries reset the attempt counter to 0 — represents a fresh retry cycle after operator intervention.
3. Jobs are never deleted — preserves execution history for debugging; a cleanup command is a natural future addition, not an oversight.
4. FIFO scheduling (`created_at ASC`) — fair, predictable, easy to explain.
5. Crash recovery — any job left in `processing` after an unexpected shutdown is reset to `pending` on the next `worker start`.
6. SQLite chosen over a JSON file or MongoDB — atomic transactions give safe concurrent writes for free; simple deployment; ACID guarantees fit a local single-node CLI tool well.
7. Command timeout (if implemented) — configurable, default e.g. 300s; treated as a normal failure so it reuses existing retry/DLQ logic with no special-casing.

**`ENGINEERING_DECISIONS.md` (separate file):** the expanded version — for each of the 7 items above, write the alternatives you considered and why you rejected them (e.g. "considered a JSON file for persistence; rejected because concurrent writes require hand-rolled file locking, which is exactly the failure mode the assignment's disqualifier list warns about"). The README stays scannable; this file is what you actually study before the interview so the answers are decisions you made, not lines you memorized.

---

## 12. Interview Prep — Likely Questions

- **"Why SQLite over JSON files?"** → atomic transactions give you safe concurrent writes for free; JSON files require you to hand-roll file locking, which is exactly the kind of thing the assignment's disqualifier list calls out.
- **"How do you guarantee no two workers process the same job?"** → the atomic UPDATE...WHERE...LIMIT 1 claim query; explain SQLite's write serialization.
- **"What happens if a worker crashes mid-job?"** → job is orphaned in `processing`; recovered on next startup scan.
- **"Why exponential backoff instead of fixed delay?"** → avoids hammering a struggling downstream dependency; standard pattern, and you've implemented it before (Because exponential backoff prevents repeatedly hammering a failing dependency, gives external systems time to recover, and is the standard retry strategy used in production distributed systems.).
- **"How would you scale this to multiple machines?"** → you'd need a real broker (Redis/RabbitMQ/SQS) instead of a local SQLite file, since SQLite doesn't support multi-host concurrent writers — good to have this answer ready even though it's out of scope to build.
- **"Why FIFO instead of random or newest-first?"** → fairness and predictability; random is untestable, newest-first can starve old jobs indefinitely under sustained load.
- **"Why didn't you normalize stdout/stderr into a separate table?"** → simple schema is sufficient for the assignment's scope and keeps `jobs` easy to query for `status`; a `job_execution_logs` table (one row per attempt, indexed on `job_id`) is the natural upgrade if per-attempt history is needed, and you can describe exactly how you'd add it without disrupting the existing schema.

**Suggested commit sequence** (small, meaningful commits beat one large dump — this is itself graded under code quality):
`initial project setup → SQLite schema → CLI skeleton → single worker execution → multi-worker + atomic claim → retry & backoff → DLQ → crash recovery on startup → timeout handling → configuration → tests → README/docs → final polish`. A reviewer scanning your git history should be able to reconstruct your design process from the commits alone.

---

## 13. Pre-Submission Checklist

- ✔ Concurrency stress test run and passing, log evidence kept
- ✔ Kill-and-restart test performed manually, confirms persistence
- ✔ All 5 CLI command categories work end-to-end
- ✔ README covers all 5 required sections
- ✔ Commits are incremental and named by feature, not one dump
- ✔ No hardcoded config values (retry count / backoff base configurable)
- ✔ You can explain every design decision above out loud without notes

---

## 14. Top Risks, Ranked

1. **Concurrency bug that only shows under load** — mitigate with the explicit stress test in section 6, run before Day 5.
2. **Running out of time on polish/README and shipping thin docs** — mitigate by treating Day 5 as fixed, not flexible; cut scope (skip bonus features) before you cut documentation time.
3. **Being unable to defend a design choice live** — mitigate by writing section 12's answers into your own words now, before the interview, not during it.
