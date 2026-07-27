# QueueCTL — Engineering Decisions

This document records the key design choices made during the implementation of QueueCTL,
the alternatives that were considered, and the reasoning behind each decision.
It is the expanded companion to the "Assumptions & Trade-offs" section in the README.

---

## 1. Persistence: SQLite over alternatives

**Decision:** Use SQLite (WAL mode, `busy_timeout=5000ms`) as the single source of truth.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **JSON file** | Concurrent writes from multiple worker processes require hand-rolled file locking. Getting this right is non-trivial — you need `fcntl`/`msvcrt` locks, atomic rename patterns, and retry logic. SQLite provides all of this at the engine level for free. JSON also has no query language, making `status` and `list` queries awkward. |
| **Redis** | Adds an external service dependency that must be installed, started, and managed separately. Not appropriate for a self-contained CLI tool. Redis also loses data on restart unless persistence is explicitly configured — the opposite of "durable". |
| **RabbitMQ / SQS** | Same infrastructure overhead problem as Redis, multiplied. These are correct choices for distributed multi-node systems; they are architectural overkill for a local single-node CLI. |
| **SQLite without WAL** | Default journal mode (`DELETE`) blocks readers while a writer holds the lock. WAL mode allows concurrent reads (for `status`, `list`) without blocking writers (workers claiming jobs). One-line change, meaningful concurrency improvement. |

**Why SQLite wins here:** An atomic `UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *`
is serialized at the DB engine level. Two concurrent workers issuing this query simultaneously
cannot both match the same row — SQLite write serialization is the only concurrency primitive
needed. Zero application-level locking code required.

---

## 2. Concurrency: Atomic claim query over application-level locks

**Decision:** The claim query is a single `UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *`.
No `threading.Lock`, `multiprocessing.Lock`, or `SELECT FOR UPDATE` workaround.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **`threading.Lock`** | Does not work across OS process boundaries. Workers are separate `multiprocessing` processes (each with their own memory space), so a lock in one process memory is invisible to another. Classic TOCTOU race. |
| **`multiprocessing.Lock`** | Requires sharing the lock object at process spawn time. Couples lock lifetime to the parent process — if the parent crashes, workers may deadlock on a lock that will never release. Adds complexity for zero benefit when the DB can do this atomically. |
| **Two-step SELECT then UPDATE** | Introduces a time window between "I found job-5" and "I claimed job-5" where another worker can claim the same job. The single-statement approach eliminates this race entirely. |

**The proof:** `tests/test_concurrency.py` spawns real OS processes sharing one SQLite file
and verifies zero double-claims under three contention ratios (equal, high-pressure, sequential).
Empirical confirmation, not just theoretical correctness.

---

## 3. Job ID policy: Reject duplicates, never overwrite

**Decision:** If a job with the same `id` already exists in any state, `enqueue` raises
`DuplicateJobError` and the CLI surfaces a clear error message.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **Silently overwrite** | Overwrites a job that may be actively `processing`. The worker that claimed it will write results back to a row with different fields — data corruption. |
| **Auto-generate IDs** | Defeats the purpose of explicit IDs. Operators use job IDs to correlate enqueue calls with status queries and DLQ entries. An opaque UUID provides no operator value. |
| **Upsert (update if exists)** | Same corruption risk. A `pending` job that gets upserted might get a different `command` while a worker is about to claim it. |

**Why reject:** Job IDs are the operator handle on a unit of work. Immutability is correct;
re-running with the same ID requires `dlq retry` or waiting for completion first — explicit and auditable.

---

## 4. DLQ retry: Reset `attempts=0`, not `attempts=max_retries-1`

**Decision:** `dlq retry <id>` resets `attempts=0` and `next_run_at=NULL`, giving the job
a completely fresh retry budget.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **Keep `attempts` at current value** | Job would immediately move back to DLQ on the next failure. The operator action would have no practical effect. |
| **Set `attempts=max_retries-1`** | Gives exactly one more attempt before going back to DLQ. Inflexible — an operator who has fixed the root cause expects the full retry cycle back, not one shot. |

**Why `attempts=0`:** A `dlq retry` is an explicit operator action implying investigation and
root-cause resolution. A full fresh retry cycle is the most useful and unsurprising behavior.

---

## 5. Job retention: Never delete completed or dead jobs

**Decision:** Jobs are never deleted. `completed` and `dead` rows remain until a future
cleanup command is added.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **Delete on completion** | Destroys the execution record. Cannot answer "when did job-X complete?" after the fact. |
| **Delete on DLQ move** | The dead job stderr is the primary debugging tool. Deleting before operator inspection is harmful. |
| **Auto-purge after N days** | Premature complexity. A future `queuectl purge --before <date>` is the correct addition. |

**Why retain:** The `jobs` table is an audit log as much as a queue. Retaining rows costs
negligible disk space and pays dividends in debuggability.

---

## 6. Scheduling: FIFO (`created_at ASC`) over alternatives

**Decision:** The claim query orders eligible `pending` jobs by `created_at ASC`.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **Random selection** | Non-deterministic. Makes claim-order test assertions impossible. No fairness guarantee. |
| **Newest-first** | Starves old jobs indefinitely under sustained load. A queue that never drains old work is a broken queue. |
| **Priority queue** | Requires a `priority` column. Not in scope; the extension is `ORDER BY priority DESC, created_at ASC` — a one-line change. |

**Why FIFO:** Fair, predictable, and trivially explainable. Every job is processed in arrival
order, matching operator intuition.

---

## 7. Timeout: Treat as a normal failure, not a special case

**Decision:** `subprocess.TimeoutExpired` sets `exit_code=-1`, appends a note to `stderr`,
and passes to the standard `handle_failure()` path — same as any non-zero exit code.

**Alternatives considered:**

| Option | Why rejected |
|--------|-------------|
| **Separate `timed_out` state** | Adds a new state with no behavioral difference from `failed`. Every state consumer needs handling for both cases. `exit_code=-1` is sufficient identification without a schema change. |
| **Immediate DLQ on timeout** | A timeout can be transient (overloaded host, resource spike). Retrying with backoff gives the job a fair chance when conditions improve. |
| **No timeout** | A runaway subprocess holds a worker indefinitely, starving all other jobs on that worker. |

**Why normal failure path:** Reusing retry → DLQ for timeouts means zero special-case logic.
The timeout is observable via `exit_code=-1` and the stderr annotation — without adding state machine complexity.

---

*Study this document before the interview. Every answer here should be expressible out loud without notes.*
