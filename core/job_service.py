"""
core/job_service.py — Business logic for the job lifecycle.

Every function in this module:
  - accepts a sqlite3.Connection as its first argument (never creates one)
  - delegates all SQL to storage.queries — no raw SQL strings here
  - raises domain exceptions (core.exceptions) so callers never handle
    raw sqlite3 errors
  - uses ``with conn:`` for every write so commits and rollbacks are explicit

Dependency direction: job_service → storage.queries, core.job, core.exceptions
                      (nothing in storage or core depends back on this module)
"""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from core.exceptions import DuplicateJobError, JobNotFoundError
from core.job import Job, JobState, OUTPUT_TRUNCATION_LIMIT
from storage import queries

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    """Return the current UTC time as an ISO-8601 string, e.g. ``2026-07-23T10:15:03Z``."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _truncate(text: Optional[str]) -> Optional[str]:
    """
    Truncate *text* to ``OUTPUT_TRUNCATION_LIMIT`` characters.

    Prevents runaway commands from bloating the database.
    Returns ``None`` unchanged so callers don't need to special-case it.
    """
    if text is None:
        return None
    return text[:OUTPUT_TRUNCATION_LIMIT]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def enqueue(
    conn: sqlite3.Connection,
    job_id: str,
    command: str,
    max_retries: int = 3,
) -> Job:
    """
    Insert a new job into the queue with ``state='pending'``.

    Args:
        conn:        Active database connection.
        job_id:      Caller-supplied unique identifier.  Must be globally unique.
        command:     Shell command string to execute (passed verbatim to the shell).
        max_retries: Maximum execution attempts before the job is moved to the DLQ.
                     Defaults to 3; will read from config in Phase 3.

    Returns:
        The newly created :class:`~core.job.Job`.

    Raises:
        DuplicateJobError: If a job with *job_id* already exists in any state.
    """
    now = _now_utc()
    try:
        with conn:
            conn.execute(queries.INSERT_JOB, (job_id, command, max_retries, now, now))
    except sqlite3.IntegrityError:
        raise DuplicateJobError(job_id)

    job = get_job(conn, job_id)
    assert job is not None, "Job must exist immediately after successful INSERT"

    logger.info(
        "Job enqueued | job_id=%s command=%r max_retries=%d",
        job_id, command, max_retries,
    )
    return job


def get_job(conn: sqlite3.Connection, job_id: str) -> Optional[Job]:
    """
    Retrieve a single job by its ID.

    Args:
        conn:   Active database connection.
        job_id: Job identifier to look up.

    Returns:
        The :class:`~core.job.Job` if found, ``None`` otherwise.
    """
    row = conn.execute(queries.SELECT_JOB_BY_ID, (job_id,)).fetchone()
    return Job.from_row(row) if row else None


def claim_job(conn: sqlite3.Connection, worker_id: str) -> Optional[Job]:
    """
    Atomically claim the oldest eligible pending job for a worker.

    Uses a single ``UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *``
    statement.  SQLite serializes writes, so two workers issuing this
    statement concurrently cannot both match the same row — the second
    worker's subquery simply finds no row after the first commits.
    No application-level lock is needed or used.

    A job is *eligible* if:
    - its ``state`` is ``'pending'``
    - its ``next_run_at`` is ``NULL`` **or** ``<= now`` (backoff delay elapsed)

    Jobs are claimed in FIFO order (``ORDER BY created_at ASC``).

    Args:
        conn:      Active database connection.
        worker_id: String identifier for the claiming worker (for audit / status).

    Returns:
        The claimed :class:`~core.job.Job` in ``'processing'`` state,
        or ``None`` if no eligible jobs are currently available.
    """
    now = _now_utc()
    with conn:
        cursor = conn.execute(queries.CLAIM_JOB, (worker_id, now, now, now))
        row = cursor.fetchone()

    if row is None:
        return None

    job = Job.from_row(row)
    logger.info(
        "Job claimed | worker=%s job_id=%s attempt=%d",
        worker_id, job.id, job.attempts,
    )
    return job


def mark_complete(
    conn: sqlite3.Connection,
    job_id: str,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> None:
    """
    Record a successful job execution.

    Sets ``state='completed'`` and stores the exit code and output.
    ``stdout`` and ``stderr`` are truncated to ``OUTPUT_TRUNCATION_LIMIT``
    characters before writing.

    Args:
        conn:      Active database connection.
        job_id:    ID of the job that completed.
        exit_code: Subprocess exit code (expected to be 0).
        stdout:    Captured stdout from the subprocess.
        stderr:    Captured stderr from the subprocess.
    """
    now = _now_utc()
    with conn:
        conn.execute(
            queries.UPDATE_JOB_COMPLETE,
            (exit_code, _truncate(stdout), _truncate(stderr), now, now, job_id),
        )
    logger.info("Job completed | job_id=%s exit_code=%d", job_id, exit_code)


def mark_failed(
    conn: sqlite3.Connection,
    job_id: str,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> Job:
    """
    Record a failed job execution and increment the attempt counter.

    Sets ``state='failed'`` and increments ``attempts`` atomically in the
    database.  The **retry decision** (reschedule vs. move to DLQ) is the
    responsibility of ``core.retry`` (Phase 3); this function only records
    what happened.

    Args:
        conn:      Active database connection.
        job_id:    ID of the job that failed.
        exit_code: Subprocess exit code (non-zero), or ``-1`` for timeout.
        stdout:    Captured stdout from the subprocess.
        stderr:    Captured stderr from the subprocess.

    Returns:
        The updated :class:`~core.job.Job` with the incremented ``attempts``
        count, so callers can immediately check :attr:`~core.job.Job.retries_exhausted`.

    Raises:
        JobNotFoundError: If no job with the given *job_id* exists.
    """
    now = _now_utc()
    with conn:
        cursor = conn.execute(
            queries.UPDATE_JOB_FAILED,
            (exit_code, _truncate(stdout), _truncate(stderr), now, now, job_id),
        )
        row = cursor.fetchone()

    if row is None:
        raise JobNotFoundError(job_id)

    job = Job.from_row(row)
    logger.warning(
        "Job failed | job_id=%s exit_code=%d attempts=%d/%d",
        job_id, exit_code, job.attempts, job.max_retries,
    )
    return job


def list_jobs(
    conn: sqlite3.Connection,
    state: Optional[str] = None,
) -> list[Job]:
    """
    Return a list of jobs, optionally filtered by state.

    Args:
        conn:  Active database connection.
        state: If provided, only return jobs in this state
               (must be a value from :class:`~core.job.JobState`).
               If ``None``, all jobs are returned.

    Returns:
        :class:`~core.job.Job` objects ordered by ``created_at ASC`` (FIFO).
    """
    if state is not None:
        rows = conn.execute(queries.SELECT_JOBS_BY_STATE, (state,)).fetchall()
    else:
        rows = conn.execute(queries.SELECT_ALL_JOBS).fetchall()

    return [Job.from_row(row) for row in rows]


def get_job_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Return a count of jobs grouped by state.

    All five states are always present in the result with a count of at
    least 0.  This guarantees a consistent display in ``queuectl status``
    regardless of which states have any jobs.

    Args:
        conn: Active database connection.

    Returns:
        Dict mapping state string → count, e.g.::

            {"pending": 5, "processing": 2, "completed": 18, "failed": 1, "dead": 0}
    """
    # Initialise all states to 0 so missing states don't produce KeyErrors.
    counts: dict[str, int] = {state: 0 for state in JobState.ALL}

    rows = conn.execute(queries.SELECT_JOB_COUNTS).fetchall()
    for row in rows:
        counts[row["state"]] = row["count"]

    return counts
