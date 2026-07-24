"""
core/retry.py — Exponential backoff calculation and DLQ movement logic.

Responsibilities:
- ``calculate_backoff``  : pure function; no DB dependency — fully unit-testable
- ``schedule_retry``     : reschedule a failed job with a backoff delay
- ``move_to_dlq``        : permanently quarantine a job in the Dead Letter Queue
- ``handle_failure``     : single decision point called by the worker after execution fails
- ``dlq_retry``          : operator-initiated reset of a dead job back to pending

Design note — ``handle_failure`` accepts ``backoff_base`` as a parameter rather
than reading config internally.  This keeps the retry module free of config
dependencies and makes it trivial to test with any base value without touching
the database config table.  The caller (worker, Phase 4) reads config once at
the start of its loop and passes the value through.

Dependency direction: retry → core.job, core.exceptions, storage.queries
                      (never → config, never → job_service to avoid circular refs)
"""

import logging
import sqlite3
from datetime import datetime, timezone, timedelta

from core.exceptions import InvalidStateTransitionError, JobNotFoundError
from core.job import Job, JobState
from storage import queries

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Maximum backoff delay in seconds.  Prevents a high base or many retries
#: from producing multi-day delays.
MAX_BACKOFF_SECONDS: int = 300

#: Default exponential base used when caller does not supply one.
DEFAULT_BACKOFF_BASE: float = 2.0


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    """Return current UTC time as an ISO-8601 string (e.g. ``2026-07-23T10:15:03Z``)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_backoff(
    attempts: int,
    base: float = DEFAULT_BACKOFF_BASE,
    cap: int = MAX_BACKOFF_SECONDS,
) -> float:
    """
    Compute the exponential backoff delay for the next retry attempt.

    Formula: ``delay = base ** attempts``, capped at *cap* seconds.

    The *attempts* value used here is the **post-increment** count returned
    by :func:`~core.job_service.mark_failed`, so:

    - 1st failure → attempts=1 → delay = base¹
    - 2nd failure → attempts=2 → delay = base²
    - …

    Passing ``attempts=0`` returns ``0.0`` (no delay), which is the
    correct behaviour for a fresh job that has never been attempted.

    Args:
        attempts: Attempt count after the most recent failure.
        base:     Exponential base (from ``config.backoff_base``).  Default 2.
        cap:      Maximum delay in seconds.  Default ``MAX_BACKOFF_SECONDS`` (300).

    Returns:
        Backoff delay in seconds as a :class:`float`.
    """
    if attempts <= 0:
        return 0.0
    return min(float(base ** attempts), float(cap))


def schedule_retry(
    conn: sqlite3.Connection,
    job_id: str,
    delay_seconds: float,
) -> None:
    """
    Reschedule a failed job by resetting it to ``'pending'`` with a backoff delay.

    Sets ``next_run_at = now + delay_seconds`` so the claim query won't pick
    the job up until the backoff window has elapsed.

    Args:
        conn:          Active database connection.
        job_id:        ID of the job to reschedule.
        delay_seconds: Seconds to wait before the job becomes claimable again.
    """
    now = _now_utc()
    next_run_at = (
        datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    with conn:
        conn.execute(queries.UPDATE_JOB_RETRY, (next_run_at, now, job_id))

    logger.info(
        "Retry scheduled | job_id=%s delay=%.1fs next_run_at=%s",
        job_id, delay_seconds, next_run_at,
    )


def move_to_dlq(conn: sqlite3.Connection, job_id: str) -> None:
    """
    Permanently quarantine a job in the Dead Letter Queue (``state='dead'``).

    Called when a job's attempt count has reached ``max_retries``.  The job
    stays in the database for auditing; a separate ``dlq retry`` command can
    revive it after operator investigation.

    Args:
        conn:   Active database connection.
        job_id: ID of the job to move to DLQ.
    """
    now = _now_utc()
    with conn:
        conn.execute(queries.UPDATE_JOB_DEAD, (now, job_id))
    logger.error("Job moved to DLQ | job_id=%s", job_id)


def handle_failure(
    conn: sqlite3.Connection,
    job: Job,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
) -> None:
    """
    Decide what to do after a job execution failure.

    This is the **single call-point** for the worker after
    :func:`~core.job_service.mark_failed` returns.  It makes exactly one
    decision:

    - If ``job.retries_exhausted`` → :func:`move_to_dlq`
    - Otherwise → :func:`schedule_retry` with the computed backoff delay

    Args:
        conn:         Active database connection.
        job:          The updated :class:`~core.job.Job` returned by
                      ``mark_failed`` (has already-incremented ``attempts``).
        backoff_base: Exponential backoff base.  Caller reads this from config
                      so this module has no config dependency.
    """
    if job.retries_exhausted:
        move_to_dlq(conn, job.id)
    else:
        delay = calculate_backoff(job.attempts, backoff_base)
        schedule_retry(conn, job.id, delay)


def dlq_retry(conn: sqlite3.Connection, job_id: str) -> Job:
    """
    Operator-initiated retry of a dead job.

    Resets ``state='pending'``, ``attempts=0``, ``next_run_at=NULL`` — a
    completely fresh retry cycle, as if the job had just been enqueued again.

    The ``AND state='dead'`` guard in the SQL means this will never silently
    reset a job that is not actually in the DLQ.

    Args:
        conn:   Active database connection.
        job_id: ID of the dead job to revive.

    Returns:
        The updated :class:`~core.job.Job` in ``pending`` state with
        ``attempts=0``.

    Raises:
        JobNotFoundError:           If *job_id* does not exist.
        InvalidStateTransitionError: If the job exists but is not ``'dead'``.
    """
    # Validate first so we can give a precise error before touching state.
    row = conn.execute(queries.SELECT_JOB_BY_ID, (job_id,)).fetchone()
    if row is None:
        raise JobNotFoundError(job_id)

    existing = Job.from_row(row)
    if existing.state != JobState.DEAD:
        raise InvalidStateTransitionError(
            job_id, existing.state, JobState.PENDING
        )

    now = _now_utc()
    with conn:
        cursor = conn.execute(queries.DLQ_RETRY_JOB, (now, job_id))
        updated_row = cursor.fetchone()

    job = Job.from_row(updated_row)
    logger.info(
        "DLQ retry | job_id=%s reset to pending (was attempts=%d)",
        job_id, existing.attempts,
    )
    return job
