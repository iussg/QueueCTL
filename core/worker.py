"""
core/worker.py — Background job execution loop.

Responsibilities:
- Crash recovery on startup (reset orphaned 'processing' jobs to 'pending')
- Poll the queue for eligible jobs using the atomic claim query
- Execute each job as a subprocess with a configurable timeout
- Record started_at, stdout/stderr, exit code, and finished_at
- Delegate to handle_failure for retry-vs-DLQ decisions
- Respond to SIGINT/SIGTERM with a clean shutdown (finish current job, exit)

Design notes:
- One Worker instance per OS process. Never share a Worker (or its connection)
  across threads or processes.
- Config values are loaded once at __init__ time from the DB config table.
  If an operator changes config while a worker is running, the worker picks
  it up on the next restart (acceptable for a background daemon).
- The worker does NOT know about multiprocessing itself; spawning multiple
  workers is the responsibility of the CLI layer.

Dependency direction: worker → core.job_service, core.retry, config, storage
"""

import logging
import os
import signal
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from typing import Optional

from config import get_float_config, get_int_config
from core.exceptions import JobNotFoundError
from core.job import Job, JobState
from core.job_service import claim_job, mark_complete, mark_failed
from core.retry import handle_failure
from storage import queries
from storage.db import get_connection, initialize_schema

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Worker class
# ---------------------------------------------------------------------------

class Worker:
    """
    A single worker that polls the queue, executes jobs, and handles results.

    Each instance is bound to one :class:`sqlite3.Connection` and one
    ``worker_id``.  Run one instance per OS process.

    Args:
        conn:             Active database connection (owned by this worker).
        worker_id:        Unique identifier for this worker.  Defaults to
                          ``"worker-<pid>"``.
        poll_interval_ms: Milliseconds to sleep between polls when the queue
                          is empty.  Defaults to the ``poll_interval_ms``
                          config value (500ms).
        timeout_seconds:  Maximum subprocess execution time in seconds.
                          Defaults to the ``timeout_seconds`` config value (300s).
        backoff_base:     Exponential backoff base passed to
                          :func:`~core.retry.handle_failure`.  Defaults to
                          the ``backoff_base`` config value (2.0).
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        worker_id: Optional[str] = None,
        poll_interval_ms: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        backoff_base: Optional[float] = None,
    ) -> None:
        self.conn = conn
        self.worker_id: str = worker_id or f"worker-{os.getpid()}"

        # Load configuration from the DB; caller-supplied values take priority.
        self.poll_interval_ms: int = (
            poll_interval_ms
            if poll_interval_ms is not None
            else get_int_config(conn, "poll_interval_ms", default=500)
        )
        self.timeout_seconds: int = (
            timeout_seconds
            if timeout_seconds is not None
            else get_int_config(conn, "timeout_seconds", default=300)
        )
        self.backoff_base: float = (
            backoff_base
            if backoff_base is not None
            else get_float_config(conn, "backoff_base", default=2.0)
        )

        self._shutdown: bool = False

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_signal(self, signum: int, frame: object) -> None:
        """
        Set the shutdown flag on SIGINT / SIGTERM.

        The worker finishes executing any job already in progress before
        checking the flag, so the DB is never left in an inconsistent state
        by a clean shutdown.
        """
        logger.info(
            "Worker %s received signal %d — finishing current job then exiting",
            self.worker_id, signum,
        )
        self._shutdown = True

    # ------------------------------------------------------------------
    # Crash recovery
    # ------------------------------------------------------------------

    def _recover_orphaned_jobs(self) -> None:
        """
        Reset jobs stuck in ``'processing'`` state from a prior crashed run.

        Called once at startup, before entering the poll loop.  Idempotent:
        if no orphaned jobs exist, this is a no-op.
        """
        rows = self.conn.execute(queries.SELECT_PROCESSING_JOBS).fetchall()
        if not rows:
            logger.info(
                "Worker %s — crash recovery: no orphaned jobs", self.worker_id
            )
            return

        orphan_ids = [row["id"] for row in rows]
        now = _now_utc()
        with self.conn:
            self.conn.execute(queries.RESET_ORPHANED_JOBS, (now,))

        logger.warning(
            "Worker %s — crash recovery: reset %d orphaned job(s) to pending: %s",
            self.worker_id, len(orphan_ids), orphan_ids,
        )

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _execute_job(self, job: Job) -> None:
        """
        Execute a single claimed job as a subprocess and record the result.

        Sets ``started_at`` just before the subprocess is launched (distinct
        from ``picked_at``, which was set by the claim query).

        On success (exit code 0):
            → :func:`~core.job_service.mark_complete`

        On failure (non-zero exit code):
            → :func:`~core.job_service.mark_failed`
            → :func:`~core.retry.handle_failure` (retry or DLQ)

        On timeout:
            → :func:`~core.job_service.mark_failed` with ``exit_code=-1``
            → :func:`~core.retry.handle_failure`
        """
        # Record when subprocess execution actually begins.
        started_at = _now_utc()
        with self.conn:
            self.conn.execute(
                queries.UPDATE_JOB_STARTED, (started_at, started_at, job.id)
            )

        logger.info(
            "Executing | worker=%s job_id=%s command=%r timeout=%ds",
            self.worker_id, job.id, job.command, self.timeout_seconds,
        )

        # ---- Subprocess execution ----
        try:
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            # subprocess.run kills the process before re-raising; output
            # (potentially partial) is available on the exception object.
            stdout_raw = exc.stdout or ""
            stderr_raw = exc.stderr or ""
            stdout = (
                stdout_raw
                if isinstance(stdout_raw, str)
                else stdout_raw.decode("utf-8", errors="replace")
            )
            stderr = (
                stderr_raw
                if isinstance(stderr_raw, str)
                else stderr_raw.decode("utf-8", errors="replace")
            )

            logger.warning(
                "Job timed out | worker=%s job_id=%s timeout=%ds",
                self.worker_id, job.id, self.timeout_seconds,
            )
            failed_job = mark_failed(
                self.conn, job.id,
                exit_code=-1,
                stdout=stdout,
                stderr=stderr + f"\n[QueueCTL] Job killed after {self.timeout_seconds}s timeout.",
            )
            handle_failure(self.conn, failed_job, backoff_base=self.backoff_base)
            return

        # ---- Interpret result ----
        if result.returncode == 0:
            logger.info(
                "Job succeeded | worker=%s job_id=%s",
                self.worker_id, job.id,
            )
            mark_complete(
                self.conn, job.id,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        else:
            logger.warning(
                "Job failed | worker=%s job_id=%s exit_code=%d",
                self.worker_id, job.id, result.returncode,
            )
            failed_job = mark_failed(
                self.conn, job.id,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
            handle_failure(self.conn, failed_job, backoff_base=self.backoff_base)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Start the worker poll loop.

        1. Register signal handlers for SIGINT/SIGTERM.
        2. Run crash recovery.
        3. Loop: claim → execute → sleep (if nothing to claim).
        4. Exit cleanly when ``_shutdown`` flag is set.
        """
        logger.info(
            "Worker %s starting | pid=%d poll=%dms timeout=%ds backoff_base=%.1f",
            self.worker_id, os.getpid(),
            self.poll_interval_ms, self.timeout_seconds, self.backoff_base,
        )

        # Register signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._handle_signal)
            except (OSError, ValueError):
                pass  # SIGTERM handling differs on Windows; best-effort only

        # Crash recovery on startup
        self._recover_orphaned_jobs()

        logger.info("Worker %s entering poll loop", self.worker_id)
        poll_sleep = self.poll_interval_ms / 1000.0

        while not self._shutdown:
            job = claim_job(self.conn, self.worker_id)
            if job is None:
                time.sleep(poll_sleep)
                continue

            logger.info(
                "Worker %s claimed job %s (attempt %d/%d)",
                self.worker_id, job.id, job.attempts + 1, job.max_retries,
            )
            self._execute_job(job)

        logger.info("Worker %s exited cleanly", self.worker_id)


# ---------------------------------------------------------------------------
# Process entry point (called by multiprocessing.Process)
# ---------------------------------------------------------------------------

def _worker_process_main(worker_id: str) -> None:
    """
    Entry point for each spawned worker process.

    Creates a fresh DB connection (connections cannot be shared across
    processes), initialises the schema (idempotent), constructs and runs
    the :class:`Worker`.
    """
    conn = get_connection()
    initialize_schema(conn)
    w = Worker(conn, worker_id=worker_id)
    try:
        w.run()
    finally:
        conn.close()
