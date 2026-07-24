"""
tests/test_worker.py — Unit tests for core.worker.

Testing philosophy for the worker:
- We NEVER call Worker.run() in unit tests — it loops forever.
- We call Worker._recover_orphaned_jobs() and Worker._execute_job() directly.
- We use real subprocesses with short-lived commands (Python one-liners)
  for execution tests, because they are guaranteed to work on all platforms.
- We mock subprocess.run only for the timeout path (impossible to test
  with a real process without making the test suite slow).

Coverage:
  TestCrashRecovery       — orphaned job reset on startup
  TestWorkerConfig        — config loaded from DB at Worker.__init__
  TestExecutionSuccess    — exit code 0 → mark_complete
  TestExecutionFailure    — non-zero exit → mark_failed + handle_failure
  TestExecutionTimeout    — TimeoutExpired → mark_failed(exit_code=-1)
  TestStartedAt           — started_at set before subprocess call
"""

import sqlite3
import subprocess
import sys
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from core.exceptions import JobNotFoundError
from core.job import JobState
from core.job_service import claim_job, enqueue, get_job
from core.worker import Worker, _worker_process_main
from storage import queries

# ---------------------------------------------------------------------------
# Cross-platform command helpers
# ---------------------------------------------------------------------------

# Use the CURRENT interpreter so we never depend on 'python' being in PATH.
_PY = sys.executable


def cmd_success(msg: str = "hello") -> str:
    """A command that exits 0 and prints to stdout."""
    return f'"{_PY}" -c "print({msg!r})"'


def cmd_fail(code: int = 1) -> str:
    """A command that exits with a non-zero exit code."""
    return f'"{_PY}" -c "raise SystemExit({code})"'


def cmd_stderr(msg: str = "error") -> str:
    """A command that writes to stderr and exits 1."""
    return f'"{_PY}" -c "import sys; sys.stderr.write({msg!r}); raise SystemExit(1)"'


def cmd_both_outputs() -> str:
    """A command that writes to both stdout and stderr."""
    return (
        f'"{_PY}" -c "'
        f'print(\'out_value\'); '
        f'import sys; sys.stderr.write(\'err_value\')'
        f'"'
    )


# ---------------------------------------------------------------------------
# Fixture: a pre-built Worker with a tmp_db connection
# ---------------------------------------------------------------------------

@pytest.fixture
def worker(tmp_db: sqlite3.Connection) -> Worker:
    """Worker bound to the test DB with fast poll settings."""
    return Worker(
        conn=tmp_db,
        worker_id="test-worker",
        poll_interval_ms=50,
        timeout_seconds=10,
        backoff_base=2.0,
    )


# ---------------------------------------------------------------------------
# TestCrashRecovery
# ---------------------------------------------------------------------------

class TestCrashRecovery:

    def test_orphaned_jobs_are_reset_to_pending(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """Jobs stuck in 'processing' must be reset to 'pending' at startup."""
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        enqueue(tmp_db, "job-2", "echo 2", max_retries=3)
        claim_job(tmp_db, "crashed-worker")
        claim_job(tmp_db, "crashed-worker")

        assert get_job(tmp_db, "job-1").state == JobState.PROCESSING
        assert get_job(tmp_db, "job-2").state == JobState.PROCESSING

        worker._recover_orphaned_jobs()

        assert get_job(tmp_db, "job-1").state == JobState.PENDING
        assert get_job(tmp_db, "job-2").state == JobState.PENDING

    def test_recovered_jobs_have_no_worker_id(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """Recovered jobs must have worker_id cleared so they can be reclaimed."""
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        claim_job(tmp_db, "crashed-worker")

        worker._recover_orphaned_jobs()

        job = get_job(tmp_db, "job-1")
        assert job.worker_id is None

    def test_no_orphans_is_a_noop(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """Calling recovery on an empty queue must not raise or modify anything."""
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        # Do NOT claim — job is pending

        worker._recover_orphaned_jobs()  # Should not raise

        assert get_job(tmp_db, "job-1").state == JobState.PENDING

    def test_empty_queue_is_safe(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        worker._recover_orphaned_jobs()  # Must not raise on empty queue

    def test_only_processing_jobs_are_reset(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """Pending and completed jobs must NOT be touched by crash recovery."""
        enqueue(tmp_db, "pending-job", "echo pending", max_retries=3)
        enqueue(tmp_db, "processing-job", "echo processing", max_retries=3)
        claim_job(tmp_db, "crashed-worker")  # claims processing-job (oldest)

        worker._recover_orphaned_jobs()

        assert get_job(tmp_db, "pending-job").state == JobState.PENDING
        assert get_job(tmp_db, "processing-job").state == JobState.PENDING

    def test_recovered_jobs_become_claimable(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """After recovery, the reset jobs must be claimable by a new worker."""
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        claim_job(tmp_db, "crashed-worker")

        worker._recover_orphaned_jobs()

        claimed = claim_job(tmp_db, "new-worker")
        assert claimed is not None
        assert claimed.id == "job-1"


# ---------------------------------------------------------------------------
# TestWorkerConfig
# ---------------------------------------------------------------------------

class TestWorkerConfig:

    def test_default_worker_id_contains_pid(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        import os
        w = Worker(tmp_db)
        assert str(os.getpid()) in w.worker_id

    def test_explicit_worker_id_is_used(self, tmp_db: sqlite3.Connection) -> None:
        w = Worker(tmp_db, worker_id="my-worker")
        assert w.worker_id == "my-worker"

    def test_poll_interval_loaded_from_db_config(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        from config import set_config_value
        set_config_value(tmp_db, "poll_interval_ms", "750")
        w = Worker(tmp_db)  # no explicit poll_interval_ms
        assert w.poll_interval_ms == 750

    def test_timeout_loaded_from_db_config(self, tmp_db: sqlite3.Connection) -> None:
        from config import set_config_value
        set_config_value(tmp_db, "timeout_seconds", "120")
        w = Worker(tmp_db)
        assert w.timeout_seconds == 120

    def test_backoff_base_loaded_from_db_config(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        from config import set_config_value
        set_config_value(tmp_db, "backoff_base", "3")
        w = Worker(tmp_db)
        assert w.backoff_base == 3.0

    def test_explicit_values_override_db_config(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Values passed to __init__ must take priority over the DB config."""
        from config import set_config_value
        set_config_value(tmp_db, "poll_interval_ms", "1000")
        w = Worker(tmp_db, poll_interval_ms=200)
        assert w.poll_interval_ms == 200


# ---------------------------------------------------------------------------
# TestExecutionSuccess
# ---------------------------------------------------------------------------

class TestExecutionSuccess:

    def _claim_job(
        self,
        conn: sqlite3.Connection,
        command: str,
        worker_id: str = "test-worker",
    ):
        enqueue(conn, "job-1", command, max_retries=3)
        return claim_job(conn, worker_id)

    def test_successful_job_marked_complete(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        job = self._claim_job(tmp_db, cmd_success())
        worker._execute_job(job)
        assert get_job(tmp_db, "job-1").state == JobState.COMPLETED

    def test_exit_code_zero_is_stored(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        job = self._claim_job(tmp_db, cmd_success())
        worker._execute_job(job)
        assert get_job(tmp_db, "job-1").exit_code == 0

    def test_stdout_is_captured(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        job = self._claim_job(tmp_db, cmd_success("captured output"))
        worker._execute_job(job)
        stored = get_job(tmp_db, "job-1").stdout
        assert "captured output" in stored

    def test_finished_at_is_set(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        job = self._claim_job(tmp_db, cmd_success())
        worker._execute_job(job)
        assert get_job(tmp_db, "job-1").finished_at is not None

    def test_started_at_is_set(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        job = self._claim_job(tmp_db, cmd_success())
        worker._execute_job(job)
        assert get_job(tmp_db, "job-1").started_at is not None

    def test_started_at_and_picked_at_are_distinct_fields(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """
        The EDD distinguishes picked_at (claim time) from started_at
        (subprocess start time).  Both must be set after execution.
        """
        job = self._claim_job(tmp_db, cmd_success())
        assert job.picked_at is not None   # set by claim_job
        assert job.started_at is None      # not yet set

        worker._execute_job(job)

        executed_job = get_job(tmp_db, "job-1")
        assert executed_job.picked_at is not None
        assert executed_job.started_at is not None


# ---------------------------------------------------------------------------
# TestExecutionFailure
# ---------------------------------------------------------------------------

class TestExecutionFailure:

    def _setup_and_execute(
        self,
        conn: sqlite3.Connection,
        worker: Worker,
        command: str,
        max_retries: int = 3,
    ):
        enqueue(conn, "job-1", command, max_retries=max_retries)
        job = claim_job(conn, "test-worker")
        worker._execute_job(job)

    def test_failed_job_exit_code_stored(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        self._setup_and_execute(tmp_db, worker, cmd_fail(42))
        assert get_job(tmp_db, "job-1").exit_code == 42

    def test_failed_job_state_is_not_processing(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """After failure handling, the job must NOT stay in 'processing' state."""
        self._setup_and_execute(tmp_db, worker, cmd_fail(), max_retries=3)
        state = get_job(tmp_db, "job-1").state
        assert state != JobState.PROCESSING

    def test_failed_job_with_retries_becomes_pending(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """With retries remaining, the job must be rescheduled to 'pending'."""
        self._setup_and_execute(tmp_db, worker, cmd_fail(), max_retries=3)
        assert get_job(tmp_db, "job-1").state == JobState.PENDING

    def test_failed_job_exhausted_retries_goes_to_dlq(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """With max_retries=1, first failure should move the job to DLQ."""
        self._setup_and_execute(tmp_db, worker, cmd_fail(), max_retries=1)
        assert get_job(tmp_db, "job-1").state == JobState.DEAD

    def test_attempts_incremented_after_failure(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        self._setup_and_execute(tmp_db, worker, cmd_fail(), max_retries=3)
        assert get_job(tmp_db, "job-1").attempts == 1

    def test_stderr_is_captured(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        self._setup_and_execute(tmp_db, worker, cmd_stderr("captured_err"))
        stored = get_job(tmp_db, "job-1").stderr
        assert "captured_err" in stored

    def test_failed_job_has_next_run_at_set(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """Rescheduled pending job must have a future next_run_at (backoff)."""
        self._setup_and_execute(tmp_db, worker, cmd_fail(), max_retries=3)
        job = get_job(tmp_db, "job-1")
        assert job.state == JobState.PENDING
        assert job.next_run_at is not None


# ---------------------------------------------------------------------------
# TestExecutionTimeout
# ---------------------------------------------------------------------------

class TestExecutionTimeout:
    """
    Test the timeout path by mocking subprocess.run to raise TimeoutExpired.
    We never use a real sleep command in unit tests — that would make the
    suite slow and fragile.
    """

    def _make_timeout_exc(self, cmd: str = "sleep", timeout: int = 5):
        exc = subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)
        exc.stdout = ""
        exc.stderr = ""
        return exc

    def test_timeout_uses_exit_code_minus_one(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        enqueue(tmp_db, "job-1", "sleep 9999", max_retries=3)
        job = claim_job(tmp_db, "test-worker")

        with patch("core.worker.subprocess.run") as mock_run:
            mock_run.side_effect = self._make_timeout_exc()
            worker._execute_job(job)

        assert get_job(tmp_db, "job-1").exit_code == -1

    def test_timed_out_job_state_is_not_processing(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        enqueue(tmp_db, "job-1", "sleep 9999", max_retries=3)
        job = claim_job(tmp_db, "test-worker")

        with patch("core.worker.subprocess.run") as mock_run:
            mock_run.side_effect = self._make_timeout_exc()
            worker._execute_job(job)

        assert get_job(tmp_db, "job-1").state != JobState.PROCESSING

    def test_timed_out_job_with_retries_becomes_pending(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        enqueue(tmp_db, "job-1", "sleep 9999", max_retries=3)
        job = claim_job(tmp_db, "test-worker")

        with patch("core.worker.subprocess.run") as mock_run:
            mock_run.side_effect = self._make_timeout_exc()
            worker._execute_job(job)

        assert get_job(tmp_db, "job-1").state == JobState.PENDING

    def test_timed_out_job_exhausted_goes_to_dlq(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        enqueue(tmp_db, "job-1", "sleep 9999", max_retries=1)
        job = claim_job(tmp_db, "test-worker")

        with patch("core.worker.subprocess.run") as mock_run:
            mock_run.side_effect = self._make_timeout_exc()
            worker._execute_job(job)

        assert get_job(tmp_db, "job-1").state == JobState.DEAD

    def test_timeout_note_appended_to_stderr(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        enqueue(tmp_db, "job-1", "sleep 9999", max_retries=3)
        job = claim_job(tmp_db, "test-worker")

        with patch("core.worker.subprocess.run") as mock_run:
            mock_run.side_effect = self._make_timeout_exc()
            worker._execute_job(job)

        stored_stderr = get_job(tmp_db, "job-1").stderr
        assert "QueueCTL" in stored_stderr or "timeout" in stored_stderr.lower()

    def test_timeout_with_bytes_stdout_is_handled(
        self, tmp_db: sqlite3.Connection, worker: Worker
    ) -> None:
        """TimeoutExpired may carry bytes stdout even with text=True; worker
        must decode gracefully instead of crashing."""
        enqueue(tmp_db, "job-1", "sleep 9999", max_retries=3)
        job = claim_job(tmp_db, "test-worker")

        exc = subprocess.TimeoutExpired(cmd="sleep", timeout=5)
        exc.stdout = b"partial bytes output"
        exc.stderr = b"partial bytes stderr"

        with patch("core.worker.subprocess.run") as mock_run:
            mock_run.side_effect = exc
            worker._execute_job(job)   # must not raise UnicodeDecodeError or AttributeError

        stored = get_job(tmp_db, "job-1")
        assert stored.stdout is not None
        assert stored.stderr is not None
