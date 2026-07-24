"""
tests/test_job_service.py — Unit tests for core.job_service.

Tests are grouped by function.  Each test is independent: the ``tmp_db``
fixture provides a fresh, isolated SQLite database per test invocation.

Coverage:
  TestEnqueue        — insert, validation, duplicate rejection
  TestGetJob         — lookup by ID, not-found
  TestClaimJob       — atomic claim, FIFO order, backoff respect, empty queue
  TestMarkComplete   — state, fields, truncation
  TestMarkFailed     — state, attempt increment, not-found
  TestListJobs       — all / filtered / ordering
  TestGetJobCounts   — correct counts, all-zero baseline
"""

import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest

from core.exceptions import DuplicateJobError, JobNotFoundError
from core.job import Job, JobState, OUTPUT_TRUNCATION_LIMIT
from core.job_service import (
    claim_job,
    enqueue,
    get_job,
    get_job_counts,
    list_jobs,
    mark_complete,
    mark_failed,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _future(seconds: int = 3600) -> str:
    """ISO-8601 timestamp N seconds in the future."""
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _past(seconds: int = 3600) -> str:
    """ISO-8601 timestamp N seconds in the past."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _set_next_run_at(conn: sqlite3.Connection, job_id: str, ts: Optional[str]) -> None:
    """Directly set next_run_at on a job (simulates backoff scheduling)."""
    conn.execute("UPDATE jobs SET next_run_at = ? WHERE id = ?", (ts, job_id))
    conn.commit()


def _reset_to_pending(conn: sqlite3.Connection, job_id: str) -> None:
    """Reset a failed job back to pending (simulates retry logic from Phase 3)."""
    conn.execute(
        "UPDATE jobs SET state = 'pending', next_run_at = NULL WHERE id = ?",
        (job_id,),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# TestEnqueue
# ---------------------------------------------------------------------------

class TestEnqueue:

    def test_returns_job_object(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        assert isinstance(job, Job)

    def test_initial_state_is_pending(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        assert job.state == JobState.PENDING

    def test_job_id_is_stored(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        assert job.id == "job-1"

    def test_command_is_stored(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        assert job.command == "echo hello"

    def test_max_retries_is_stored(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=5)
        assert job.max_retries == 5

    def test_attempts_start_at_zero(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        assert job.attempts == 0

    def test_created_at_is_set(self, tmp_db: sqlite3.Connection) -> None:
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        assert job.created_at is not None
        assert "T" in job.created_at   # ISO-8601 sanity check
        assert job.created_at.endswith("Z")

    def test_job_is_persisted_in_database(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        retrieved = get_job(tmp_db, "job-1")
        assert retrieved is not None
        assert retrieved.id == "job-1"
        assert retrieved.command == "echo hello"

    def test_duplicate_id_raises_duplicate_job_error(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        with pytest.raises(DuplicateJobError):
            enqueue(tmp_db, "job-1", "echo world", max_retries=3)

    def test_duplicate_error_message_contains_job_id(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-abc", "echo a", max_retries=3)
        with pytest.raises(DuplicateJobError) as exc_info:
            enqueue(tmp_db, "job-abc", "echo b", max_retries=3)
        assert "job-abc" in str(exc_info.value)

    def test_first_job_not_affected_by_duplicate_rejection(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Duplicate rejection must not corrupt the original job."""
        original = enqueue(tmp_db, "job-1", "echo original", max_retries=3)
        with pytest.raises(DuplicateJobError):
            enqueue(tmp_db, "job-1", "echo overwrite", max_retries=99)
        retrieved = get_job(tmp_db, "job-1")
        assert retrieved.command == "echo original"
        assert retrieved.max_retries == 3

    def test_multiple_unique_jobs_accepted(self, tmp_db: sqlite3.Connection) -> None:
        for i in range(5):
            job = enqueue(tmp_db, f"job-{i}", f"echo {i}", max_retries=3)
            assert job.id == f"job-{i}"
        assert len(list_jobs(tmp_db)) == 5


# ---------------------------------------------------------------------------
# TestGetJob
# ---------------------------------------------------------------------------

class TestGetJob:

    def test_returns_job_for_existing_id(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        job = get_job(tmp_db, "job-1")
        assert job is not None
        assert job.id == "job-1"

    def test_returns_none_for_nonexistent_id(self, tmp_db: sqlite3.Connection) -> None:
        result = get_job(tmp_db, "does-not-exist")
        assert result is None

    def test_returned_job_has_correct_fields(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-x", "sleep 1", max_retries=7)
        job = get_job(tmp_db, "job-x")
        assert job.command == "sleep 1"
        assert job.max_retries == 7
        assert job.attempts == 0


# ---------------------------------------------------------------------------
# TestClaimJob
# ---------------------------------------------------------------------------

class TestClaimJob:

    def test_returns_none_when_queue_is_empty(self, tmp_db: sqlite3.Connection) -> None:
        result = claim_job(tmp_db, "worker-1")
        assert result is None

    def test_returns_job_when_pending_exists(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claimed = claim_job(tmp_db, "worker-1")
        assert claimed is not None
        assert claimed.id == "job-1"

    def test_claimed_state_is_processing(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claimed = claim_job(tmp_db, "worker-1")
        assert claimed.state == JobState.PROCESSING

    def test_claimed_job_has_correct_worker_id(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claimed = claim_job(tmp_db, "worker-99")
        assert claimed.worker_id == "worker-99"

    def test_claimed_job_has_picked_at_set(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claimed = claim_job(tmp_db, "worker-1")
        assert claimed.picked_at is not None

    def test_claim_is_committed_and_visible(self, tmp_db: sqlite3.Connection) -> None:
        """
        The state change must be committed so other connections (workers)
        see it — this is the basic atomicity requirement.
        """
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claim_job(tmp_db, "worker-1")
        db_state = get_job(tmp_db, "job-1").state
        assert db_state == JobState.PROCESSING

    def test_second_claim_returns_none_with_one_job(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claim_job(tmp_db, "worker-1")
        second = claim_job(tmp_db, "worker-2")
        assert second is None

    def test_claim_is_fifo_oldest_first(self, tmp_db: sqlite3.Connection) -> None:
        """EDD Section 7: claim query orders by created_at ASC."""
        enqueue(tmp_db, "job-first", "echo first", max_retries=3)
        time.sleep(0.02)   # ensure distinct created_at timestamps
        enqueue(tmp_db, "job-second", "echo second", max_retries=3)

        claimed = claim_job(tmp_db, "worker-1")
        assert claimed.id == "job-first", (
            "Oldest pending job must be claimed first (FIFO)"
        )

    def test_skips_job_with_future_next_run_at(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Job whose backoff delay has not yet elapsed must not be claimed."""
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        _set_next_run_at(tmp_db, job.id, _future(3600))

        result = claim_job(tmp_db, "worker-1")
        assert result is None, "Job with future next_run_at must not be claimable"

    def test_claims_job_with_past_next_run_at(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Job whose backoff delay has elapsed must be claimable."""
        job = enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        _set_next_run_at(tmp_db, job.id, _past(60))

        claimed = claim_job(tmp_db, "worker-1")
        assert claimed is not None
        assert claimed.id == "job-1"

    def test_claims_job_with_null_next_run_at(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Freshly enqueued jobs (next_run_at IS NULL) are immediately eligible."""
        enqueue(tmp_db, "job-1", "echo hello", max_retries=3)
        claimed = claim_job(tmp_db, "worker-1")
        assert claimed is not None

    def test_returns_none_when_all_jobs_processing(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        enqueue(tmp_db, "job-2", "echo 2", max_retries=3)
        claim_job(tmp_db, "worker-1")
        claim_job(tmp_db, "worker-2")
        assert claim_job(tmp_db, "worker-3") is None

    def test_claim_skips_non_pending_states(self, tmp_db: sqlite3.Connection) -> None:
        """Completed and dead jobs must never be claimable."""
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        claim_job(tmp_db, "worker-1")
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="")

        result = claim_job(tmp_db, "worker-2")
        assert result is None


# ---------------------------------------------------------------------------
# TestMarkComplete
# ---------------------------------------------------------------------------

class TestMarkComplete:

    def _setup(self, conn: sqlite3.Connection) -> None:
        enqueue(conn, "job-1", "echo hello", max_retries=3)
        claim_job(conn, "worker-1")

    def test_state_becomes_completed(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="out", stderr="")
        assert get_job(tmp_db, "job-1").state == JobState.COMPLETED

    def test_exit_code_is_recorded(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="")
        assert get_job(tmp_db, "job-1").exit_code == 0

    def test_stdout_is_recorded(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="hello\n", stderr="")
        assert get_job(tmp_db, "job-1").stdout == "hello\n"

    def test_stderr_is_recorded(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="warn msg")
        assert get_job(tmp_db, "job-1").stderr == "warn msg"

    def test_finished_at_is_set(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="")
        assert get_job(tmp_db, "job-1").finished_at is not None

    def test_long_stdout_is_truncated_to_limit(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="x" * 10_000, stderr="")
        stored = get_job(tmp_db, "job-1").stdout
        assert len(stored) == OUTPUT_TRUNCATION_LIMIT

    def test_long_stderr_is_truncated_to_limit(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="e" * 10_000)
        stored = get_job(tmp_db, "job-1").stderr
        assert len(stored) == OUTPUT_TRUNCATION_LIMIT

    def test_short_output_is_not_truncated(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="short", stderr="also short")
        job = get_job(tmp_db, "job-1")
        assert job.stdout == "short"
        assert job.stderr == "also short"


# ---------------------------------------------------------------------------
# TestMarkFailed
# ---------------------------------------------------------------------------

class TestMarkFailed:

    def _setup(self, conn: sqlite3.Connection) -> None:
        enqueue(conn, "job-1", "false", max_retries=3)
        claim_job(conn, "worker-1")

    def test_state_becomes_failed(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        job = mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="err")
        assert job.state == JobState.FAILED

    def test_attempts_incremented_to_one(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        job = mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="")
        assert job.attempts == 1

    def test_attempts_incremented_across_retries(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "false", max_retries=5)
        for expected in range(1, 4):
            claim_job(tmp_db, "worker-1")
            job = mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="")
            assert job.attempts == expected
            if expected < 3:
                _reset_to_pending(tmp_db, "job-1")

    def test_exit_code_is_recorded(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        job = mark_failed(tmp_db, "job-1", exit_code=127, stdout="", stderr="not found")
        assert job.exit_code == 127

    def test_stderr_is_recorded(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        job = mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="error msg")
        assert job.stderr == "error msg"

    def test_finished_at_is_set(self, tmp_db: sqlite3.Connection) -> None:
        self._setup(tmp_db)
        mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="")
        assert get_job(tmp_db, "job-1").finished_at is not None

    def test_retries_exhausted_true_when_max_hit(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "false", max_retries=1)
        claim_job(tmp_db, "worker-1")
        job = mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="")
        assert job.retries_exhausted is True

    def test_retries_not_exhausted_when_below_max(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        self._setup(tmp_db)   # max_retries=3
        job = mark_failed(tmp_db, "job-1", exit_code=1, stdout="", stderr="")
        assert job.retries_exhausted is False  # attempts=1, max_retries=3

    def test_nonexistent_job_raises_job_not_found(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        with pytest.raises(JobNotFoundError):
            mark_failed(tmp_db, "no-such-job", exit_code=1, stdout="", stderr="")

    def test_not_found_error_contains_job_id(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        with pytest.raises(JobNotFoundError) as exc_info:
            mark_failed(tmp_db, "ghost-job", exit_code=1, stdout="", stderr="")
        assert "ghost-job" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TestListJobs
# ---------------------------------------------------------------------------

class TestListJobs:

    def test_empty_queue_returns_empty_list(self, tmp_db: sqlite3.Connection) -> None:
        assert list_jobs(tmp_db) == []

    def test_returns_all_jobs_without_filter(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        enqueue(tmp_db, "job-2", "echo 2", max_retries=3)
        assert len(list_jobs(tmp_db)) == 2

    def test_filter_by_pending(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        enqueue(tmp_db, "job-2", "echo 2", max_retries=3)
        claim_job(tmp_db, "worker-1")   # job-1 → processing

        pending = list_jobs(tmp_db, state=JobState.PENDING)
        assert len(pending) == 1
        assert pending[0].id == "job-2"

    def test_filter_by_processing(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        claim_job(tmp_db, "worker-1")

        processing = list_jobs(tmp_db, state=JobState.PROCESSING)
        assert len(processing) == 1
        assert processing[0].id == "job-1"

    def test_filter_by_completed(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        claim_job(tmp_db, "worker-1")
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="")

        completed = list_jobs(tmp_db, state=JobState.COMPLETED)
        assert len(completed) == 1

    def test_ordered_by_created_at_ascending(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-first", "echo first", max_retries=3)
        time.sleep(0.02)
        enqueue(tmp_db, "job-second", "echo second", max_retries=3)

        jobs = list_jobs(tmp_db)
        assert jobs[0].id == "job-first"
        assert jobs[1].id == "job-second"

    def test_empty_list_for_state_with_no_jobs(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        assert list_jobs(tmp_db, state=JobState.COMPLETED) == []

    def test_returns_job_objects(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        jobs = list_jobs(tmp_db)
        assert all(isinstance(j, Job) for j in jobs)


# ---------------------------------------------------------------------------
# TestGetJobCounts
# ---------------------------------------------------------------------------

class TestGetJobCounts:

    def test_empty_queue_all_states_zero(self, tmp_db: sqlite3.Connection) -> None:
        counts = get_job_counts(tmp_db)
        assert counts == {
            "pending": 0, "processing": 0, "completed": 0,
            "failed": 0, "dead": 0,
        }

    def test_all_five_states_always_present(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        counts = get_job_counts(tmp_db)
        assert set(counts.keys()) == {"pending", "processing", "completed", "failed", "dead"}

    def test_counts_reflect_actual_distribution(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        enqueue(tmp_db, "job-2", "echo 2", max_retries=3)
        enqueue(tmp_db, "job-3", "echo 3", max_retries=3)
        claim_job(tmp_db, "worker-1")   # job-1 → processing

        counts = get_job_counts(tmp_db)
        assert counts["pending"] == 2
        assert counts["processing"] == 1
        assert counts["completed"] == 0
        assert counts["failed"] == 0
        assert counts["dead"] == 0

    def test_counts_update_after_completion(self, tmp_db: sqlite3.Connection) -> None:
        enqueue(tmp_db, "job-1", "echo 1", max_retries=3)
        claim_job(tmp_db, "worker-1")
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="")

        counts = get_job_counts(tmp_db)
        assert counts["pending"] == 0
        assert counts["processing"] == 0
        assert counts["completed"] == 1
