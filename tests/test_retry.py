"""
tests/test_retry.py — Unit tests for core.retry and config.

Coverage:
  TestCalculateBackoff   — pure function; no DB needed
  TestScheduleRetry      — sets next_run_at and resets state
  TestMoveToDlq          — moves job to dead state
  TestHandleFailure      — correct routing: retry vs DLQ
  TestDlqRetry           — operator reset of dead job, error cases
  TestConfig             — CRUD, typed getters, key normalisation
"""

import sqlite3
from datetime import datetime, timezone, timedelta

import pytest

from config import (
    display_key,
    get_all_config,
    get_config_value,
    get_float_config,
    get_int_config,
    normalize_key,
    set_config_value,
)
from core.exceptions import InvalidStateTransitionError, JobNotFoundError
from core.job import JobState
from core.job_service import claim_job, enqueue, get_job, mark_failed
from core.retry import (
    DEFAULT_BACKOFF_BASE,
    MAX_BACKOFF_SECONDS,
    calculate_backoff,
    dlq_retry,
    handle_failure,
    move_to_dlq,
    schedule_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp string into an aware datetime."""
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _enqueue_and_fail(
    conn: sqlite3.Connection,
    job_id: str = "job-1",
    max_retries: int = 3,
) -> "Job":
    """Enqueue a job, claim it, and mark it failed once. Returns the updated job."""
    enqueue(conn, job_id, "false", max_retries=max_retries)
    claim_job(conn, "worker-1")
    return mark_failed(conn, job_id, exit_code=1, stdout="", stderr="err")


# ---------------------------------------------------------------------------
# TestCalculateBackoff — pure function; no database required
# ---------------------------------------------------------------------------

class TestCalculateBackoff:

    def test_base2_attempt1_returns_2(self) -> None:
        assert calculate_backoff(1, base=2) == 2.0

    def test_base2_attempt2_returns_4(self) -> None:
        assert calculate_backoff(2, base=2) == 4.0

    def test_base2_attempt3_returns_8(self) -> None:
        assert calculate_backoff(3, base=2) == 8.0

    def test_delay_capped_at_max(self) -> None:
        """Very high attempts must not exceed MAX_BACKOFF_SECONDS."""
        result = calculate_backoff(100, base=2)
        assert result == float(MAX_BACKOFF_SECONDS)

    def test_delay_capped_at_custom_cap(self) -> None:
        result = calculate_backoff(100, base=2, cap=60)
        assert result == 60.0

    def test_attempts_zero_returns_zero(self) -> None:
        """No delay for a job that has not been attempted yet."""
        assert calculate_backoff(0, base=2) == 0.0

    def test_negative_attempts_returns_zero(self) -> None:
        assert calculate_backoff(-5, base=2) == 0.0

    def test_base3_attempt2_returns_9(self) -> None:
        assert calculate_backoff(2, base=3) == 9.0

    def test_base1_always_returns_1(self) -> None:
        """Base of 1 produces constant delay of 1.0 for any positive attempts."""
        for n in range(1, 6):
            assert calculate_backoff(n, base=1) == 1.0

    def test_default_base_is_two(self) -> None:
        assert DEFAULT_BACKOFF_BASE == 2.0

    def test_default_cap_is_300(self) -> None:
        assert MAX_BACKOFF_SECONDS == 300

    def test_returns_float(self) -> None:
        assert isinstance(calculate_backoff(2, base=2), float)


# ---------------------------------------------------------------------------
# TestScheduleRetry
# ---------------------------------------------------------------------------

class TestScheduleRetry:

    def test_state_reset_to_pending(self, tmp_db: sqlite3.Connection) -> None:
        _enqueue_and_fail(tmp_db)
        schedule_retry(tmp_db, "job-1", delay_seconds=60.0)
        assert get_job(tmp_db, "job-1").state == JobState.PENDING

    def test_next_run_at_is_set(self, tmp_db: sqlite3.Connection) -> None:
        _enqueue_and_fail(tmp_db)
        schedule_retry(tmp_db, "job-1", delay_seconds=60.0)
        assert get_job(tmp_db, "job-1").next_run_at is not None

    def test_next_run_at_is_in_future(self, tmp_db: sqlite3.Connection) -> None:
        _enqueue_and_fail(tmp_db)
        schedule_retry(tmp_db, "job-1", delay_seconds=60.0)
        next_run = _parse_ts(get_job(tmp_db, "job-1").next_run_at)
        assert next_run > datetime.now(timezone.utc)

    def test_next_run_at_approximately_correct(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """next_run_at should be within ±5 seconds of (now + delay)."""
        delay = 120.0
        before = datetime.now(timezone.utc)
        _enqueue_and_fail(tmp_db)
        schedule_retry(tmp_db, "job-1", delay_seconds=delay)
        after = datetime.now(timezone.utc)

        next_run = _parse_ts(get_job(tmp_db, "job-1").next_run_at)
        expected_low  = before + timedelta(seconds=delay - 5)
        expected_high = after  + timedelta(seconds=delay + 5)
        assert expected_low <= next_run <= expected_high

    def test_zero_delay_makes_job_immediately_claimable(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        _enqueue_and_fail(tmp_db)
        schedule_retry(tmp_db, "job-1", delay_seconds=0.0)
        # With 0 delay next_run_at = now → job should be claimable
        claimed = claim_job(tmp_db, "worker-2")
        assert claimed is not None
        assert claimed.id == "job-1"


# ---------------------------------------------------------------------------
# TestMoveToDlq
# ---------------------------------------------------------------------------

class TestMoveToDlq:

    def test_state_becomes_dead(self, tmp_db: sqlite3.Connection) -> None:
        _enqueue_and_fail(tmp_db)
        move_to_dlq(tmp_db, "job-1")
        assert get_job(tmp_db, "job-1").state == JobState.DEAD

    def test_dead_job_is_not_claimable(self, tmp_db: sqlite3.Connection) -> None:
        _enqueue_and_fail(tmp_db)
        move_to_dlq(tmp_db, "job-1")
        result = claim_job(tmp_db, "worker-2")
        assert result is None


# ---------------------------------------------------------------------------
# TestHandleFailure
# ---------------------------------------------------------------------------

class TestHandleFailure:

    def test_schedules_retry_when_retries_not_exhausted(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        job = _enqueue_and_fail(tmp_db)  # attempts=1, max_retries=3
        assert not job.retries_exhausted

        handle_failure(tmp_db, job, backoff_base=2.0)

        updated = get_job(tmp_db, "job-1")
        assert updated.state == JobState.PENDING
        assert updated.next_run_at is not None

    def test_moves_to_dlq_when_retries_exhausted(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        job = _enqueue_and_fail(tmp_db, max_retries=1)  # attempts=1, max_retries=1
        assert job.retries_exhausted

        handle_failure(tmp_db, job, backoff_base=2.0)

        assert get_job(tmp_db, "job-1").state == JobState.DEAD

    def test_backoff_base_affects_next_run_at(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """A larger backoff base must produce a later next_run_at."""
        # Job A — base 2
        enqueue(tmp_db, "job-a", "false", max_retries=3)
        claim_job(tmp_db, "worker-1")
        job_a = mark_failed(tmp_db, "job-a", exit_code=1, stdout="", stderr="")
        handle_failure(tmp_db, job_a, backoff_base=2.0)

        # Job B — base 10
        enqueue(tmp_db, "job-b", "false", max_retries=3)
        claim_job(tmp_db, "worker-1")
        job_b = mark_failed(tmp_db, "job-b", exit_code=1, stdout="", stderr="")
        handle_failure(tmp_db, job_b, backoff_base=10.0)

        a_next = _parse_ts(get_job(tmp_db, "job-a").next_run_at)
        b_next = _parse_ts(get_job(tmp_db, "job-b").next_run_at)
        assert b_next > a_next, "Larger backoff base must produce a later next_run_at"


# ---------------------------------------------------------------------------
# TestDlqRetry
# ---------------------------------------------------------------------------

class TestDlqRetry:

    def _make_dead_job(self, conn: sqlite3.Connection, job_id: str = "job-1") -> None:
        """Helper: enqueue, fail until exhausted, then move to DLQ."""
        _enqueue_and_fail(conn, job_id, max_retries=1)
        move_to_dlq(conn, job_id)

    def test_resets_state_to_pending(self, tmp_db: sqlite3.Connection) -> None:
        self._make_dead_job(tmp_db)
        job = dlq_retry(tmp_db, "job-1")
        assert job.state == JobState.PENDING

    def test_resets_attempts_to_zero(self, tmp_db: sqlite3.Connection) -> None:
        self._make_dead_job(tmp_db)
        job = dlq_retry(tmp_db, "job-1")
        assert job.attempts == 0

    def test_resets_next_run_at_to_null(self, tmp_db: sqlite3.Connection) -> None:
        self._make_dead_job(tmp_db)
        job = dlq_retry(tmp_db, "job-1")
        assert job.next_run_at is None

    def test_returns_updated_job(self, tmp_db: sqlite3.Connection) -> None:
        self._make_dead_job(tmp_db)
        result = dlq_retry(tmp_db, "job-1")
        from core.job import Job
        assert isinstance(result, Job)
        assert result.id == "job-1"

    def test_job_is_claimable_after_dlq_retry(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        self._make_dead_job(tmp_db)
        dlq_retry(tmp_db, "job-1")
        claimed = claim_job(tmp_db, "worker-fresh")
        assert claimed is not None
        assert claimed.id == "job-1"

    def test_raises_job_not_found_for_missing_id(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        with pytest.raises(JobNotFoundError):
            dlq_retry(tmp_db, "does-not-exist")

    def test_not_found_error_contains_job_id(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        with pytest.raises(JobNotFoundError) as exc_info:
            dlq_retry(tmp_db, "ghost")
        assert "ghost" in str(exc_info.value)

    def test_raises_invalid_transition_for_pending_job(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        with pytest.raises(InvalidStateTransitionError):
            dlq_retry(tmp_db, "job-1")

    def test_raises_invalid_transition_for_processing_job(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        claim_job(tmp_db, "worker-1")
        with pytest.raises(InvalidStateTransitionError):
            dlq_retry(tmp_db, "job-1")

    def test_raises_invalid_transition_for_completed_job(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        from core.job_service import mark_complete
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        claim_job(tmp_db, "worker-1")
        mark_complete(tmp_db, "job-1", exit_code=0, stdout="", stderr="")
        with pytest.raises(InvalidStateTransitionError):
            dlq_retry(tmp_db, "job-1")

    def test_error_message_contains_state_info(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        enqueue(tmp_db, "job-1", "echo hi", max_retries=3)
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            dlq_retry(tmp_db, "job-1")
        assert "pending" in str(exc_info.value).lower() or "job-1" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TestConfig
# ---------------------------------------------------------------------------

class TestConfig:

    # --- Key normalisation ---

    def test_normalize_hyphenated_max_retries(self) -> None:
        assert normalize_key("max-retries") == "max_retries"

    def test_normalize_hyphenated_backoff_base(self) -> None:
        assert normalize_key("backoff-base") == "backoff_base"

    def test_normalize_hyphenated_timeout(self) -> None:
        assert normalize_key("timeout-seconds") == "timeout_seconds"

    def test_normalize_hyphenated_poll_interval(self) -> None:
        assert normalize_key("poll-interval-ms") == "poll_interval_ms"

    def test_normalize_already_underscored_key(self) -> None:
        assert normalize_key("max_retries") == "max_retries"

    def test_display_key_round_trips(self) -> None:
        for cli_key in ("max-retries", "backoff-base", "timeout-seconds", "poll-interval-ms"):
            db_key = normalize_key(cli_key)
            assert display_key(db_key) == cli_key

    # --- get_config_value ---

    def test_get_existing_key_returns_value(self, tmp_db: sqlite3.Connection) -> None:
        value = get_config_value(tmp_db, "max_retries")
        assert value == "3"

    def test_get_missing_key_returns_none(self, tmp_db: sqlite3.Connection) -> None:
        assert get_config_value(tmp_db, "nonexistent_key") is None

    def test_get_missing_key_returns_default(self, tmp_db: sqlite3.Connection) -> None:
        result = get_config_value(tmp_db, "nonexistent_key", default="42")
        assert result == "42"

    # --- set_config_value ---

    def test_set_updates_existing_key(self, tmp_db: sqlite3.Connection) -> None:
        set_config_value(tmp_db, "max_retries", "10")
        assert get_config_value(tmp_db, "max_retries") == "10"

    def test_set_is_idempotent(self, tmp_db: sqlite3.Connection) -> None:
        set_config_value(tmp_db, "max_retries", "5")
        set_config_value(tmp_db, "max_retries", "5")
        assert get_config_value(tmp_db, "max_retries") == "5"

    def test_set_overwrites_previous_value(self, tmp_db: sqlite3.Connection) -> None:
        set_config_value(tmp_db, "max_retries", "5")
        set_config_value(tmp_db, "max_retries", "7")
        assert get_config_value(tmp_db, "max_retries") == "7"

    # --- get_all_config ---

    def test_get_all_returns_all_seeded_keys(self, tmp_db: sqlite3.Connection) -> None:
        config = get_all_config(tmp_db)
        assert set(config.keys()) == {
            "max_retries", "backoff_base", "timeout_seconds", "poll_interval_ms"
        }

    def test_get_all_returns_correct_defaults(self, tmp_db: sqlite3.Connection) -> None:
        config = get_all_config(tmp_db)
        assert config["max_retries"] == "3"
        assert config["backoff_base"] == "2"

    # --- get_int_config ---

    def test_get_int_returns_integer(self, tmp_db: sqlite3.Connection) -> None:
        result = get_int_config(tmp_db, "max_retries", default=3)
        assert result == 3
        assert isinstance(result, int)

    def test_get_int_returns_default_for_missing_key(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        assert get_int_config(tmp_db, "missing_key", default=99) == 99

    def test_get_int_returns_default_for_non_int_value(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        set_config_value(tmp_db, "max_retries", "not_a_number")
        result = get_int_config(tmp_db, "max_retries", default=3)
        assert result == 3

    # --- get_float_config ---

    def test_get_float_returns_float(self, tmp_db: sqlite3.Connection) -> None:
        result = get_float_config(tmp_db, "backoff_base", default=2.0)
        assert result == 2.0
        assert isinstance(result, float)

    def test_get_float_returns_default_for_missing_key(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        assert get_float_config(tmp_db, "missing_key", default=1.5) == 1.5

    def test_get_float_parses_integer_string_as_float(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        set_config_value(tmp_db, "backoff_base", "3")
        result = get_float_config(tmp_db, "backoff_base", default=2.0)
        assert result == 3.0
