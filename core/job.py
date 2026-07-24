"""
core/job.py — Job domain model and state machine.

Defines:
- ``JobState``   : valid state strings as named constants (avoids magic strings)
- ``VALID_TRANSITIONS`` : explicit state machine — every allowed transition is
  declared; everything else is forbidden by default.
- ``Job``        : dataclass mirroring the ``jobs`` table schema, with helpers
  for construction from a sqlite3.Row and for transition validation.

This module has zero dependencies on storage or config — it is pure domain
logic and can be unit-tested without a database.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# State constants — reference these everywhere; never use raw strings.
# ---------------------------------------------------------------------------

class JobState:
    PENDING:    str = "pending"
    PROCESSING: str = "processing"
    COMPLETED:  str = "completed"
    FAILED:     str = "failed"
    DEAD:       str = "dead"

    ALL: frozenset[str] = frozenset(
        {"pending", "processing", "completed", "failed", "dead"}
    )


# ---------------------------------------------------------------------------
# State machine — explicit allowed transitions.
#
# Reading the table:
#   pending    → processing          (worker claims the job)
#   processing → completed           (exit code 0)
#   processing → failed              (non-zero exit / timeout)
#   processing → pending             (retry reschedule: attempt < max_retries)
#   failed     → pending             (retry reschedule path, kept for clarity)
#   failed     → dead                (attempts >= max_retries → DLQ)
#   dead       → pending             (operator issues `dlq retry <id>`)
#   completed  → (nothing)           terminal state
# ---------------------------------------------------------------------------

VALID_TRANSITIONS: dict[str, frozenset[str]] = {
    JobState.PENDING:    frozenset({JobState.PROCESSING}),
    JobState.PROCESSING: frozenset({JobState.COMPLETED, JobState.FAILED, JobState.PENDING}),
    JobState.FAILED:     frozenset({JobState.PENDING, JobState.DEAD}),
    JobState.COMPLETED:  frozenset(),   # terminal — no outgoing transitions
    JobState.DEAD:       frozenset({JobState.PENDING}),  # only via explicit dlq retry
}

# Maximum characters stored for stdout/stderr per execution.
# Prevents runaway commands from bloating the database.
OUTPUT_TRUNCATION_LIMIT: int = 5_000


# ---------------------------------------------------------------------------
# Job dataclass
# ---------------------------------------------------------------------------

@dataclass
class Job:
    """
    Represents a single job and its current execution state.

    Field names and types mirror the ``jobs`` table schema exactly so that
    :meth:`from_row` is a mechanical mapping — no transformation hidden here.
    """

    id:          str
    command:     str
    state:       str
    attempts:    int
    max_retries: int
    next_run_at: Optional[str]   # ISO-8601 UTC or None (eligible immediately)
    worker_id:   Optional[str]   # identifier of the owning worker process
    exit_code:   Optional[int]   # most recent subprocess exit code
    stdout:      Optional[str]   # most recent stdout (truncated)
    stderr:      Optional[str]   # most recent stderr (truncated)
    created_at:  str
    picked_at:   Optional[str]   # when worker atomically claimed this job
    started_at:  Optional[str]   # when subprocess execution began
    finished_at: Optional[str]   # when execution ended
    updated_at:  str

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        """
        Construct a :class:`Job` from a :class:`sqlite3.Row`.

        Assumes the connection has ``row_factory = sqlite3.Row`` set,
        which :func:`storage.db.get_connection` guarantees.
        """
        return cls(
            id=row["id"],
            command=row["command"],
            state=row["state"],
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            next_run_at=row["next_run_at"],
            worker_id=row["worker_id"],
            exit_code=row["exit_code"],
            stdout=row["stdout"],
            stderr=row["stderr"],
            created_at=row["created_at"],
            picked_at=row["picked_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # State machine helpers
    # ------------------------------------------------------------------

    def can_transition_to(self, target_state: str) -> bool:
        """
        Return ``True`` if transitioning from :attr:`state` to
        *target_state* is a valid move in the job state machine.

        Args:
            target_state: The desired next state string.

        Returns:
            ``True`` if the transition is allowed, ``False`` otherwise.
        """
        return target_state in VALID_TRANSITIONS.get(self.state, frozenset())

    def assert_can_transition_to(self, target_state: str) -> None:
        """
        Raise :class:`ValueError` if the transition is not allowed.

        Use this at service-layer boundaries where an invalid transition
        indicates a programming error rather than bad user input.

        Args:
            target_state: The desired next state string.

        Raises:
            ValueError: If the transition is invalid.
        """
        if not self.can_transition_to(target_state):
            raise ValueError(
                f"Invalid state transition: {self.state!r} → {target_state!r} "
                f"(job_id={self.id!r})"
            )

    @property
    def is_terminal(self) -> bool:
        """Return ``True`` if the job is in a terminal state (no further transitions)."""
        return not VALID_TRANSITIONS.get(self.state, frozenset())

    @property
    def retries_exhausted(self) -> bool:
        """Return ``True`` if the job has consumed all allowed retry attempts."""
        return self.attempts >= self.max_retries
