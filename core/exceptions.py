"""
core/exceptions.py — Domain exceptions for QueueCTL.

All custom exceptions raised by the service layer live here.
CLI handlers catch these and translate them into clean, user-facing
error messages.  No raw sqlite3 exceptions or stack traces should
ever reach the user.
"""


class QueueCTLError(Exception):
    """Base class for all QueueCTL domain errors."""


class DuplicateJobError(QueueCTLError):
    """Raised when attempting to enqueue a job ID that already exists.

    Job IDs are immutable identifiers.  Rejecting duplicates preserves
    data integrity and prevents accidental overwrites of in-progress jobs.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(
            f"Job ID '{job_id}' already exists.  "
            "Job IDs are immutable — enqueue a new job with a unique ID."
        )
        self.job_id = job_id


class JobNotFoundError(QueueCTLError):
    """Raised when a referenced job ID does not exist in the database."""

    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job '{job_id}' not found.")
        self.job_id = job_id


class InvalidStateTransitionError(QueueCTLError):
    """Raised when a requested state transition violates the state machine.

    Used at service-layer boundaries where an invalid transition indicates
    a programming error, not bad user input.
    """

    def __init__(self, job_id: str, from_state: str, to_state: str) -> None:
        super().__init__(
            f"Invalid state transition for job '{job_id}': "
            f"'{from_state}' → '{to_state}'."
        )
        self.job_id = job_id
        self.from_state = from_state
        self.to_state = to_state
