"""
tests/test_concurrency.py — Multiprocessing concurrency stress test.

This file proves the core invariant of QueueCTL:
    *No two workers can claim the same job simultaneously.*

The proof is empirical: we spawn real OS processes that compete for a shared
SQLite database over the same file path.  If the atomic claim query
(UPDATE … WHERE id = (SELECT … LIMIT 1) RETURNING *) has any race condition,
it will surface here as a double-claim.

Why this matters:
    The single-process unit tests in test_job_service.py verify the claim
    query's *logic*.  This test verifies the *concurrency guarantee* under
    actual multiprocessing contention — the only way to prove SQLite's write
    serialization + WAL mode hold for multiple OS processes writing to the
    same file simultaneously.

Design:
    - Each worker process is a plain module-level function (_stress_worker)
      so it is picklable by multiprocessing on Windows (spawn start method).
    - Workers run until MAX_IDLE consecutive empty polls, then exit cleanly.
    - Results (job IDs each worker claimed) are returned via Pool.map so
      the parent can assert invariants without reading the DB again for proof.
    - The DB path is passed as a string — connections cannot cross process
      boundaries.
"""

import multiprocessing
import subprocess
import sys
import time
from pathlib import Path

import pytest

from core.job_service import enqueue, get_job, list_jobs
from storage.db import get_connection, initialize_schema

# ---------------------------------------------------------------------------
# Cross-platform command — guaranteed to work anywhere Python runs
# ---------------------------------------------------------------------------

_PY = sys.executable


def _cmd_fast_success() -> str:
    """A command that exits 0 in < 50ms."""
    return f'"{_PY}" -c "import sys; sys.exit(0)"'


# ---------------------------------------------------------------------------
# Stress worker — module-level so multiprocessing can pickle it on Windows
# ---------------------------------------------------------------------------

def _stress_worker(args: tuple) -> list:
    """
    Worker process entry point for concurrency tests.

    Polls until ``MAX_IDLE`` consecutive empty polls, then exits.
    Returns the list of job IDs this process successfully claimed and ran.

    Args (packed as a tuple for Pool.map compatibility):
        worker_id (str): Unique identifier for this worker.
        db_path   (str): Path to the shared SQLite file.
    """
    worker_id, db_path = args
    MAX_IDLE = 8          # quit after 8 consecutive empty polls
    POLL_SLEEP = 0.005    # 5ms — aggressive polling to maximise contention

    # Each process must create its own connection.
    conn = get_connection(Path(db_path))
    initialize_schema(conn)

    claimed: list[str] = []
    idle = 0

    while idle < MAX_IDLE:
        # Use the same atomic claim function the real Worker uses.
        from core.job_service import claim_job, mark_complete, mark_failed

        job = claim_job(conn, worker_id)
        if job is None:
            idle += 1
            time.sleep(POLL_SLEEP)
            continue

        idle = 0
        claimed.append(job.id)

        # Execute the job
        try:
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                mark_complete(
                    conn, job.id,
                    exit_code=0,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )
            else:
                from core.job_service import mark_failed
                from core.retry import handle_failure
                failed = mark_failed(
                    conn, job.id,
                    exit_code=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )
                handle_failure(conn, failed, backoff_base=2.0)
        except Exception:
            pass  # never let a worker exception abort the pool

    conn.close()
    return claimed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _enqueue_n(db_path: Path, n: int, command: str) -> list[str]:
    """Enqueue N jobs into the shared DB and return their IDs."""
    conn = get_connection(db_path)
    initialize_schema(conn)
    job_ids = []
    for i in range(n):
        job = enqueue(conn, f"stress-job-{i:04d}", command, max_retries=1)
        job_ids.append(job.id)
    conn.close()
    return job_ids


def _count_state(db_path: Path, state: str) -> int:
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE state = ?", (state,)
    ).fetchone()
    conn.close()
    return row[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAtomicClaim:
    """
    Prove that no two workers ever claim the same job.

    If the atomic claim query has a race condition, we'll see:
    - duplicate job IDs across workers (detected by comparing claimed lists)
    - or fewer completed jobs than enqueued (one worker's result overwrote
      another's)
    """

    def test_no_double_claim_equal_workers_and_jobs(
        self, tmp_db_path: Path
    ) -> None:
        """
        10 workers compete for exactly 10 jobs.
        Each job must be claimed by exactly ONE worker.
        """
        N = 10
        command = _cmd_fast_success()
        job_ids = _enqueue_n(tmp_db_path, N, command)

        args = [(f"worker-{i:02d}", str(tmp_db_path)) for i in range(N)]

        with multiprocessing.Pool(processes=N) as pool:
            results = pool.map(_stress_worker, args)

        # Flatten: collect every job_id claimed by any worker
        all_claimed: list[str] = [jid for worker in results for jid in worker]

        # ── Invariant 1: No job was claimed more than once ──────────────────
        claimed_set = set(all_claimed)
        assert len(all_claimed) == len(claimed_set), (
            f"DOUBLE-CLAIM DETECTED!\n"
            f"Total claims: {len(all_claimed)}, unique: {len(claimed_set)}\n"
            f"Duplicates: {[j for j in all_claimed if all_claimed.count(j) > 1]}"
        )

        # ── Invariant 2: All enqueued jobs were eventually claimed ───────────
        assert claimed_set == set(job_ids), (
            f"Some jobs were never claimed!\n"
            f"Missing: {set(job_ids) - claimed_set}"
        )

        # ── Invariant 3: All jobs are in completed state ─────────────────────
        completed = _count_state(tmp_db_path, "completed")
        assert completed == N, (
            f"Expected {N} completed jobs, got {completed}. "
            f"Pending={_count_state(tmp_db_path, 'pending')}, "
            f"Processing={_count_state(tmp_db_path, 'processing')}"
        )

    def test_no_double_claim_more_workers_than_jobs(
        self, tmp_db_path: Path
    ) -> None:
        """
        20 workers compete for 10 jobs — exactly half should be idle.
        This is the highest-contention scenario: every job has 2 workers
        racing for it simultaneously.
        """
        N_JOBS = 10
        N_WORKERS = 20
        command = _cmd_fast_success()
        job_ids = _enqueue_n(tmp_db_path, N_JOBS, command)

        args = [(f"worker-{i:02d}", str(tmp_db_path)) for i in range(N_WORKERS)]

        with multiprocessing.Pool(processes=N_WORKERS) as pool:
            results = pool.map(_stress_worker, args)

        all_claimed = [jid for worker in results for jid in worker]

        # ── No double-claims ──────────────────────────────────────────────
        assert len(all_claimed) == len(set(all_claimed)), (
            f"DOUBLE-CLAIM DETECTED under 2x worker contention!\n"
            f"Claims: {sorted(all_claimed)}"
        )

        # ── All jobs claimed ──────────────────────────────────────────────
        assert set(all_claimed) == set(job_ids)

        # ── All completed ──────────────────────────────────────────────────
        assert _count_state(tmp_db_path, "completed") == N_JOBS

    def test_no_double_claim_fewer_workers_than_jobs(
        self, tmp_db_path: Path
    ) -> None:
        """
        3 workers share 15 jobs — each worker claims ~5 jobs sequentially.
        Verifies that the FIFO ordering and repeated claiming within a single
        worker also produce no duplicates.
        """
        N_JOBS = 15
        N_WORKERS = 3
        command = _cmd_fast_success()
        job_ids = _enqueue_n(tmp_db_path, N_JOBS, command)

        args = [(f"worker-{i:02d}", str(tmp_db_path)) for i in range(N_WORKERS)]

        with multiprocessing.Pool(processes=N_WORKERS) as pool:
            results = pool.map(_stress_worker, args)

        all_claimed = [jid for worker in results for jid in worker]

        assert len(all_claimed) == len(set(all_claimed)), (
            "Double-claim in sequential-claim scenario"
        )
        assert set(all_claimed) == set(job_ids)
        assert _count_state(tmp_db_path, "completed") == N_JOBS

    def test_fifo_order_preserved_under_concurrent_load(
        self, tmp_db_path: Path
    ) -> None:
        """
        FIFO is best-effort under concurrency (workers race simultaneously),
        but each individual worker's claimed list must be in FIFO order
        (oldest ID first).  Verifies ORDER BY created_at ASC is respected.
        """
        N_JOBS = 6
        N_WORKERS = 2
        command = _cmd_fast_success()
        _enqueue_n(tmp_db_path, N_JOBS, command)

        args = [(f"worker-{i:02d}", str(tmp_db_path)) for i in range(N_WORKERS)]

        with multiprocessing.Pool(processes=N_WORKERS) as pool:
            results = pool.map(_stress_worker, args)

        # Each worker's own claim list must be in ascending job-ID order
        # (stress-job-0000 < stress-job-0001 < …) because the claim query
        # orders by created_at ASC and jobs were enqueued sequentially.
        for worker_idx, worker_claims in enumerate(results):
            if len(worker_claims) > 1:
                assert worker_claims == sorted(worker_claims), (
                    f"Worker {worker_idx} claimed jobs out of FIFO order: "
                    f"{worker_claims}"
                )

    def test_no_job_left_processing_after_workers_exit(
        self, tmp_db_path: Path
    ) -> None:
        """
        After all workers exit cleanly, no job must be stuck in 'processing'.
        A job in 'processing' after all workers are gone indicates a claim
        that was never completed — a correctness bug.
        """
        N = 8
        _enqueue_n(tmp_db_path, N, _cmd_fast_success())

        args = [(f"worker-{i:02d}", str(tmp_db_path)) for i in range(4)]

        with multiprocessing.Pool(processes=4) as pool:
            pool.map(_stress_worker, args)

        stuck = _count_state(tmp_db_path, "processing")
        assert stuck == 0, (
            f"{stuck} job(s) stuck in 'processing' after all workers exited"
        )
