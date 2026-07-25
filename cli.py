"""
cli.py — QueueCTL command-line interface entrypoint.

This module is the CLI boundary only.  It:
  - Parses commands and validates user input
  - Delegates all business logic to the core layer
  - Formats and prints output

No SQL, no subprocess calls, no business logic lives here.

Command surface (per EDD Section 8):
  queuectl enqueue '<json>'
  queuectl worker start [--count N]
  queuectl worker stop
  queuectl status
  queuectl list [--state <state>]
  queuectl dlq list
  queuectl dlq retry <job-id>
  queuectl config set <key> <value>
"""

import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Optional

import click

from config import (
    KNOWN_KEYS,
    display_key,
    get_all_config,
    get_config_value,
    normalize_key,
    set_config_value,
)
from core.exceptions import (
    DuplicateJobError,
    InvalidStateTransitionError,
    JobNotFoundError,
)
from core.job import JobState
from core.job_service import (
    enqueue as svc_enqueue,
    get_job_counts,
    list_jobs as svc_list_jobs,
)
from core.retry import dlq_retry as svc_dlq_retry
from storage.db import get_connection, initialize_schema

# ---------------------------------------------------------------------------
# Config key validation helpers
# ---------------------------------------------------------------------------

# CLI-style keys accepted by `config set` and `config get`.
_VALID_CLI_KEYS: frozenset[str] = frozenset(
    {"max-retries", "backoff-base", "timeout-seconds", "poll-interval-ms"}
)

# Per-key type constraints used when validating `config set` values.
_KEY_CONSTRAINTS: dict[str, dict] = {
    "max_retries":     {"type": int,   "min": 0},
    "backoff_base":    {"type": float, "min": 1.0},
    "timeout_seconds": {"type": int,   "min": 1},
    "poll_interval_ms":{"type": int,   "min": 100},
}


def _validate_config_value(db_key: str, raw: str) -> str:
    """
    Validate *raw* against the type and minimum constraint for *db_key*.

    Returns *raw* unchanged if valid.
    Raises :class:`click.ClickException` with a descriptive message if not.
    """
    constraint = _KEY_CONSTRAINTS.get(db_key)
    if constraint is None:
        return raw   # unknown key — no validation

    expected_type = constraint["type"]
    min_val = constraint["min"]
    try:
        parsed = expected_type(raw)
    except ValueError:
        raise click.ClickException(
            f"Invalid value for '{display_key(db_key)}': "
            f"expected {expected_type.__name__}, got {raw!r}."
        )
    if parsed < min_val:
        raise click.ClickException(
            f"Invalid value for '{display_key(db_key)}': "
            f"must be >= {min_val}, got {parsed}."
        )
    return raw

# ---------------------------------------------------------------------------
# Logging configuration
# EDD Section 9 format: [LEVEL] timestamp message
# Example: [INFO] 2026-07-23T10:15:03Z Worker-2 Job17 attempt=2 claimed
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)sZ %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PID file helpers — used by worker start / worker stop
# ---------------------------------------------------------------------------

def _pid_file_path() -> Path:
    """
    Return the path to the worker PID file.

    The file is placed in the same directory as the SQLite DB so that
    ``worker stop`` can locate it without any extra configuration.
    """
    from storage.db import get_db_path
    return get_db_path().parent / "queuectl_workers.json"


def _write_pid_file(pids: list[int]) -> None:
    """Persist the PIDs of all running workers to disk."""
    import json
    pid_file = _pid_file_path()
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(
        json.dumps({"pids": pids, "count": len(pids)}),
        encoding="utf-8",
    )
    logger.info("PID file written: %s (pids=%s)", pid_file, pids)


def _remove_pid_file() -> None:
    """Delete the PID file after all workers have stopped."""
    pid_file = _pid_file_path()
    pid_file.unlink(missing_ok=True)


def _read_pid_file() -> list[int] | None:
    """Return the PIDs from the PID file, or None if the file does not exist."""
    import json
    pid_file = _pid_file_path()
    if not pid_file.exists():
        return None
    try:
        data = json.loads(pid_file.read_text(encoding="utf-8"))
        return data.get("pids", [])
    except (json.JSONDecodeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Root command group
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(version="0.1.0", prog_name="queuectl")
def cli() -> None:
    """QueueCTL — CLI-based background job queue."""


# ---------------------------------------------------------------------------
# queuectl enqueue
# ---------------------------------------------------------------------------

@cli.command("enqueue")
@click.argument("job_json")
def enqueue(job_json: str) -> None:
    """Enqueue a new job from a JSON specification.

    JOB_JSON must be a JSON object with at least "id" and "command" fields.
    An optional "max_retries" integer field overrides the queue default.

    Example:

        queuectl enqueue '{"id":"job1","command":"sleep 2"}'
    """
    # --- Parse ---
    try:
        data = json.loads(job_json)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Invalid JSON: {exc}")

    if not isinstance(data, dict):
        raise click.ClickException("JOB_JSON must be a JSON object, not a scalar or array.")

    job_id: str = data.get("id", "").strip()
    command: str = data.get("command", "").strip()

    if not job_id:
        raise click.ClickException("Missing required field: 'id'")
    if not command:
        raise click.ClickException("Missing required field: 'command'")

    max_retries = data.get("max_retries", 3)
    if not isinstance(max_retries, int) or max_retries < 0:
        raise click.ClickException("'max_retries' must be a non-negative integer.")

    # --- Execute ---
    conn = get_connection()
    initialize_schema(conn)
    try:
        job = svc_enqueue(conn, job_id, command, max_retries)
        click.echo(
            f"Enqueued  id={job.id!r}  command={job.command!r}  "
            f"max_retries={job.max_retries}  state={job.state}"
        )
    except DuplicateJobError as exc:
        raise click.ClickException(str(exc))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# queuectl worker (subgroup)
# ---------------------------------------------------------------------------

@cli.group("worker")
def worker_group() -> None:
    """Manage worker processes."""


@worker_group.command("start")
@click.option(
    "--count", "-n",
    default=1,
    show_default=True,
    type=click.IntRange(min=1),
    help="Number of worker processes to spawn.",
)
def worker_start(count: int) -> None:
    """Start N worker processes and block until interrupted.

    Each worker polls the queue independently, claims jobs atomically,
    and executes them as subprocesses.

    Worker PIDs are written to a file alongside the database so that
    ``queuectl worker stop`` (run from a second terminal) can locate
    and gracefully signal them.

    Press Ctrl+C in this terminal to stop all workers after the current
    job finishes (same effect as ``worker stop``).

    Examples:

        queuectl worker start          # single worker (default)
        queuectl worker start -n 4     # 4 parallel workers
    """
    import multiprocessing
    import os
    from core.worker import _worker_process_main

    if count == 1:
        pid = os.getpid()
        worker_id = f"worker-{pid}"
        click.echo(f"Starting 1 worker (pid={pid})...  Press Ctrl+C or run 'queuectl worker stop' to stop.")
        _write_pid_file([pid])
        try:
            _worker_process_main(worker_id)
        finally:
            _remove_pid_file()
        return

    # Spawn N worker processes.
    click.echo(f"Starting {count} workers...  Press Ctrl+C or run 'queuectl worker stop' to stop all.")
    processes: list[multiprocessing.Process] = []
    for i in range(count):
        worker_id = f"worker-{os.getpid()}-{i}"
        p = multiprocessing.Process(
            target=_worker_process_main,
            args=(worker_id,),
            name=worker_id,
            daemon=False,
        )
        p.start()
        click.echo(f"  Started {worker_id}  (pid={p.pid})")
        processes.append(p)

    _write_pid_file([p.pid for p in processes if p.pid is not None])
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        click.echo("\nCtrl+C received — waiting for current jobs to finish...")
        for p in processes:
            p.join(timeout=30)
        for p in processes:
            if p.is_alive():
                click.echo(f"  Force-terminating {p.name} (pid={p.pid})")
                p.terminate()
    finally:
        _remove_pid_file()


@worker_group.command("stop")
@click.option(
    "--timeout", "-t",
    default=30,
    show_default=True,
    type=click.IntRange(min=1),
    help="Seconds to wait for each worker to finish its current job before force-killing.",
)
def worker_stop(timeout: int) -> None:
    """Gracefully stop all workers started by 'worker start'.

    Reads the PID file written by 'worker start', sends each worker a
    termination signal, then waits up to TIMEOUT seconds for it to
    finish its current job and exit cleanly.

    On Unix/macOS: SIGTERM is sent — workers complete the current job
    before exiting (the signal handler sets a shutdown flag).

    On Windows: TerminateProcess is used (immediate) because Windows
    does not support inter-process SIGTERM delivery.

    After TIMEOUT seconds any worker that has not yet exited is
    force-killed with SIGKILL (Unix) or TerminateProcess (Windows).

    Run from a second terminal while 'worker start' is blocking:

        queuectl worker stop
        queuectl worker stop --timeout 60   # allow up to 60 s
    """
    import json
    import time

    pids = _read_pid_file()
    if pids is None:
        raise click.ClickException(
            "No worker PID file found.  Either no workers are running or "
            "the file was cleaned up already."
        )
    if not pids:
        click.echo("PID file exists but is empty — nothing to stop.")
        _remove_pid_file()
        return

    click.echo(f"Stopping {len(pids)} worker(s): {pids}")

    # ── Send graceful shutdown signal ────────────────────────────────────
    for pid in pids:
        try:
            if sys.platform == "win32":
                # TerminateProcess — immediate on Windows.
                # Workers will not finish their current job, but the
                # process does stop.  Orphaned jobs are recovered on
                # next startup via crash recovery.
                import ctypes
                ctypes.windll.kernel32.TerminateProcess(
                    ctypes.windll.kernel32.OpenProcess(1, False, pid), 1
                )
            else:
                os.kill(pid, signal.SIGTERM)
            click.echo(f"  Signalled pid {pid}")
        except ProcessLookupError:
            click.echo(f"  pid {pid}: already stopped")
        except PermissionError:
            click.echo(f"  pid {pid}: permission denied — skipping")

    # ── Wait for processes to exit ────────────────────────────────────────
    click.echo(f"Waiting up to {timeout}s for workers to finish current jobs...")
    deadline = time.monotonic() + timeout
    remaining = list(pids)

    while remaining and time.monotonic() < deadline:
        time.sleep(0.5)
        still_alive = []
        for pid in remaining:
            try:
                os.kill(pid, 0)          # signal 0 = existence check, no-op
                still_alive.append(pid)
            except (ProcessLookupError, PermissionError):
                click.echo(f"  pid {pid}: stopped cleanly")
        remaining = still_alive

    # ── Force-kill any that didn't stop in time ───────────────────────────
    if remaining:
        click.echo(
            f"  {len(remaining)} worker(s) did not stop within {timeout}s "
            f"— force-killing: {remaining}"
        )
        for pid in remaining:
            try:
                if sys.platform == "win32":
                    import ctypes
                    ctypes.windll.kernel32.TerminateProcess(
                        ctypes.windll.kernel32.OpenProcess(1, False, pid), 1
                    )
                else:
                    os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    _remove_pid_file()
    click.echo("All workers stopped.")
    logger.info("worker stop complete | pids=%s", pids)


# ---------------------------------------------------------------------------
# queuectl status
# ---------------------------------------------------------------------------

@cli.command("status")
def status() -> None:
    """Show a summary of job counts by state.

    This command is read-only — it never modifies the database.
    """
    conn = get_connection()
    initialize_schema(conn)
    counts = get_job_counts(conn)
    conn.close()

    click.echo("")
    click.echo(f"  Pending        : {counts['pending']}")
    click.echo(f"  Processing     : {counts['processing']}")
    click.echo(f"  Completed      : {counts['completed']}")
    click.echo(f"  Failed         : {counts['failed']}")
    click.echo(f"  Dead (DLQ)     : {counts['dead']}")
    click.echo("")


# ---------------------------------------------------------------------------
# queuectl list
# ---------------------------------------------------------------------------

@cli.command("list")
@click.option(
    "--state", "-s",
    default=None,
    type=click.Choice(
        ["pending", "processing", "completed", "failed", "dead"],
        case_sensitive=False,
    ),
    help="Filter jobs by state.  Omit to list all jobs.",
)
def list_jobs(state: Optional[str]) -> None:
    """List jobs, optionally filtered by state."""
    conn = get_connection()
    initialize_schema(conn)
    jobs = svc_list_jobs(conn, state=state)
    conn.close()

    if not jobs:
        click.echo("No jobs found.")
        return

    # Table header
    header = (
        f"  {'ID':<24} {'STATE':<12} {'ATTEMPTS':>8}  "
        f"{'COMMAND':<40}  CREATED AT"
    )
    click.echo("")
    click.echo(header)
    click.echo("  " + "-" * (len(header) - 2))

    for job in jobs:
        cmd_display = (
            job.command[:37] + "..." if len(job.command) > 40 else job.command
        )
        click.echo(
            f"  {job.id:<24} {job.state:<12} {job.attempts:>8}  "
            f"{cmd_display:<40}  {job.created_at}"
        )
    click.echo("")


# ---------------------------------------------------------------------------
# queuectl dlq (subgroup)
# ---------------------------------------------------------------------------

@cli.group("dlq")
def dlq_group() -> None:
    """Manage the Dead Letter Queue."""


@dlq_group.command("list")
def dlq_list() -> None:
    """List all jobs in the Dead Letter Queue."""
    conn = get_connection()
    initialize_schema(conn)
    jobs = svc_list_jobs(conn, state=JobState.DEAD)
    conn.close()

    if not jobs:
        click.echo("Dead Letter Queue is empty.")
        return

    click.echo(f"\n  {len(jobs)} job(s) in the Dead Letter Queue:\n")
    for job in jobs:
        cmd_display = (
            job.command[:37] + "..." if len(job.command) > 40 else job.command
        )
        click.echo(
            f"  {job.id:<24} attempts={job.attempts}/{job.max_retries}  "
            f"command={cmd_display!r}"
        )
    click.echo("")


@dlq_group.command("retry")
@click.argument("job_id")
def dlq_retry(job_id: str) -> None:
    """Re-enqueue a dead job for another attempt.

    JOB_ID is reset to pending with attempts=0 — a fresh retry cycle
    after operator investigation.
    """
    conn = get_connection()
    initialize_schema(conn)
    try:
        job = svc_dlq_retry(conn, job_id)
        click.echo(
            f"Job '{job.id}' re-queued  state={job.state}  attempts={job.attempts}"
        )
    except JobNotFoundError as exc:
        raise click.ClickException(str(exc))
    except InvalidStateTransitionError as exc:
        raise click.ClickException(str(exc))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# queuectl config
# ---------------------------------------------------------------------------

@cli.group("config")
def config_group() -> None:
    """View and modify queue configuration."""


@config_group.command("set")
@click.argument("key", metavar="KEY")
@click.argument("value", metavar="VALUE")
def config_set(key: str, value: str) -> None:
    """Set a configuration value.

    Supported keys and their constraints:

    \b
      max-retries       integer >= 0   (default 3)
      backoff-base      number  >= 1.0 (default 2)
      timeout-seconds   integer >= 1   (default 300)
      poll-interval-ms  integer >= 100 (default 500)

    Example:

        queuectl config set max-retries 5
    """
    if key not in _VALID_CLI_KEYS:
        valid = ", ".join(sorted(_VALID_CLI_KEYS))
        raise click.ClickException(
            f"Unknown config key: '{key}'.  Valid keys: {valid}"
        )
    db_key = normalize_key(key)
    _validate_config_value(db_key, value)  # raises ClickException on invalid

    conn = get_connection()
    initialize_schema(conn)
    try:
        set_config_value(conn, db_key, value)
        click.echo(f"  {key} = {value}")
    finally:
        conn.close()


@config_group.command("get")
@click.argument("key", metavar="KEY")
def config_get(key: str) -> None:
    """Show the current value of a configuration key.

    Supported keys: max-retries, backoff-base, timeout-seconds, poll-interval-ms

    Example:

        queuectl config get max-retries
    """
    if key not in _VALID_CLI_KEYS:
        valid = ", ".join(sorted(_VALID_CLI_KEYS))
        raise click.ClickException(
            f"Unknown config key: '{key}'.  Valid keys: {valid}"
        )
    db_key = normalize_key(key)

    conn = get_connection()
    initialize_schema(conn)
    value = get_config_value(conn, db_key)
    conn.close()

    if value is None:
        raise click.ClickException(f"Config key '{key}' has no value set.")
    click.echo(f"  {key} = {value}")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
