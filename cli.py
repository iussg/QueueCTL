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
import sys
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
# Format mirrors EDD Section 9: [LEVEL] timestamp source message
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)sZ %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)


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
    and executes them as subprocesses.  Press Ctrl+C to stop all workers
    after the current job finishes.

    Examples:

        queuectl worker start          # single worker (default)
        queuectl worker start -n 4     # 4 parallel workers
    """
    import multiprocessing
    import os
    from core.worker import Worker, _worker_process_main

    if count == 1:
        # Run directly in the foreground — no subprocess overhead,
        # output goes straight to the terminal.
        click.echo(f"Starting 1 worker (pid={os.getpid()})...  Press Ctrl+C to stop.")
        _worker_process_main(f"worker-{os.getpid()}")
        return

    # Spawn N worker processes.
    click.echo(f"Starting {count} workers...  Press Ctrl+C to stop all.")
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

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        click.echo("\nShutting down workers (waiting for current jobs to finish)...")
        # Workers have their own SIGINT handlers; just wait for them.
        for p in processes:
            p.join(timeout=30)
        # Force-terminate any that didn't stop cleanly.
        for p in processes:
            if p.is_alive():
                click.echo(f"  Force-terminating {p.name} (pid={p.pid})")
                p.terminate()


@worker_group.command("stop")
def worker_stop() -> None:
    """Gracefully stop all running worker processes.

    Workers respond to Ctrl+C / SIGINT by finishing the current job
    and then exiting.  This command is a placeholder; in production
    you would send SIGINT/SIGTERM to the worker PIDs directly.
    """
    click.echo(
        "Send Ctrl+C (SIGINT) to the worker process to stop it after "
        "the current job finishes."
    )


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
