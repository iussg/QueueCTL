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

from core.exceptions import DuplicateJobError, JobNotFoundError
from core.job import JobState
from core.job_service import (
    enqueue as svc_enqueue,
    get_job_counts,
    list_jobs as svc_list_jobs,
)
from storage.db import get_connection, initialize_schema

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
    """Start N worker processes."""
    # Implemented in Phase 4 (worker)
    click.echo("worker start: not yet implemented", err=True)
    sys.exit(1)


@worker_group.command("stop")
def worker_stop() -> None:
    """Gracefully stop all running worker processes."""
    # Implemented in Phase 4 (worker)
    click.echo("worker stop: not yet implemented", err=True)
    sys.exit(1)


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

    JOB_ID is reset to pending with attempts=0.
    """
    # Implemented in Phase 3 (retry)
    click.echo("dlq retry: not yet implemented", err=True)
    sys.exit(1)


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

    Supported keys: max-retries, backoff-base, timeout-seconds, poll-interval-ms

    Example:

        queuectl config set max-retries 5
    """
    # Implemented in Phase 3 (config)
    click.echo("config set: not yet implemented", err=True)
    sys.exit(1)


@config_group.command("get")
@click.argument("key", metavar="KEY")
def config_get(key: str) -> None:
    """Get the current value of a configuration key."""
    # Implemented in Phase 3 (config)
    click.echo("config get: not yet implemented", err=True)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
