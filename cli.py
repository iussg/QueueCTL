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

import logging
import sys

import click

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

    JOB_JSON must be a JSON object with at least an "id" and "command" field.

    Example:

        queuectl enqueue '{"id":"job1","command":"sleep 2"}'
    """
    # Implemented in Phase 2 (job_service)
    click.echo("enqueue: not yet implemented", err=True)
    sys.exit(1)


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
    """Show queue and worker status summary."""
    # Implemented in Phase 2 (job_service)
    click.echo("status: not yet implemented", err=True)
    sys.exit(1)


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
def list_jobs(state: str | None) -> None:
    """List jobs, optionally filtered by state."""
    # Implemented in Phase 2 (job_service)
    click.echo("list: not yet implemented", err=True)
    sys.exit(1)


# ---------------------------------------------------------------------------
# queuectl dlq (subgroup)
# ---------------------------------------------------------------------------

@cli.group("dlq")
def dlq_group() -> None:
    """Manage the Dead Letter Queue."""


@dlq_group.command("list")
def dlq_list() -> None:
    """List all jobs in the Dead Letter Queue."""
    # Implemented in Phase 2 (job_service)
    click.echo("dlq list: not yet implemented", err=True)
    sys.exit(1)


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
