"""
storage/db.py — Database connection management and schema initialization.

Responsibilities:
- Resolve the database file path (env var override → CWD default)
- Provide configured SQLite connections (WAL, busy_timeout, row_factory)
- Initialize the schema idempotently on first use

Design notes:
- Each worker process calls get_connection() independently; connections
  are never shared across OS process boundaries.
- WAL mode allows concurrent readers alongside a single writer, which
  is the exact access pattern of N polling workers + occasional CLI reads.
- busy_timeout=5000 makes SQLite retry write-lock acquisition for up to
  5 seconds before raising an error, eliminating spurious "database is
  locked" crashes under normal multi-worker contention.
"""

import logging
import os
import sqlite3
from pathlib import Path
from typing import Optional

from storage import queries

logger = logging.getLogger(__name__)

_DEFAULT_DB_NAME: str = "queuectl.db"


def get_db_path() -> Path:
    """
    Return the resolved path to the SQLite database file.

    Resolution order:
      1. ``QUEUECTL_DB`` environment variable (absolute or relative path)
      2. ``./queuectl.db`` in the current working directory

    Returns:
        Absolute Path to the database file.
    """
    env_path = os.environ.get("QUEUECTL_DB")
    if env_path:
        return Path(env_path).resolve()
    return Path.cwd() / _DEFAULT_DB_NAME


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """
    Create and return a configured SQLite connection.

    Applied configuration:
    - ``PRAGMA journal_mode=WAL``:  improves concurrent read/write throughput
      for the multi-worker polling pattern.
    - ``PRAGMA busy_timeout=5000``: retry lock acquisition for up to 5 s before
      raising ``sqlite3.OperationalError``, eliminating false "locked" errors.
    - ``row_factory = sqlite3.Row``:  rows behave like dicts
      (``row["column_name"]``), which simplifies deserialization into Job objects.

    Args:
        db_path: Optional explicit path to the database file.  When omitted,
                 ``get_db_path()`` is called to resolve the default location.

    Returns:
        A fully configured :class:`sqlite3.Connection`.
    """
    path: Path = db_path if db_path is not None else get_db_path()

    # Ensure parent directories exist (handles first-run and nested paths).
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    logger.debug("Database connection opened: %s", path)
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    """
    Create all tables and indexes if they do not already exist, then seed
    default configuration values.

    This function is **idempotent** — safe to call on every startup without
    risk of data loss or duplicate rows.  Schema creation uses
    ``CREATE ... IF NOT EXISTS``; config seeding uses ``INSERT OR IGNORE``.

    Args:
        conn: An active :class:`sqlite3.Connection` obtained from
              :func:`get_connection`.
    """
    with conn:
        conn.execute(queries.CREATE_JOBS_TABLE)
        conn.execute(queries.CREATE_CONFIG_TABLE)
        conn.execute(queries.CREATE_JOBS_INDEX)

        for statement in queries.SEED_DEFAULT_CONFIG:
            conn.execute(statement)

    logger.info("Database schema initialized")
