"""
config.py — Queue configuration management.

Reads and writes configuration values from/to the ``config`` table in SQLite.
All settings live in the database — no INI files, no environment-variable
config (only the DB path uses an env var, handled in storage/db.py).

Design decisions:
- Values are stored as TEXT and parsed to int/float on demand.  SQLite has
  no typed columns for config, and keeping raw strings means we never lose
  the original value when reading back.
- ``INSERT OR CONFLICT ... DO UPDATE`` (UPSERT) makes set_config_value
  idempotent — safe to call repeatedly.
- Only the four keys seeded during schema initialisation are considered
  "known".  Other keys can be stored but callers are responsible for their
  own naming conventions.

Dependency direction: config → storage.queries, storage.db (never → core/)
"""

import logging
import sqlite3
from typing import Optional

from storage import queries

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known configuration keys (DB-style, underscore-separated)
# ---------------------------------------------------------------------------

KNOWN_KEYS: frozenset[str] = frozenset(
    {"max_retries", "backoff_base", "timeout_seconds", "poll_interval_ms"}
)

# CLI uses hyphenated keys; this mapping normalises them to DB-style.
_CLI_TO_DB_KEY: dict[str, str] = {
    "max-retries":      "max_retries",
    "backoff-base":     "backoff_base",
    "timeout-seconds":  "timeout_seconds",
    "poll-interval-ms": "poll_interval_ms",
}

# Inverse map for display purposes.
_DB_TO_CLI_KEY: dict[str, str] = {v: k for k, v in _CLI_TO_DB_KEY.items()}


# ---------------------------------------------------------------------------
# Key normalisation
# ---------------------------------------------------------------------------

def normalize_key(cli_key: str) -> str:
    """
    Convert a CLI-style hyphenated key to the DB-style underscore key.

    If *cli_key* is already DB-style (contains no hyphens or is unknown),
    it is returned unchanged.

    Examples::

        normalize_key("max-retries")    → "max_retries"
        normalize_key("max_retries")    → "max_retries"
        normalize_key("backoff-base")   → "backoff_base"
    """
    return _CLI_TO_DB_KEY.get(cli_key, cli_key.replace("-", "_"))


def display_key(db_key: str) -> str:
    """Return the CLI-style key for display (e.g. ``max_retries`` → ``max-retries``)."""
    return _DB_TO_CLI_KEY.get(db_key, db_key)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def get_config_value(
    conn: sqlite3.Connection,
    key: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Return the raw string value for *key* from the config table.

    Args:
        conn:    Active database connection.
        key:     DB-style key (e.g. ``"max_retries"``).
        default: Value returned when the key is not present.

    Returns:
        The stored string value, or *default* if the key does not exist.
    """
    row = conn.execute(queries.SELECT_CONFIG_VALUE, (key,)).fetchone()
    return row[0] if row else default


def set_config_value(conn: sqlite3.Connection, key: str, value: str) -> None:
    """
    Persist *value* for *key* in the config table (upsert).

    Args:
        conn:  Active database connection.
        key:   DB-style key (e.g. ``"max_retries"``).
        value: String representation of the new value.
    """
    with conn:
        conn.execute(queries.UPSERT_CONFIG_VALUE, (key, value))
    logger.info("Config updated | key=%s value=%s", key, value)


def get_all_config(conn: sqlite3.Connection) -> dict[str, str]:
    """
    Return all configuration entries as a ``{key: value}`` dict.

    Args:
        conn: Active database connection.

    Returns:
        Dict of all rows from the config table, ordered by key.
    """
    rows = conn.execute(queries.SELECT_ALL_CONFIG).fetchall()
    return {row["key"]: row["value"] for row in rows}


# ---------------------------------------------------------------------------
# Typed getters (convenience wrappers used by retry and worker)
# ---------------------------------------------------------------------------

def get_int_config(conn: sqlite3.Connection, key: str, default: int) -> int:
    """
    Return the config value for *key* parsed as an integer.

    Falls back to *default* if the key is absent or the value cannot
    be parsed.  A warning is logged on parse failure.
    """
    raw = get_config_value(conn, key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Config key '%s' has non-integer value %r — using default %d",
            key, raw, default,
        )
        return default


def get_float_config(conn: sqlite3.Connection, key: str, default: float) -> float:
    """
    Return the config value for *key* parsed as a float.

    Falls back to *default* if the key is absent or the value cannot
    be parsed.  A warning is logged on parse failure.
    """
    raw = get_config_value(conn, key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "Config key '%s' has non-float value %r — using default %f",
            key, raw, default,
        )
        return default
