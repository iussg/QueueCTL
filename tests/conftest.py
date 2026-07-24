"""
tests/conftest.py — Shared pytest fixtures for the QueueCTL test suite.

Fixtures defined here are available to all test modules automatically.
"""

import sqlite3
from pathlib import Path

import pytest

from storage.db import get_connection, initialize_schema


@pytest.fixture
def tmp_db(tmp_path: Path) -> sqlite3.Connection:
    """
    Provide an initialized SQLite connection backed by a temporary file.

    The file is isolated per test (pytest's ``tmp_path`` is unique per test
    invocation), so tests never share state.  The connection is closed after
    the test completes.

    Yields:
        A :class:`sqlite3.Connection` with schema fully initialized.
    """
    db_path = tmp_path / "test_queuectl.db"
    conn = get_connection(db_path)
    initialize_schema(conn)
    yield conn
    conn.close()
