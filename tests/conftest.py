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


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """
    Provide the *file path* to an initialized temporary SQLite database.

    Used by concurrency tests that must pass the DB location to child
    processes — sqlite3 connections cannot be shared across OS process
    boundaries, so each worker creates its own connection from this path.

    Yields:
        A :class:`pathlib.Path` pointing to the initialized DB file.
    """
    db_path = tmp_path / "concurrency_test.db"
    conn = get_connection(db_path)
    initialize_schema(conn)
    conn.close()
    return db_path
