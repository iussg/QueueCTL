"""
tests/test_schema.py — Tests for database schema creation, configuration, and
connection configuration.

Coverage:
  - Schema creation is idempotent (safe to call multiple times)
  - Both tables are created with the correct columns
  - The performance index is created
  - Default config values are seeded correctly
  - Seeding never overwrites existing user-set values
  - WAL journal mode is active on every connection
  - sqlite3.Row factory enables dict-like column access
  - Database file and parent directories are created on first connection
"""

import sqlite3
from pathlib import Path

import pytest

from storage.db import get_connection, initialize_schema


# ---------------------------------------------------------------------------
# Schema structure tests
# ---------------------------------------------------------------------------

class TestTableCreation:
    """Verify that initialize_schema creates the expected tables."""

    def test_jobs_table_exists(self, tmp_db: sqlite3.Connection) -> None:
        row = tmp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
        ).fetchone()
        assert row is not None, "jobs table must exist after schema initialization"

    def test_config_table_exists(self, tmp_db: sqlite3.Connection) -> None:
        row = tmp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='config'"
        ).fetchone()
        assert row is not None, "config table must exist after schema initialization"

    def test_jobs_index_exists(self, tmp_db: sqlite3.Connection) -> None:
        row = tmp_db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='index' AND name='idx_jobs_state_next_run'"
        ).fetchone()
        assert row is not None, "Performance index idx_jobs_state_next_run must exist"


class TestJobsTableColumns:
    """Verify the jobs table has exactly the schema defined in the EDD."""

    def _get_column_names(self, conn: sqlite3.Connection) -> set[str]:
        return {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}

    def test_all_required_columns_present(self, tmp_db: sqlite3.Connection) -> None:
        expected = {
            "id", "command", "state", "attempts", "max_retries",
            "next_run_at", "worker_id", "exit_code", "stdout", "stderr",
            "created_at", "picked_at", "started_at", "finished_at", "updated_at",
        }
        actual = self._get_column_names(tmp_db)
        missing = expected - actual
        extra   = actual - expected
        assert not missing, f"Missing columns in jobs table: {missing}"
        assert not extra,   f"Unexpected columns in jobs table: {extra}"

    def test_state_column_check_constraint_rejects_invalid(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """The CHECK constraint on state must reject values not in the allowed set."""
        now = "2026-01-01T00:00:00Z"
        with pytest.raises(sqlite3.IntegrityError):
            tmp_db.execute(
                "INSERT INTO jobs (id, command, state, created_at, updated_at) "
                "VALUES ('x','echo x','invalid_state',?,?)",
                (now, now),
            )
            tmp_db.commit()

    def test_id_is_primary_key(self, tmp_db: sqlite3.Connection) -> None:
        """Inserting a duplicate job ID must raise IntegrityError."""
        now = "2026-01-01T00:00:00Z"
        tmp_db.execute(
            "INSERT INTO jobs (id, command, state, created_at, updated_at) "
            "VALUES ('dup','echo a','pending',?,?)",
            (now, now),
        )
        tmp_db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            tmp_db.execute(
                "INSERT INTO jobs (id, command, state, created_at, updated_at) "
                "VALUES ('dup','echo b','pending',?,?)",
                (now, now),
            )
            tmp_db.commit()


class TestConfigTableColumns:
    """Verify the config table structure."""

    def test_config_columns_are_key_and_value(self, tmp_db: sqlite3.Connection) -> None:
        columns = {row[1] for row in tmp_db.execute("PRAGMA table_info(config)")}
        assert columns == {"key", "value"}

    def test_key_is_primary_key(self, tmp_db: sqlite3.Connection) -> None:
        """Duplicate config keys must raise IntegrityError."""
        tmp_db.execute("INSERT INTO config (key, value) VALUES ('test_key','1')")
        tmp_db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            tmp_db.execute("INSERT INTO config (key, value) VALUES ('test_key','2')")
            tmp_db.commit()


# ---------------------------------------------------------------------------
# Config seeding tests
# ---------------------------------------------------------------------------

class TestConfigSeeding:
    """Verify that default configuration values are seeded correctly."""

    def test_all_default_keys_are_present(self, tmp_db: sqlite3.Connection) -> None:
        rows = tmp_db.execute("SELECT key FROM config").fetchall()
        keys = {row[0] for row in rows}
        expected_keys = {"max_retries", "backoff_base", "timeout_seconds", "poll_interval_ms"}
        assert keys == expected_keys

    def test_default_max_retries_is_3(self, tmp_db: sqlite3.Connection) -> None:
        value = tmp_db.execute(
            "SELECT value FROM config WHERE key='max_retries'"
        ).fetchone()[0]
        assert value == "3"

    def test_default_backoff_base_is_2(self, tmp_db: sqlite3.Connection) -> None:
        value = tmp_db.execute(
            "SELECT value FROM config WHERE key='backoff_base'"
        ).fetchone()[0]
        assert value == "2"

    def test_default_timeout_seconds_is_300(self, tmp_db: sqlite3.Connection) -> None:
        value = tmp_db.execute(
            "SELECT value FROM config WHERE key='timeout_seconds'"
        ).fetchone()[0]
        assert value == "300"

    def test_default_poll_interval_ms_is_500(self, tmp_db: sqlite3.Connection) -> None:
        value = tmp_db.execute(
            "SELECT value FROM config WHERE key='poll_interval_ms'"
        ).fetchone()[0]
        assert value == "500"

    def test_seed_does_not_overwrite_user_values(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Re-running initialize_schema must not overwrite user-modified config."""
        tmp_db.execute("UPDATE config SET value='10' WHERE key='max_retries'")
        tmp_db.commit()

        initialize_schema(tmp_db)  # re-seed

        value = tmp_db.execute(
            "SELECT value FROM config WHERE key='max_retries'"
        ).fetchone()[0]
        assert value == "10", (
            "INSERT OR IGNORE must not overwrite an existing config value"
        )


# ---------------------------------------------------------------------------
# Idempotency tests
# ---------------------------------------------------------------------------

class TestSchemaIdempotency:
    """Verify that repeated calls to initialize_schema are safe."""

    def test_double_initialization_does_not_raise(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """Calling initialize_schema a second time must not raise any exception."""
        initialize_schema(tmp_db)  # called once already by the fixture; call again

    def test_table_count_unchanged_after_second_init(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        initialize_schema(tmp_db)
        count = tmp_db.execute(
            "SELECT count(*) FROM sqlite_master "
            "WHERE type='table' AND name IN ('jobs','config')"
        ).fetchone()[0]
        assert count == 2, "Exactly 2 tables must exist after repeated initialization"

    def test_config_row_count_unchanged_after_second_init(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        initialize_schema(tmp_db)
        count = tmp_db.execute("SELECT count(*) FROM config").fetchone()[0]
        assert count == 4, "Exactly 4 default config rows must exist (no duplicates)"


# ---------------------------------------------------------------------------
# Connection configuration tests
# ---------------------------------------------------------------------------

class TestConnectionConfiguration:
    """Verify that get_connection applies the required PRAGMA settings."""

    def test_wal_mode_is_enabled(self, tmp_db: sqlite3.Connection) -> None:
        mode = tmp_db.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal", f"Expected WAL journal mode, got '{mode}'"

    def test_row_factory_allows_column_name_access(
        self, tmp_db: sqlite3.Connection
    ) -> None:
        """sqlite3.Row must be set so we can access columns by name."""
        row = tmp_db.execute(
            "SELECT key, value FROM config WHERE key='max_retries'"
        ).fetchone()
        assert row is not None
        # This would raise TypeError if row_factory were not set to sqlite3.Row
        assert row["key"] == "max_retries"
        assert row["value"] == "3"

    def test_database_file_is_created_on_first_connect(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "new_db.db"
        assert not db_path.exists(), "Database file should not exist before first connection"
        conn = get_connection(db_path)
        conn.close()
        assert db_path.exists(), "get_connection must create the database file"

    def test_nested_parent_directories_are_created(self, tmp_path: Path) -> None:
        """get_connection must create missing parent directories."""
        db_path = tmp_path / "a" / "b" / "c" / "queuectl.db"
        assert not db_path.parent.exists()
        conn = get_connection(db_path)
        conn.close()
        assert db_path.parent.exists(), (
            "get_connection must create all intermediate parent directories"
        )
