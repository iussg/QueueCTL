"""
seed_jobs.py — Quick job seeder for manual testing.

Run from the queuectl directory:
    python seed_jobs.py

This bypasses PowerShell quoting hell by calling the service layer
directly — the same code the CLI calls internally.
"""

import sys
import os

# Ensure imports resolve from the queuectl directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.db import get_connection, initialize_schema
from core.job_service import enqueue
from core.exceptions import DuplicateJobError


def seed():
    conn = get_connection()
    initialize_schema(conn)

    jobs = [
        {
            "id": "job-001",
            "command": "echo Hello from QueueCTL",
            "max_retries": 3,
        },
        {
            "id": "job-002",
            "command": "python -c \"import sys; sys.exit(1)\"",
            "max_retries": 2,
        },
        {
            "id": "job-003",
            "command": "python -c \"import sys; sys.exit(1)\"",
            "max_retries": 1,
        },
    ]

    for spec in jobs:
        try:
            job = enqueue(conn, spec["id"], spec["command"], spec["max_retries"])
            print(f"  Enqueued  id={job.id!r:15s}  state={job.state}  max_retries={job.max_retries}")
        except DuplicateJobError:
            print(f"  Skipped   id={spec['id']!r:15s}  (already exists)")

    conn.close()
    print("\nDone. Run: python cli.py status")


if __name__ == "__main__":
    seed()
