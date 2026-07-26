"""
simulate_crash.py — Inject an orphaned 'processing' job directly into the DB.

This replicates exactly what happens when a worker crashes mid-execution:
a job is left in state='processing' with a worker_id that no longer exists.

Run this script, then start a fresh worker to see crash recovery trigger.

Usage:
    python simulate_crash.py
    python cli.py worker start   ← new worker will recover the orphaned job
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.db import get_connection, initialize_schema
from core.job_service import enqueue

def simulate_crash():
    conn = get_connection()
    initialize_schema(conn)

    # Enqueue a job normally first (state=pending)
    job_id = "orphaned-job"
    try:
        enqueue(conn, job_id, "echo this was rescued by crash recovery", max_retries=3)
        print(f"  Enqueued  id={job_id!r}")
    except Exception:
        # Already exists — update it
        with conn:
            conn.execute("UPDATE jobs SET state='pending', attempts=0 WHERE id=?", (job_id,))
        print(f"  Reset existing job {job_id!r} to pending")

    # Directly set it to processing with a fake dead worker_id
    # This simulates a worker that claimed the job and then crashed.
    with conn:
        conn.execute("""
            UPDATE jobs
            SET    state     = 'processing',
                   worker_id = 'worker-99999',
                   picked_at = datetime('now'),
                   started_at = datetime('now'),
                   updated_at = datetime('now')
            WHERE  id = ?
        """, (job_id,))

    # Verify
    row = conn.execute("SELECT id, state, worker_id FROM jobs WHERE id=?", (job_id,)).fetchone()
    print(f"\n  Job '{row['id']}' is now stuck in state='{row['state']}'")
    print(f"  Fake crashed worker_id: '{row['worker_id']}'")
    print(f"\n  This is what a crashed worker leaves behind.")
    print(f"  Now run:  python cli.py worker start")
    print(f"  And watch for the crash recovery WARNING line.\n")

    conn.close()

if __name__ == "__main__":
    simulate_crash()
