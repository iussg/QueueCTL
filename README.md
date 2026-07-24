# QueueCTL

A durable, CLI-driven background job queue — a minimal, single-node version
of what Sidekiq, Celery, or AWS SQS + Lambda do in production.

> **Status:** Phase 1 (scaffold + schema) complete. Full documentation will
> be written in Phase 7 once all features are implemented.

## Quick Start

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run tests
pytest
```

## Architecture

See [`QueueCTL_EDD.md`](../QueueCTL_EDD.md) for the full Engineering Design
Document and [`ENGINEERING_DECISIONS.md`](ENGINEERING_DECISIONS.md) (Phase 7)
for the rationale behind every design decision.

## Development Phases

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | ✅ Complete | Project scaffold, SQLite schema, storage layer |
| 2 | ⏳ Pending | Job domain model + job service (enqueue, claim, complete, fail) |
| 3 | ⏳ Pending | Retry logic, backoff, DLQ, config management |
| 4 | ⏳ Pending | Worker process loop, SIGTERM handler, crash recovery |
| 5 | ⏳ Pending | Full CLI implementation |
| 6 | ⏳ Pending | Test suite (concurrency stress test) |
| 7 | ⏳ Pending | README, ENGINEERING_DECISIONS.md, design.md |
