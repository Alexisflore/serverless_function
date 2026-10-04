# CLAUDE.md

## Project

Hourly sync of Shopify stores (US, JP, UK) into a shared Supabase Postgres database.
- One GitHub Actions workflow per store (`.github/workflows/process-daily-data*.yml`) runs `run_daily_sync.py` -> `api/process_daily_data.py`.
- The store is selected only through env vars: `SHOPIFY_STORE_DOMAIN`, the Shopify credentials, and `COMMERCIAL_ORGANISATION`.
- Rows are told apart by `commercial_organisation`.

## Common Commands

```bash
pipenv install --deploy                       # install dependencies (Python 3.10)
pipenv run python run_daily_sync.py           # run a sync for the store set in env (writes to the DB)
pipenv run python -m compileall -q api/       # syntax check
pipenv run python -m pytest test/test_process_inventory_queue_mock.py -v
```

## Guardrails

- One store per process. Set the store env, and any runtime token exchange, before importing `api.process_daily_data`, because some modules read env at import time.
- Every write sets `get_store_context()` fields. Incremental reads filter by store.
- Never print tokens, client secrets or full customer PII. Never commit `.env` or `data/`.
- Don't run syncs, backfills or SQL against production without explicit approval.

## AI agents orchestration

This repository contains specialized AI agents in `.claude/agents/`. Delegate to them proactively.

| AI agent | Use for |
|---|---|
| repo-explorer | Locating code paths with file:line evidence (read-only) |
| architect | Designing multi-store, schema or auth changes before coding (read-only) |
| shopify-sync-engineer | Processors in `api/lib/`, Shopify API calls, store context, currency and market |
| database-engineer | SQL migrations in `database_creation/`, org seed rows, indexes, views |
| devops-engineer | GitHub Actions workflows, schedules, secrets mapping |
| backfill-engineer | Historical backfills and Shopify-vs-DB reconciliation per store |
| debugger | Root-causing failed runs and data mismatches |
| test-engineer | Unit tests with mocks, store isolation regressions, smoke tests |
| code-reviewer | Independent final review of a diff (read-only) |
| security-reviewer | Secrets, tokens, PII, SQL injection, workflow triggers (read-only) |

**Standard feature workflow:**
1. architect, if the change spans several modules or stores
2. repo-explorer
3. implementation AI agent: shopify-sync-engineer, database-engineer, devops-engineer or backfill-engineer
4. test-engineer
5. code-reviewer
6. security-reviewer, when tokens, secrets, PII or workflows are touched

**New store onboarding:** architect -> database-engineer (org row) -> devops-engineer (secrets + workflow) -> shopify-sync-engineer (store-aware fixes, token exchange) -> test-engineer -> backfill-engineer -> code-reviewer + security-reviewer.

**Debugging workflow:** debugger -> implementation AI agent -> test-engineer -> code-reviewer (then backfill-engineer if data needs repair).

**Completion criteria:** implementation, then tests, then independent review, then deterministic verification. The AI agent that wrote the code must not be the only one judging it correct. Never run AI agents in parallel when they edit the same files.
