# TerSQL Implementation Plan

---

## v0.0.2 Upgrade — ✅ Completed

All tasks from the v0.0.2 upgrade plan have been implemented.

### Entry Point — ✅ Done
- [x] **[main.py]** Filled as the true CLI entry point; imports and calls `main()` from `TerSQL.py`. Argument parsing wired correctly.

### Database Plugins — ✅ Done
- [x] **[postgre.py]** PostgreSQL plugin implemented via `psycopg2-binary`. Supports connection pooling, cursor handling, streaming, and `pg_dump`-based backup. Registered as `"postgresql"`.
- [x] **[mongodb.py]** MongoDB plugin implemented via `pymongo`. Bridges SQL-like interactions via the NLP engine; exposes collections as schemas. Registered as `"mongodb"`.

### Core Enhancements — ✅ Done
- [x] **[Core.py — Bookmarks]** Load/save bookmarks from `~/.tersql_bookmarks.json`; methods `add_bookmark`, `list_bookmarks`, `run_bookmark`, `del_bookmark` exposed.
- [x] **[Core.py — Auto-corrections]** `SQL_FIXES` regex logic hooked into `TerSQLCore.run()` execution path; common shorthands rewritten before reaching the NLP engine.
- [x] **[TerSQL.py — Aliases]** `.q`, `.st`, `.bm`, `.fav`, and others mapped to their canonical dot commands.
- [x] **[TerSQL.py — Dot Commands]** `.bookmark`, `.bookmarks`, `.run`, `.delbookmark` registered and routing to `TerSQLCore`. `DOT_COMMANDS` list updated for autocomplete.

---

## v0.0.3 Upgrade — ✅ Completed

### NLP.py

- [x] **[ConditionParser — BETWEEN]** Added `_BETWEEN_RE` — parses `"age between 20 and 40"` into `age BETWEEN 20 AND 40`.
- [x] **[ConditionParser — IN list]** Added `_IN_LIST_RE` — parses `"category in electronics, books"` (no parentheses) into `category IN ('electronics', 'books')`.
- [x] **[ConditionParser — NOT operators]** Added `NOT LIKE` and `NOT IN` expansions to `OP_MAP`.
- [x] **[SchemaResolver — fuzzy fallback]** Edit-distance fuzzy matching for column/table name resolution; minor typos now resolve to the closest known name instead of failing silently.
- [x] **[SafetyChecker]** Added `DROP SCHEMA`, `DROP VIEW`, `DROP FUNCTION`, `DROP PROCEDURE` to dangerous keyword patterns; now fully aligned with `SafetyGate` in `Core.py`.
- [x] **[NLPEngine — early return]** `translate()` short-circuits on raw SQL passthrough before normalisation.
- [x] **[NLPEngine — filler stripper]** `_normalise()` strips filler words (`please`, `can you`, `show me`, `fetch`, `retrieve`) before intent matching.
- [x] **[NLPEngine — contractions]** `_normalise()` expands `don't`, `can't`, `isn't`, `won't`, `didn't`, `hasn't`, `aren't`.
- [x] **[NLPEngine — batch]** New `translate_batch(texts)` method for bulk NL translation.
- [x] **[NLPEngine — dialect]** Dialect-aware SQL tweaks wired through `SQLBuilder`; PostgreSQL receives `ILIKE` instead of `LIKE`.
- [x] **[SQLBuilder — subquery]** New `_build_subquery(outer_table, outer_col, inner_sql)` helper for nested `SELECT … WHERE IN (…)` patterns.

### Core.py

- [x] **[SafetyGate]** `_DANGEROUS` regex expanded to cover `DROP SCHEMA`, `DROP VIEW`, `DROP FUNCTION`, `DROP PROCEDURE`; mirrors `NLP.SafetyChecker` exactly.
- [x] **[TerSQLCore.run()]** Removed dead Groq source branch. `source == "none"` (unmatched NL input) handled cleanly without crash.
- [x] **[TerSQLCore.run() — truncation]** Render path checks `result.truncated` and `result.total_count` (new `QueryResult` fields); appends truncation note to row label when applicable.
- [x] **[TerSQLCore.status()]** Prints full `PluginStats` summary block; surfaces `HealthStatus.warnings` and `h.extra` fields (e.g. `cache_size`, `pool_size`).
- [x] **[TerSQLCore._do_backup()]** Catches `NotImplementedError` specifically — MongoDB backends prompt to continue without backup rather than crashing.
- [x] **[Convenience wrappers]** `table_info(table)`, `primary_keys(table)`, `plugins_info()` added as thin wrappers over plugin methods and `PluginRegistry.list_meta()`.
- [x] **[QueryRecord.source]** Docstring updated for all three valid sources: `"rule"`, `"passthrough"`, `"none"`.

### TerSQL.py

- [x] **[.info <table>]** New dot command — calls `core.table_info()` to display enriched metadata.
- [x] **[.pks <table>]** New dot command — calls `core.primary_keys()` to display primary key columns.
- [x] **[.plugins]** New dot command — calls `core.plugins_info()` and renders the registered plugin list.
- [x] **[.stats]** New dot command — displays current `PluginStats` session summary.
- [x] **[.schema enrichment]** `.schema` now appends `table_info()` metadata block after column listing.
- [x] **[.dbs — MongoDB routing]** MongoDB path routes to `db.list_databases()` instead of issuing `SHOW DATABASES`.
- [x] **[New aliases]** `.pk → .pks`, `.plug → .plugins`, `.tableinfo → .info`.
- [x] **[HELP_TEXT & DOT_COMMANDS]** Updated to include all four new commands and NL examples for `BETWEEN` and `IN` list patterns.
- [x] **[`__version__`]** Bumped to `"0.0.3"`.
- [x] **[EXPLAIN guard]** Routes through `TerSQLCore.explain()` which skips `EXPLAIN` for MongoDB backends instead of crashing.

---

## v0.0.4 Roadmap — Active Planning

The following items are candidates for the next release cycle. Open a [GitHub Discussion](https://github.com/developersayandutta/TerSQL/discussions) to propose prioritisation changes or add new ideas.

### Performance & Scalability
- [ ] **Streaming large result sets** — Page through results lazily instead of loading the full result set into memory; remove the current memory-bound limitation.
- [ ] **Connection pooling for MySQL** — Mirror the pooling architecture already present in the PostgreSQL plugin.

### NLP Engine
- [ ] **JOIN intent** — Detect `"join orders with users on id"` style natural-language patterns and emit valid `JOIN` clauses.
- [ ] **Aggregate functions** — Recognise `"average salary by department"` → `SELECT department, AVG(salary) FROM … GROUP BY department`.
- [ ] **ORDER BY / LIMIT intent** — Parse `"top 10 products by price"` patterns.

### Output & Export
- [ ] **Pager integration** — Pipe large outputs through `less`/`more` automatically when result height exceeds terminal height.
- [ ] **Tee mode** — Mirror all output to a log file alongside the terminal.
- [ ] **`.source <file>`** — Execute `.sql` script files directly from the REPL.

### Plugin System
- [ ] **SQLite plugin** — Lightweight local file-backed database support; useful for offline testing and scripting.
- [ ] **Plugin metadata versioning** — Surface plugin version and minimum TerSQL version requirement in `PluginRegistry.list_meta()`.

### Developer Experience
- [ ] **`pytest` integration suite expansion** — Full unit coverage for `ConditionParser`, `SchemaResolver`, and all v0.0.3 `Core.py` changes.
- [ ] **Docker Compose dev environment** — Single `docker-compose up` to spin up MySQL + PostgreSQL + MongoDB for local integration testing.

---

## Open Questions

1. Should `translate_batch()` support async execution for large batches, or is a synchronous loop sufficient for the REPL use case?
2. Is there appetite for a `--config` flag pointing to a TOML/YAML file for persistent connection profiles, replacing repeated CLI flags?
3. Which SQLite distribution is preferred for the upcoming plugin: the standard library `sqlite3` module (zero dependencies) or `sqlalchemy` (better type mapping)?